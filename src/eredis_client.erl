%%
%% eredis_client
%%
%% The client is implemented as a gen_server which keeps one socket
%% open to a single Redis instance. Users call us using the API in
%% eredis.erl.
%%
%% The client works like this:
%%  * When starting up, we connect to Redis with the given connection
%%     information, or fail.
%%  * Users calls us using gen_server:call, we send the request to Redis,
%%    add the calling process at the end of the queue and reply with
%%    noreply. We are then free to handle new requests and may reply to
%%    the user later.
%%  * We receive data on the socket, we parse the response and reply to
%%    the client at the front of the queue. If the parser does not have
%%    enough data to parse the complete response, we will wait for more
%%    data to arrive.
%%
-module(eredis_client).
-author('knut.nesheim@wooga.com').

-behaviour(gen_server).

-include("eredis.hrl").

%% API
-export([start_link/4, stop/1, select_database/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          host :: string() | undefined,
          port :: integer() | undefined,
          password :: binary() | undefined,
          database :: binary() | undefined,

          socket :: port() | undefined,
          parser_state :: #pstate{} | undefined,
          queue :: queue() | undefined
}).

-define(SOCKET_OPTS, [binary, {active, once}, {packet, raw}, {reuseaddr, true}]).

%%
%% API
%%

-spec start_link(Host::list(), Port::integer(), Database::integer(),
                 Password::string()) -> {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Host, Port, Database, Password) ->
    gen_server:start_link(?MODULE, [Host, Port, Database, Password], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Host, Port, Database, Password]) ->
    State = #state{host = Host,
                   port = Port,
                   database = list_to_binary(integer_to_list(Database)),
                   password = list_to_binary(Password),
                   parser_state = eredis_parser:init(),
                   queue = queue:new()},

    case connect(State) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

handle_call({request, Req}, From, State) ->
    case gen_tcp:send(State#state.socket, Req) of
        ok ->
            NewQueue = queue:in(From, State#state.queue),
            {noreply, State#state{queue = NewQueue}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, State};

handle_call(_Request, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Receive data from socket, see handle_response/2
handle_info({tcp, _Socket, Bs}, State) ->
    inet:setopts(State#state.socket, [{active, once}]),
    {noreply, handle_response(Bs, State)};

%% Socket got closed, for example by Redis terminating idle clients.
%% Reconnect and if it fails, stop the gen_server.
handle_info({tcp_closed, _Socket}, State) ->
    case connect(State) of
        {ok, NewState} ->
            %% Throw away the queue, as we will never get a response
            %% for the requests sent on the old socket, ever
            {noreply, NewState#state{queue = queue:new()}};
        {error, Reason} ->
            {stop, {reconnect_error, Reason}}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    gen_tcp:close(State#state.socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-spec handle_response(Data::binary(), State::#state{}) -> NewState::#state{}.
%% @doc: Handle the response coming from Redis. This includes parsing
%% and replying to the correct client, handling partial responses,
%% handling too much data and handling continuations.
handle_response(Data, #state{parser_state = ParserState,
                             queue = Queue} = State) ->

    case eredis_parser:parse(ParserState, Data) of
        %% Got complete response, return value to client
        {ReturnCode, Value, NewParserState} ->
            NewQueue = reply({ReturnCode, Value}, Queue),
            State#state{parser_state = NewParserState,
                        queue = NewQueue};

        %% Got complete response, with extra data, reply to client and
        %% recurse over the extra data
        {ReturnCode, Value, Rest, NewParserState} ->
            NewQueue = reply({ReturnCode, Value}, Queue),
            handle_response(Rest, State#state{parser_state = NewParserState,
                                              queue = NewQueue});

        %% Parser needs more data, the parser state now contains the
        %% continuation data and we will try calling parse again when
        %% we have more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState}
    end.

%% @doc: Sends a value to the first client in queue. Returns the new
%% queue without this client.
reply(Value, Queue) ->
    case queue:out(Queue) of
        {{value, From}, NewQueue} ->
            gen_server:reply(From, Value),
            NewQueue;
        {empty, Queue} ->
            %% Oops
            error_logger:info_msg("Nothing in queue, but got value from parser~n"),
            throw(empty_queue)
    end.


%% @doc: Helper for connecting to Redis, authenticating and selecting
%% the correct database. These commands are synchronous and if Redis
%% returns something we don't expect, we crash. Returns {ok, State} or
%% {SomeError, Reason}.
connect(State) ->
    case gen_tcp:connect(State#state.host, State#state.port, ?SOCKET_OPTS) of
        {ok, Socket} ->
            case authenticate(Socket, State#state.password) of
                ok ->
                    case select_database(Socket, State#state.database) of
                        ok ->
                            {ok, State#state{socket = Socket}};
                        {error, Reason} ->
                            {select_error, Reason}
                    end;
                {error, Reason} ->
                    {authentication_error, Reason}
            end;
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

select_database(Socket, Database) ->
    do_sync_command(Socket, ["SELECT", " ", Database, "\r\n"]).

authenticate(_Socket, <<>>) ->
    ok;
authenticate(Socket, Password) ->
    do_sync_command(Socket, ["AUTH", " ", Password, "\r\n"]).

%% @doc: Executes the given command synchronously, expects Redis to
%% return "+OK\r\n", otherwise it will fail.
do_sync_command(Socket, Command) ->
    inet:setopts(Socket, [{active, false}]),
    case gen_tcp:send(Socket, Command) of
        ok ->
            %% Hope there's nothing else coming down on the socket..
            case gen_tcp:recv(Socket, 0) of
                {ok, <<"+OK\r\n">>} ->
                    inet:setopts(Socket, [{active, once}]),
                    ok;
                Other ->
                    {error, {unexpected_data, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
