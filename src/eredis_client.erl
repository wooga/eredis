%%
%% eredis_client
%%
%% The client is implemented as a gen_server which keeps one socket
%% open to a single Redis instance. Users call us using the API in
%% eredis.erl.
%%
%% The client works like this:
%%  * When starting up, we connect to Redis with the given connection
%%    information, or fail.
%%  * Users calls us using gen_server:call, we send the request to Redis,
%%    add the calling process at the end of the queue and reply with
%%    noreply. We are then free to handle new requests and may reply to
%%    the user later.
%%  * We receive data on the socket, we parse the response and reply to
%%    the client at the front of the queue. If the parser does not have
%%    enough data to parse the complete response, we will wait for more
%%    data to arrive.
%%  * For pipeline commands, we include the number of responses we are
%%    waiting for in each element of the queue. Responses are queued until
%%    we have all the responses we need and then reply with all of them.
%%  * Redis pubsub messages are handled by sending Erlang messages to
%%    all registered subscribers.
%%
-module(eredis_client).
-author('knut.nesheim@wooga.com').

-behaviour(gen_server).

-include("eredis_priv.hrl").

%% API
-export([start_link/5, stop/1, select_database/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SOCKET_OPTS, [binary, {active, once}, {packet, raw}, {reuseaddr, true}]).

%%
%% API
%%

-spec start_link(Host::list(),
                 Port::integer(),
                 Database::integer(),
                 Password::string(),
                 ReconnectSleep::integer()) ->
                        {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Host, Port, Database, Password, ReconnectSleep) ->
    gen_server:start_link(?MODULE, [Host, Port, Database, Password, ReconnectSleep], []).


stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Host, Port, Database, Password, ReconnectSleep]) ->
    State = #state{host = Host,
                   port = Port,
                   database = list_to_binary(integer_to_list(Database)),
                   password = list_to_binary(Password),
                   reconnect_sleep = ReconnectSleep,

                   parser_state = eredis_parser:init(),
                   queue = queue:new(),
                   msg_queue = queue:new()},

    case connect(State) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

handle_call({request, Req}, From, State) ->
    do_request(Req, From, State);

handle_call({pipeline, Pipeline}, From, State) ->
    do_pipeline(Pipeline, From, State);

handle_call({controlling_process, Pid}, _From, State) ->
    do_controlling_process(Pid, State);

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, unknown_request, State}.


handle_cast({ack_message, Pid},
            #state{controlling_process={_, Pid}} = State) ->
    {noreply, maybe_send_message(State#state{msg_state=ready})};
handle_cast({ack_message, _}, State) ->
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.


%% Receive data from socket, see handle_response/2
handle_info({tcp, _Socket, Bs}, State) ->
    NewState = State#state{conn_state=passive},
    {noreply, handle_response(Bs, update_socket_state(NewState))};

%% Socket got closed, for example by Redis terminating idle
%% clients. Spawn of a new process which will try to reconnect and
%% notify us when Redis is ready. In the meantime, we can respond with
%% an error message to all our clients.
handle_info({tcp_closed, _Socket}, State) ->
    Self = self(),
    send_to_controller({eredis_disconnected, Self}, State),
    spawn(fun() -> reconnect_loop(Self, State) end),

    %% Throw away the socket and the queue, as we will never get a
    %% response to the requests sent on the old socket. The absence of
    %% a socket is used to signal we are "down"
    {noreply, State#state{socket = undefined, queue = queue:new()}};

%% Redis is ready to accept requests, the given Socket is a socket
%% already connected and authenticated.
handle_info({connection_ready, Socket}, #state{socket = undefined} = State) ->
    send_to_controller({eredis_connected, self()}, State),
    {noreply, State#state{socket = Socket, conn_state = active_once}};

%% Our controlling process is down.
handle_info({'DOWN', Ref, process, Pid, _Reason},
            #state{controlling_process={Ref, Pid}} = State) ->
    {stop, shutdown, State#state{controlling_process=undefined,
                                 msg_state=ready,
                                 msg_queue=queue:new()}};

%% eredis can be used in Poolboy, but it requires to support a simple API
%% that Poolboy uses to manage the connections.
handle_info(stop, State) ->
    {stop, shutdown, State};

handle_info(_Info, State) ->
    {stop, {unhandled_message, _Info}, State}.

terminate(_Reason, State) ->
    case State#state.socket of
        undefined -> ok;
        Socket    -> gen_tcp:close(Socket)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

-spec do_request(Req::iolist(), From::pid(), #state{}) ->
                        {noreply, #state{}} | {reply, Reply::any(), #state{}}.
%% @doc: Sends the given request to redis. If we do not have a
%% connection, returns error.
do_request(_Req, _From, #state{socket = undefined} = State) ->
    {reply, {error, no_connection}, State};

do_request(Req, From, State) ->
    case gen_tcp:send(State#state.socket, Req) of
        ok ->
            NewQueue = queue:in({1, From}, State#state.queue),
            {noreply, update_socket_state(State#state{queue = NewQueue})};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

-spec do_pipeline(Pipeline::pipeline(), From::pid(), #state{}) ->
                         {noreply, #state{}} | {reply, Reply::any(), #state{}}.
%% @doc: Sends the entire pipeline to redis. If we do not have a
%% connection, returns error.
do_pipeline(_Pipeline, _From, #state{socket = undefined} = State) ->
    {reply, {error, no_connection}, State};

do_pipeline(Pipeline, From, State) ->
    case gen_tcp:send(State#state.socket, Pipeline) of
        ok ->
            NewQueue = queue:in({length(Pipeline), From, []}, State#state.queue),
            {noreply, update_socket_state(State#state{queue = NewQueue})};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

-spec do_controlling_process(Pid::pid(), #state{}) -> {reply, Reply::{ok, reference()}, #state{}}.
do_controlling_process(Pid, State) ->
    case State#state.controlling_process of
        undefined ->
            ok;
        {OldRef, _OldPid} ->
            erlang:demonitor(OldRef)
    end,
    Ref = erlang:monitor(process, Pid),
    {reply, ok, State#state{controlling_process={Ref, Pid}}}.

-spec handle_response(Data::binary(), State::#state{}) -> NewState::#state{}.
%% @doc: Handle the response coming from Redis. This includes parsing
%% and replying to the correct client, handling partial responses,
%% handling too much data and handling continuations.
handle_response(Data, #state{parser_state = ParserState} = State) ->
    case eredis_parser:parse(ParserState, Data) of
        %% Got complete response, return value to client
        {ReturnCode, Value, NewParserState} ->
            reply({ReturnCode, Value}, State#state{parser_state=NewParserState});

        %% Got complete response, with extra data, reply to client and
        %% recurse over the extra data
        {ReturnCode, Value, Rest, NewParserState} ->
            NewState = reply({ReturnCode, Value},
                             State#state{parser_state=NewParserState}),
            handle_response(Rest, NewState);

        %% Parser needs more data, the parser state now contains the
        %% continuation data and we will try calling parse again when
        %% we have more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState}
    end.

%% @doc: Sends a value to the first client in queue. Returns the new
%% queue without this client. If we are still waiting for parts of a
%% pipelined request, push the reply to the the head of the queue and
%% wait for another reply from redis. Pubsub messages are not part of
%% the normal reply stream and will instead be sent as Erlang messages
%% to the controlling process (if any).
reply({ok, [<<"message">>, Channel, Message]}, State) ->
    Msg = {message, Channel, Message, self()},
    MsgQueue = queue:in(Msg, State#state.msg_queue),
    maybe_send_message(State#state{msg_queue=MsgQueue});
reply(Value, #state{queue=Queue} = State) ->
    case queue:out(Queue) of
        {{value, {1, From}}, NewQueue} ->
            gen_server:reply(From, Value),
            State#state{queue=NewQueue};
        {{value, {1, From, Replies}}, NewQueue} ->
            gen_server:reply(From, lists:reverse([Value | Replies])),
            State#state{queue=NewQueue};
        {{value, {N, From, Replies}}, NewQueue} when N > 1 ->
            State#state{queue=queue:in_r({N - 1, From, [Value | Replies]},
                                         NewQueue)};
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

%% @doc: Loop until a connection can be established, this includes
%% successfully issuing the auth and select calls. When we have a
%% connection, give the socket to the redis client.
reconnect_loop(Client, #state{reconnect_sleep=ReconnectSleep}=State) ->
    case catch(connect(State)) of
        {ok, #state{socket = Socket}} ->
            gen_tcp:controlling_process(Socket, Client),
            Client ! {connection_ready, Socket};
        {error, _Reason} ->
            timer:sleep(ReconnectSleep),
            reconnect_loop(Client, State);
        %% Something bad happened when connecting, like Redis might be
        %% loading the dataset and we got something other than 'OK' in
        %% auth or select
        _ ->
            timer:sleep(ReconnectSleep),
            reconnect_loop(Client, State)
    end.


send_to_controller(_Msg, #state{controlling_process=undefined}) ->
    ok;
send_to_controller(Msg, #state{controlling_process={_Ref, Pid}}) ->
    Pid ! Msg.


maybe_send_message(#state{controlling_process=undefined} = State) ->
    State#state{msg_queue=queue:new()};
maybe_send_message(#state{msg_state=need_ack} = State) ->
    State;
maybe_send_message(State) ->
    case queue:out(State#state.msg_queue) of
        {empty, _Queue} ->
            State;
        {{value, Msg}, Queue} ->
            send_to_controller(Msg, State),
            State#state{msg_queue=Queue, msg_state=need_ack}
    end.


update_socket_state(#state{conn_state=active_once} = State) ->
    State;
update_socket_state(#state{controlling_process=undefined} = State) ->
    inet:setopts(State#state.socket, [{active, once}]),
    State#state{conn_state=active_once};
update_socket_state(#state{msg_state=ready} = State) ->
    inet:setopts(State#state.socket, [{active, once}]),
    State#state{conn_state=active_once};
update_socket_state(State) ->
    case queue:is_empty(State#state.queue) of
        true ->
            State;
        false ->
            inet:setopts(State#state.socket, [{active, once}]),
            State#state{conn_state=active_once}
    end.
