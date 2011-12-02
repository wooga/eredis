%%
%% eredis_pubsub_client
%%
%% This client implements a subscriber to a Redis pubsub channel. It
%% is implemented in the same way as eredis_client, except channel
%% messages are streamed to the controlling process. eredis will only
%% receive the next message on the socket when the current message has
%% been acked.
%%
%% There is one consuming process per eredis_sub_client.
-module(eredis_sub_client).
-author('knut.nesheim@wooga.com').

-behaviour(gen_server).
-include("eredis.hrl").
-include("eredis_sub.hrl").


%% API
-export([start_link/5, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%
%% API
%%

-spec start_link(Host::list(),
                 Port::integer(),
                 Password::string(),
                 Channels::[channel()],
                 ReconnectSleep::integer()) ->
                        {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Host, Port, Password, Channels, ReconnectSleep) ->
    Args = [Host, Port, Password, Channels, ReconnectSleep],
    gen_server:start_link(?MODULE, Args, []).


stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Host, Port, Password, Channels, ReconnectSleep]) ->
    State = #state{host = Host,
                   port = Port,
                   password = list_to_binary(Password),
                   reconnect_sleep = ReconnectSleep,
                   channels = Channels,
                   parser_state = eredis_parser:init(),
                   msg_queue = queue:new()},

    case connect(State) of
        {ok, NewState} ->
            inet:setopts(NewState#state.socket, [{active, once}]),
            {ok, NewState};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

%% Set the controlling process. All messages on all channels are directed here.
handle_call({controlling_process, Pid}, _From, State) ->
    case State#state.controlling_process of
        undefined ->
            ok;
        {OldRef, _OldPid} ->
            erlang:demonitor(OldRef)
    end,
    Ref = erlang:monitor(process, Pid),
    {reply, ok, State#state{controlling_process={Ref, Pid}}};


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, unknown_request, State}.

%% Controlling process acknowledges receipt of previous message. Send
%% the next if there is any messages queued or ask for more on the
%% socket.
handle_cast({ack_message, Pid},
            #state{controlling_process={_, Pid}} = State) ->

    NewState = case queue:out(State#state.msg_queue) of
                   {empty, _Queue} ->
                       inet:setopts(State#state.socket, [{active, once}]),
                       State#state{msg_state = ready};
                   {{value, Msg}, Queue} ->
                       send_to_controller(Msg, State),
                       State#state{msg_queue = Queue, msg_state = need_ack}
               end,
    {noreply, NewState};

handle_cast({ack_message, _}, State) ->
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


%% Receive data from socket, see handle_response/2
handle_info({tcp, _Socket, Bs}, State) ->
    {noreply, handle_response(Bs, State)};

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
    {noreply, State#state{socket = Socket}};

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

-spec handle_response(Data::binary(), State::#state{}) -> NewState::#state{}.
%% @doc: Handle the response coming from Redis. This should only be
%% channel messages that we should forward to the controlling process
%% or queue if the previous message has not been acked. If there are
%% more than a single response in the data we got, queue the responses
%% and serve them up when the controlling process is ready
handle_response(Data, #state{parser_state = ParserState} = State) ->
    case eredis_parser:parse(ParserState, Data) of
        %% Channel message
        {ReturnCode, Value, NewParserState} ->
            reply({ReturnCode, Value},
                  State#state{parser_state=NewParserState});

        {ReturnCode, Value, Rest, NewParserState} ->
            error_logger:info_msg("too much!~n"),
            NewState = reply({ReturnCode, Value},
                             State#state{parser_state=NewParserState}),
            handle_response(Rest, NewState);

        {continue, NewParserState} ->
            error_logger:info_msg("cont~n"),
            inet:setopts(State#state.socket, [{active, once}]),
            State#state{parser_state = NewParserState}
    end.

%% @doc: Sends a reply to the controlling process if the process has
%% acknowledged the previous process, otherwise the message is queued
%% for later delivery.
reply({ok, [<<"message">>, Channel, Message]}, State) ->
    Msg = {message, Channel, Message, self()},

    %% Crash if there is no controlling process?

    case State#state.msg_state of
        need_ack ->
            error_logger:info_msg("queuing~n"),
            MsgQueue = queue:in(Msg, State#state.msg_queue),
            State#state{msg_queue = MsgQueue};
        ready ->
            send_to_controller(Msg, State),
            State#state{msg_state = need_ack}
    end;
reply({ReturnCode, Value}, State) ->
    throw({unexpected_response_from_redis, ReturnCode, Value, State}).


%% @doc: Helper for connecting to Redis. These commands are
%% synchronous and if Redis returns something we don't expect, we
%% crash. Returns {ok, State} or {SomeError, Reason}.
connect(State) ->
    case gen_tcp:connect(State#state.host, State#state.port, ?SOCKET_OPTS) of
        {ok, Socket} ->
            inet:setopts(Socket, [{active, false}]),
            case authenticate(Socket, State#state.password) of
                ok ->
                    case do_subscribe(Socket, State#state.channels) of
                        ok ->
                            {ok, State#state{socket = Socket}};
                        {error, Reason} ->
                            {error, {channel_subscribe_error, Reason}}
                    end;
                {error, Reason} ->
                    {error, {authentication_error, Reason}}
            end;
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

do_subscribe(_Socket, []) ->
    {error, no_channels};
do_subscribe(Socket, Channels) ->
    Command = eredis:create_multibulk(["SUBSCRIBE" | Channels]),
    ok = gen_tcp:send(Socket, Command),

    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            case parse_subscribe_result(Data, eredis_parser:init(), []) of
                {ok, Channels} ->
                    ok;
                Other ->
                    {error, {unexpected_data, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

parse_subscribe_result(Data, ParserState, Acc) ->
    case eredis_parser:parse(ParserState, Data) of
        {ok, [<<"subscribe">>, Chan, _], _} ->
            {ok, lists:reverse([Chan | Acc])};
        {ok, [<<"subscribe">>, Chan, _], Rest, NewParserState} ->
            parse_subscribe_result(Rest, NewParserState, [Chan | Acc])
    end.



authenticate(_Socket, <<>>) ->
    ok;
authenticate(Socket, Password) ->
    do_sync_command(Socket, ["AUTH", " ", Password, "\r\n"]).

%% @doc: Executes the given command synchronously, expects Redis to
%% return "+OK\r\n", otherwise it will fail.
do_sync_command(Socket, Command) ->
    case gen_tcp:send(Socket, Command) of
        ok ->
            %% Hope there's nothing else coming down on the socket..
            case gen_tcp:recv(Socket, 0) of
                {ok, <<"+OK\r\n">>} ->
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
