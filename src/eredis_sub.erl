%%
%% Erlang PubSub Redis client
%%
-module(eredis_sub).
-author('knut.nesheim@wooga.com').

-include("eredis.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start_link/1, start_link/4, start_link/5,
         controlling_process/1, controlling_process/2, controlling_process/3,
         ack_message/1]).

%%
%% PUBLIC API
%%

start_link(Host, Port, Password, Channels) ->
    start_link(Host, Port, Password, Channels, 100).

start_link(Host, Port, Password, Channels, ReconnectSleep)
  when is_list(Host);
       is_integer(Port);
       is_list(Password);
       is_list(Channels);
       is_integer(ReconnectSleep) ->

    eredis_sub_client:start_link(Host, Port, Password, Channels, ReconnectSleep).


%% @doc: Callback for starting from poolboy
-spec start_link(server_args()) -> {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Args) ->
    Host           = proplists:get_value(host, Args, "127.0.0.1"),
    Port           = proplists:get_value(port, Args, 6379),
    Database       = proplists:get_value(database, Args, 0),
    Password       = proplists:get_value(password, Args, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, Args, 100),
    start_link(Host, Port, Database, Password, ReconnectSleep).


-spec controlling_process(Client::pid()) -> ok.
%% @doc: Make the calling process the controlling process. The
%% controlling process received pubsub-related messages, of which
%% there are three kinds. In each message, the pid refers to the
%% eredis client process.
%%
%%   {message, Channel::binary(), Message::binary(), pid()}
%%     This is sent for each pubsub message received by the client.
%%
%%   {eredis_disconnected, pid()}
%%     This is sent when the eredis client is disconnected from redis.
%%
%%   {eredis_connected, pid()}
%%     This is sent when the eredis client reconnects to redis after
%%     an existing connection was disconnected.
%%
%% Any message of the form {message, _, _, _} must be acknowledged
%% before any subsequent message of the same form is sent. This
%% prevents the controlling process from being overrun with redis
%% pubsub messages. See ack_message/1.
controlling_process(Client) ->
    controlling_process(Client, self()).

-spec controlling_process(Client::pid(), Pid::pid()) -> ok.
%% @doc: Make the given process (pid) the controlling process.
controlling_process(Client, Pid) ->
    controlling_process(Client, Pid, ?TIMEOUT).

%% @doc: Make the given process (pid) the controlling process subscriber
%% with the given Timeout.
controlling_process(Client, Pid, Timeout) ->
    gen_server:call(Client, {controlling_process, Pid}, Timeout).


-spec ack_message(Client::pid()) -> ok.
%% @doc: acknowledge the receipt of a pubsub message. each pubsub
%% message must be acknowledged before the next one is received
ack_message(Client) ->
    gen_server:cast(Client, {ack_message, self()}).
