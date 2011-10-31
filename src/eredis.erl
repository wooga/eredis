%%
%% Erlang Redis client
%%
%% Usage:
%%   {ok, Client} = eredis:start_link().
%%   {ok, <<"OK">>} = eredis:q(["SET", "foo", "bar"]).
%%   {ok, <<"bar">>} = eredis:q(["GET", "foo"]).

-module(eredis).
-author('knut.nesheim@wooga.com').

-include("eredis.hrl").

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start_link/0, start_link/1, start_link/2, start_link/3, start_link/4,
         start_link/5, q/2, q/3, qp/2, qp/3,
         controlling_process/1, controlling_process/2, controlling_process/3,
         ack_message/1]).

%% Exported for testing
-export([create_multibulk/1]).

%%
%% PUBLIC API
%%

start_link() ->
    start_link("127.0.0.1", 6379, 0, "").

start_link(Host, Port) ->
    start_link(Host, Port, 0, "").

start_link(Host, Port, Database) ->
    start_link(Host, Port, Database, "").

start_link(Host, Port,  Database, Password) ->
    start_link(Host, Port, Database, Password, 100).

start_link(Host, Port, Database, Password, ReconnectSleep)
  when is_list(Host);
       is_integer(Port);
       is_integer(Database);
       is_list(Password);
       is_integer(ReconnectSleep) ->

    eredis_client:start_link(Host, Port, Database, Password, ReconnectSleep).

%% @doc: Callback for starting from poolboy
-spec start_link(server_args()) -> {ok, Pid::pid()} | {error, Reason::term()}.
start_link(Args) ->
    Host           = proplists:get_value(host, Args, "127.0.0.1"),
    Port           = proplists:get_value(port, Args, 6379),
    Database       = proplists:get_value(database, Args, 0),
    Password       = proplists:get_value(password, Args, ""),
    ReconnectSleep = proplists:get_value(reconnect_sleep, Args, 100),
    start_link(Host, Port, Database, Password, ReconnectSleep).


-spec q(Client::pid(), Command::iolist()) ->
               {ok, return_value()} | {error, Reason::binary() | no_connection}.
%% @doc: Executes the given command in the specified connection. The
%% command must be a valid Redis command and may contain arbitrary
%% data which will be converted to binaries. The returned values will
%% always be binaries.
q(Client, Command) ->
    call(Client, Command, ?TIMEOUT).

q(Client, Command, Timeout) ->
    call(Client, Command, Timeout).


-spec qp(Client::pid(), Pipeline::pipeline()) ->
                [{ok, return_value()} | {error, Reason::binary()}] |
                {error, no_connection}.
%% @doc: Executes the given pipeline (list of commands) in the
%% specified connection. The commands must be valid Redis commands and
%% may contain arbitrary data which will be converted to binaries. The
%% values returned by each command in the pipeline are returned in a list.
qp(Client, Pipeline) ->
    pipeline(Client, Pipeline, ?TIMEOUT).

qp(Client, Pipeline, Timeout) ->
    pipeline(Client, Pipeline, Timeout).


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
%% Note that you must still issue SUBSCRIBE or PSUBSCRIBE redis
%% commands to receive pubsub messages. Also, once you issue a
%% SUBSCRIBE or PSUBSCRIBE command, that eredis client may only be
%% used to add or remove pubsub subscriptions and to receive pubsub
%% messages. That is how Redis pubsub works, it is not an artifact
%% of eredis.
%%
%% Any message of the form {message, _, _, _} must be acknowledged
%% before any subsequent message of the same form is sent. This
%% prevents the controlling process from being overrun with redis
%% pubsub messages. See ack_message/2 below.
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


%%
%% INTERNAL HELPERS
%%

call(Client, Command, Timeout) ->
    Request = {request, create_multibulk(Command)},
    gen_server:call(Client, Request, Timeout).

pipeline(_Client, [], _Timeout) ->
    [];
pipeline(Client, Pipeline, Timeout) ->
    Request = {pipeline, [create_multibulk(Command) || Command <- Pipeline]},
    gen_server:call(Client, Request, Timeout).

-spec create_multibulk(Args::iolist()) -> Command::iolist().
%% @doc: Creates a multibulk command with all the correct size headers
create_multibulk(Args) ->
    ArgCount = [<<$*>>, integer_to_list(length(Args)), <<?NL>>],
    ArgsBin = lists:map(fun to_bulk/1, lists:map(fun to_binary/1, Args)),

    [ArgCount, ArgsBin].

to_bulk(B) when is_binary(B) ->
    [<<$$>>, integer_to_list(iolist_size(B)), <<?NL>>, B, <<?NL>>].

%% @doc: Convert given value to binary. Fallbacks to
%% term_to_binary/1. For floats, throws {cannot_store_floats, Float}
%% as we do not want floats to be stored in Redis. Your future self
%% will thank you for this.
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_binary(X)  -> X;
to_binary(X) when is_integer(X) -> list_to_binary(integer_to_list(X));
to_binary(X) when is_float(X)   -> throw({cannot_store_floats, X});
to_binary(X)                    -> term_to_binary(X).
