-module(eredis_sentinel_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SERVERS, [
                  {session1,  6380},
                  {session2,  6381},
                  {cache1,    6382},
                  {cache2,    6383}
                 ]).

-define(SENTINELS, [
                    {sentinel1, 26380},
                    {sentinel2, 26381}
                   ]).

-define(CONFIG, [{"localhost",26380}, {"localhost",26381}]).

-compile(export_all).

%% Suite initialization
start_cluster() ->
    [start_redis(Name) || {Name, _Port} <- ?SERVERS],
    [start_sentinel(Name) || {Name, _Port} <- ?SENTINELS],
    ok.

stop_cluster(_) ->
    kill_servers([sentinel1, sentinel2, session1, session2, cache1, cache2], 2000),
    ok.

%%% Working with redis cluster
start_redis(Name) ->
    run_cmd("redis-server ~s/redis_~s.conf", [code:priv_dir(eredis),Name]),
    ?assert(is_server_alive(Name)).

start_sentinel(Name) ->
    run_cmd("redis-sentinel ~p/redis_~s.conf ", [code:priv_dir(eredis), Name]),
    ?assert(is_server_alive(Name)).

run_cmd(CmdFmt, Args) ->
    Cmd = lists:flatten(io_lib:format(CmdFmt, Args)),
    os:cmd(Cmd).

%%% Test definition

sentinel_test_() ->
    case check_env() of
        ok ->
            error_logger:tty(false),
            Ts =
                [{setup,
                  fun start_cluster/0,
                  fun stop_cluster/1,
                  T
                 } || T <- tests() ],
            {inorder, Ts};
        {error, Error} ->
            [fun() ->
                     ?debugFmt("~n~nWARNING! SENTINEL TESTS ARE SKIPPED!~nError: ~s~n", [Error])
             end]
    end.

tests() ->
    [
     {"it returns sentinel_unreachable if no connections to sentinels",
      fun t_no_sentinel_connection/0},

     {"it returns master_unknown if no one sentinel knows about this cluster",
      fun t_master_unknown/0},

     {"it returns master_unreachable if no one sentinel knows master host for cluster",
      fun t_master_unreachable/0},

     {"it returns valid master host data",
      fun t_normal_operation/0},

     {"it uses the same connection for several master requests",
      fun t_connection_reuse/0},

     {"it connects to the next sentinel if current failed",
      fun t_failed_sentinel/0},

     {timeout, 30,
      {"it returns new master host/port on redis failover",
       fun t_failover/0}},

     {"eredis should understand sentine:master_name notation",
      fun t_eredis_support/0}

    ].


%%% Tests

t_no_sentinel_connection() ->
    {ok,_Pid} = eredis_sentinel:start_link(["unreachablehost"]),
    ?assertMatch({error, sentinel_unreachable}, eredis_sentinel:get_master(session)).

t_master_unknown() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({error, master_unknown}, eredis_sentinel:get_master(unknonwmaster)).

t_master_unreachable() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({error, master_unreachable}, eredis_sentinel:get_master(badmaster)).

t_normal_operation() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    ?assertMatch({ok, {"127.0.0.1", 6382}}, eredis_sentinel:get_master(cache)).

t_connection_reuse() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    {ok, {"localhost", 26380, ConnPid}} = eredis_sentinel:get_current_sentinel(),
    ?assertMatch({ok, {"127.0.0.1", 6382}}, eredis_sentinel:get_master(cache)),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    ?assertMatch({ok, {"localhost", 26380, ConnPid}}, eredis_sentinel:get_current_sentinel()).

t_failed_sentinel() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    {ok, {"localhost", 26380, ConnPid1}} = eredis_sentinel:get_current_sentinel(),

    eredis_sentinel_client:stop(ConnPid1),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    {ok, {"localhost", 26380, ConnPid2}} = eredis_sentinel:get_current_sentinel(),
    ?assert(ConnPid1 =/= ConnPid2),

    kill_servers([sentinel1], 2000),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session)),
    ?assertMatch({ok, {"localhost", 26381, _}}, eredis_sentinel:get_current_sentinel()).

t_eredis_support() ->
    {ok, _Pid} = eredis_sentinel:start_link(?CONFIG),
    {ok, Conn} = eredis:start_link("sentinel:session", 0),
    ?assertMatch({ok,[<<"port">>, <<"6380">>]}, eredis:q(Conn, ["config", "get", "port"])),
    {ok, Conn2} = eredis:start_link("sentinel:cache", 0),
    ?assertMatch({ok,[<<"port">>, <<"6382">>]}, eredis:q(Conn2, ["config", "get", "port"])).


t_failover() ->
    {ok,_Pid} = eredis_sentinel:start_link(?CONFIG),
    ?assertMatch({ok, {"127.0.0.1", 6380}}, eredis_sentinel:get_master(session, true)),
    {ok, Conn} = eredis:start_link("sentinel:session", 0),
    ?assertMatch({ok,[<<"port">>, <<"6380">>]}, eredis:q(Conn, ["config", "get", "port"])),

    % this sleep need to sentinels find out slaves of master
    timer:sleep(1000),
    % just change master, not kill it because real failover need up to 35 seconds to complete
    change_master(6380,6381),
    % waiting sentinels to see new master
    timer:sleep(2000),
    ?assertMatch({ok, {"127.0.0.1", 6381}}, eredis_sentinel:get_master(session)),
    ?assertMatch([{sentinel, {reconnect, session, "127.0.0.1", 6381}}], get_messages()),
    ?assert(is_process_alive(Conn)),
    wait_redis_connect(Conn, 2000),
    ?assertMatch({ok,[<<"port">>, <<"6381">>]}, eredis:q(Conn, ["config", "get", "port"])).



%%% Internal ----------------------------------------------------------

get_messages() ->
    get_messages([]).
get_messages(Acc) ->
    receive
        M ->
            get_messages([M|Acc])
    after 0 ->
            Acc
    end.

%% Killing external OS processes

kill_servers(Names, Timeout) ->
    [kill_server(N) || N <- Names],
    wait_server_die(Names, [], Timeout).

kill_server(Name) ->
    Cmd = lists:flatten(io_lib:format("kill $(cat ./redis_~s.pid)", [Name])),
    os:cmd(Cmd).

wait_server_die(Names, Acc, Timeout) when Timeout =< 0 ->
    throw({"Killing servers timeout", Names++Acc});
wait_server_die([], [], _) ->
    ok;
wait_server_die([], Names, Timeout) ->
    timer:sleep(100),
    kill_servers(Names, Timeout - 100);
wait_server_die([Name|Names], Acc, Timeout) ->
    case is_server_alive(Name) of
        true ->
            wait_server_die(Names, [Name | Acc ], Timeout);
        _ ->
            wait_server_die(Names, Acc, Timeout)
    end.

is_server_alive(Name) ->
    Cmd = lists:flatten(io_lib:format("kill -0 $(cat ./redis_~s.pid) && echo -n ok", [Name])),
    "ok" == os:cmd(Cmd).


%% Waiting redis client to connect to redis
wait_redis_connect(_Conn, Timeout) when Timeout =< 0 ->
    {error, "Waiting redis connection timeout"};
wait_redis_connect(Conn, Timeout) ->
    case eredis:q(Conn, ["PING"]) of
        {error, no_connection} ->
            timer:sleep(100),
            wait_redis_connect(Conn, Timeout - 100);
        {ok,<<"PONG">>} ->
            ok
    end.

%% Failover imitation
change_master(FromPort, ToPort) ->
    {ok, From} = eredis:start_link("localhost", FromPort),
    {ok, To} = eredis:start_link("localhost", ToPort),
    eredis:q(To, ["slaveof", "no", "one"]),
    eredis:q(From, ["slaveof", "localhost", ToPort]),
    ok.

%% Check that sentinel tests can be run
check_env() ->
    case {check_prog("redis-server"),check_prog("redis-sentinel")} of
        {"",""} ->
            ok;
        {E1, E2} ->
            {error, string:join([E || E <- [E1,E2], E =/= []], "\n")}
    end.

check_prog(ProgName) ->
    case os:cmd("which " ++ ProgName) of
        [] ->
            ProgName ++ " not found";
        _ ->
            ""
    end.
