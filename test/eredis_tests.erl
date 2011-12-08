-module(eredis_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../src/eredis_priv.hrl").

-import(eredis, [create_multibulk/1]).

get_set_test() ->
    C = c(),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", foo])),

    ?assertEqual({ok, undefined}, eredis:q(C, ["GET", foo])),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["SET", foo, bar])),
    ?assertEqual({ok, <<"bar">>}, eredis:q(C, ["GET", foo])).


delete_test() ->
    C = c(),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", foo])),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["SET", foo, bar])),
    ?assertEqual({ok, <<"1">>}, eredis:q(C, ["DEL", foo])),
    ?assertEqual({ok, undefined}, eredis:q(C, ["GET", foo])).

mset_mget_test() ->
    C = c(),
    Keys = lists:seq(1, 1000),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL" | Keys])),

    KeyValuePairs = [[K, K*2] || K <- Keys],
    ExpectedResult = [list_to_binary(integer_to_list(K * 2)) || K <- Keys],

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["MSET" | lists:flatten(KeyValuePairs)])),
    ?assertEqual({ok, ExpectedResult}, eredis:q(C, ["MGET" | Keys])),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL" | Keys])).

exec_test() ->
    C = c(),

    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k1", "b"])),
    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k1", "a"])),
    ?assertMatch({ok, _}, eredis:q(C, ["LPUSH", "k2", "c"])),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C, ["MULTI"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C, ["LRANGE", "k1", "0", "-1"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C, ["LRANGE", "k2", "0", "-1"])),

    ExpectedResult = [[<<"a">>, <<"b">>], [<<"c">>]],

    ?assertEqual({ok, ExpectedResult}, eredis:q(C, ["EXEC"])),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL", "k1", "k2"])).

exec_nil_test() ->
    C1 = c(),
    C2 = c(),

    ?assertEqual({ok, <<"OK">>}, eredis:q(C1, ["WATCH", "x"])),
    ?assertMatch({ok, _}, eredis:q(C2, ["INCR", "x"])),
    ?assertEqual({ok, <<"OK">>}, eredis:q(C1, ["MULTI"])),
    ?assertEqual({ok, <<"QUEUED">>}, eredis:q(C1, ["GET", "x"])),
    ?assertEqual({ok, undefined}, eredis:q(C1, ["EXEC"])),
    ?assertMatch({ok, _}, eredis:q(C1, ["DEL", "x"])).

pipeline_test() ->
    C = c(),

    P1 = [["SET", a, "1"],
          ["LPUSH", b, "3"],
          ["LPUSH", b, "2"]],

    ?assertEqual([{ok, <<"OK">>}, {ok, <<"1">>}, {ok, <<"2">>}],
                 eredis:qp(C, P1)),

    P2 = [["MULTI"],
          ["GET", a],
          ["LRANGE", b, "0", "-1"],
          ["EXEC"]],

    ?assertEqual([{ok, <<"OK">>},
                  {ok, <<"QUEUED">>},
                  {ok, <<"QUEUED">>},
                  {ok, [<<"1">>, [<<"2">>, <<"3">>]]}],
                 eredis:qp(C, P2)),

    ?assertMatch({ok, _}, eredis:q(C, ["DEL", a, b])).

pipeline_mixed_test() ->
    C = c(),
    P1 = [["LPUSH", c, "1"] || _ <- lists:seq(1, 100)],
    P2 = [["LPUSH", d, "1"] || _ <- lists:seq(1, 100)],
    Expect = [{ok, list_to_binary(integer_to_list(I))} || I <- lists:seq(1, 100)],
    spawn(fun () ->
                  erlang:yield(),
                  ?assertEqual(Expect, eredis:qp(C, P1))
          end),
    spawn(fun () ->
                  ?assertEqual(Expect, eredis:qp(C, P2))
          end),
    timer:sleep(10),
    ?assertMatch({ok, _}, eredis:q(C, ["DEL", c, d])).


c() ->
    Res = eredis:start_link(),
    ?assertMatch({ok, _}, Res),
    {ok, C} = Res,
    C.



multibulk_test_() ->
    [?_assertEqual(<<"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>,
                   list_to_binary(create_multibulk(["SET", "foo", "bar"]))),
     ?_assertEqual(<<"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>,
                   list_to_binary(create_multibulk(['SET', foo, bar]))),

     ?_assertEqual(<<"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n">>,
                   list_to_binary(create_multibulk(['SET', foo, 123]))),

     ?_assertThrow({cannot_store_floats, 123.5},
                   list_to_binary(create_multibulk(['SET', foo, 123.5])))
    ].



pubsub_test() ->
    Pub = c(),
    Sub = c(),
    ok = eredis:controlling_process(Sub),
    Res1 = eredis:q(Sub, ["SUBSCRIBE", chan]),
    ?assertEqual({ok, [<<"subscribe">>, <<"chan">>, <<"1">>]}, Res1),
    Res2 = eredis:q(Pub, ["PUBLISH", chan, msg]),
    ?assertEqual({ok, <<"1">>}, Res2),
    Msg = receive
              {message, _, _, _} = InMsg ->
                  InMsg
          end,
    ?assertEqual({message, <<"chan">>, <<"msg">>, Sub}, Msg).


pubsub_manage_subscribers_test() ->
    Pub = c(),
    Sub = c(),
    unlink(Sub),
    eredis:q(Sub, ["SUBSCRIBE", chan]),
    #state{controlling_process=undefined} = get_state(Sub),
    S1 = subscriber(Sub),
    ok = eredis:controlling_process(Sub, S1),
    #state{controlling_process={_, S1}} = get_state(Sub),
    S2 = subscriber(Sub),
    ok = eredis:controlling_process(Sub, S2),
    #state{controlling_process={_, S2}} = get_state(Sub),
    eredis:q(Pub, ["PUBLISH", chan, msg1]),
    S1 ! stop,
    ok = wait_for_stop(S1),
    eredis:q(Pub, ["PUBLISH", chan, msg2]),
    M2 = wait_for_msg(S2),
    ?assertEqual(M2, {message, <<"chan">>, <<"msg1">>, Sub}),
    M3 = wait_for_msg(S2),
    ?assertEqual(M3, {message, <<"chan">>, <<"msg2">>, Sub}),
    S2 ! stop,
    ok = wait_for_stop(S2),
    Ref = erlang:monitor(process, Sub),
    receive {'DOWN', Ref, process, Sub, _} -> ok end.


pubsub_connect_disconnect_messages_test() ->
    Pub = c(),
    Sub = c(),
    eredis:q(Sub, ["SUBSCRIBE", chan]),
    S = subscriber(Sub),
    ok = eredis:controlling_process(Sub, S),
    eredis:q(Pub, ["PUBLISH", chan, msg]),
    wait_for_msg(S),
    #state{socket=Sock} = get_state(Sub),
    gen_tcp:close(Sock),
    Sub ! {tcp_closed, Sock},
    M1 = wait_for_msg(S),
    ?assertEqual({eredis_disconnected, Sub}, M1),
    M2 = wait_for_msg(S),
    ?assertEqual({eredis_connected, Sub}, M2).


subscriber(Client) ->
    Test = self(),
    Pid = spawn(fun () -> subscriber(Client, Test) end),
    spawn(fun() ->
                  Ref = erlang:monitor(process, Pid),
                  receive
                      {'DOWN', Ref, _, _, _} ->
                          Test ! {stopped, Pid}
                  end
          end),
    Pid.

subscriber(Client, Test) ->
    receive
        stop ->
            ok;
        Msg ->
            Test ! {got_message, self(), Msg},
            eredis:ack_message(Client),
            subscriber(Client, Test)
    end.

wait_for_msg(Subscriber) ->
    receive
        {got_message, Subscriber, Msg} ->
            Msg
    end.

wait_for_stop(Subscriber) ->
    receive
        {stopped, Subscriber} ->
            ok
    end.

get_state(Pid)
  when is_pid(Pid) ->
    {status, _, _, [_, _, _, _, State]} = sys:get_status(Pid),
    get_state(State);
get_state([{data, [{"State", State}]} | _]) ->
    State;
get_state([_|Rest]) ->
    get_state(Rest).
