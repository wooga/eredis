%% ADT for sentinel masters data.
%% Keeps current masters host/port and list of subscribers.
%% Notifies subscribers when master changes.

-module(eredis_sentinel_masters).
-author("Mikl Kurkov <mkurkov@gmail.com>").

%% Imports
-import(lists, [sort/1]).

%% API
-export([new/0, update/4, subscribe/3, unsubscribe/2]).

%% Records
-record(master, {
          name :: master_name(),
          host :: master_host(),
          port :: master_port(),
          pids :: [pid()]}).

%% Types
-type master_host() :: string().
-type master_port() :: integer().
-type master_name() :: atom().
-type masters() :: [#master{}].

%%% API ---------------------------------------------------------------

%% @doc Masters initialization
-spec new() -> {ok, masters()}.
new() ->
    {ok,[]}.

%% @doc Add new master or update if it already exists
-spec update(masters(), master_name(), master_host(), master_port()) -> {ok,masters()}.
update(Masters, MasterName, Host, Port)
  when is_list(Masters),
       is_atom(MasterName),
       is_list(Host),
       is_integer(Port)
  ->
    case find(Masters, MasterName) of
        {ok, Master} ->
            NewMaster = update_master(Master, Host, Port);
        undefined ->
            NewMaster = new_master(MasterName, Host, Port)
    end,
    {ok, set_master(Masters, NewMaster)}.

%% @doc Subscribe process to master updates.
-spec subscribe(masters(), master_name(), pid()) -> {ok, masters()} | {error, no_master_found}.
subscribe(Masters, MasterName, Pid)
  when is_list(Masters), is_atom(MasterName), is_pid(Pid) ->
    case find(Masters, MasterName) of
        {ok, Master} ->
            NewMaster = add_pid(Master, Pid),
            {ok, set_master(Masters, NewMaster)};
        undefined ->
            {error, no_master_found}
    end.

%% @doc Unsubscribe process from all masters.
-spec unsubscribe(masters(), pid()) -> {ok, masters()}.
unsubscribe(Masters, Pid)
  when is_list(Masters), is_pid(Pid) ->
    RemovePid = fun(M) -> rm_pid(M, Pid) end,
    {ok, lists:map(RemovePid, Masters)}.


%%% Internal ----------------------------------------------------------

new_master(MasterName, Host, Port) ->
    #master{name = MasterName, host = Host, port = Port, pids =[]}.

find(Masters,MasterName) ->
    case lists:keysearch(MasterName, #master.name, Masters) of
        {value, Master} ->
            {ok, Master};
        false ->
            undefined
    end.

set_master(Masters, Master) ->
    lists:keystore(Master#master.name, #master.name, Masters, Master).

update_master(#master{host=Host, port=Port}=Master, Host, Port) ->
    Master;
update_master(Master, Host, Port) ->
    MasterNew = Master#master{host=Host,port=Port},
    notify_pids(MasterNew),
    MasterNew.

notify_pids(#master{pids=Pids, name=Name, host=Host, port=Port}) ->
    Message = {sentinel, {reconnect, Name, Host, Port}},
    [ P ! Message || P <- Pids],
    ok.

add_pid(#master{pids=Pids} = Master, Pid) ->
    Master#master{pids = lists:umerge(Pids, [Pid])}.

rm_pid(#master{pids=Pids} = Master, Pid) ->
    Master#master{pids = Pids -- [Pid]}.


%%% Tests -------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

two_masters() ->
    {ok, M0} = new(),
    {ok, M1} = update(M0, master1, "host1", 1),
    {ok, M2} = update(M1, master2, "host2", 2),
    M2.

add_master_test() ->
    {ok, M3} = update(two_masters(), master3, "host3", 3),
    ?assertMatch(
       [#master{name=master1, host="host1", port=1, pids=[]},
        #master{name=master2, host="host2", port=2, pids=[]},
        #master{name=master3, host="host3", port=3, pids=[]}
       ],
       sort(M3)).

update_master_host_test() ->
    {ok, M3} = update(two_masters(), master2, "host22", 2),
    ?assertMatch(
       [#master{name=master1, host="host1", port=1, pids=[]},
        #master{name=master2, host="host22", port=2, pids=[]}],
       sort(M3)).

update_master_port_test() ->
    {ok, M3} = update(two_masters(), master2, "host2", 22),
    ?assertMatch(
       [#master{name=master1, host="host1", port=1, pids=[]},
        #master{name=master2, host="host2", port=22, pids=[]}],
       sort(M3)).

find_test() ->
    ?assertMatch(undefined,
                 find(two_masters(), master3)),
    ?assertMatch({ok, #master{name=master1, host="host1", port=1, pids=[]}},
                 find(two_masters(), master1)),
    ?assertMatch({ok, #master{name=master2, host="host2", port=2, pids=[]}},
                 find(two_masters(), master2)).

subscribe_test() ->
    Pid = self(),
    ?assertMatch({error, no_master_found},
                 subscribe(two_masters(), master3, self())),
    ?assertMatch([#master{name=master1,host="host1",port=1, pids=[Pid]},
                  #master{name=master2, host="host2", port=2, pids=[]}],
                 sort(ok(subscribe(two_masters(), master1, Pid)) )),
    ?assertMatch([#master{name=master1,host="host1",port=1, pids=[Pid]},
                  #master{name=master2, host="host2", port=2, pids=[]}],
                 sort(ok(subscribe(
                           ok(subscribe(two_masters(), master1, Pid)),
                           master1, Pid)))).

unsubscribe_test() ->
    Pid1 = list_to_pid("<0.1.0>"),
    Pid2 = list_to_pid("<0.2.0>"),
    {ok,Ms1} = subscribe(two_masters(), master1, Pid1),
    {ok,Ms2} = subscribe(Ms1, master2, Pid2),
    {ok,Ms3} = unsubscribe(Ms2, Pid1),
    ?assertMatch([#master{name=master1, host="host1", port=1, pids=[]},
                  #master{name=master2, host="host2", port=2, pids=[Pid2]}],
                 sort(Ms3)).

update_notify_test() ->
    Pid = self(),
    {ok, Ms1} = subscribe(two_masters(), master1, Pid),
    {ok, Ms2} = update(Ms1, master1, "host1", 1),
    ?assertMatch([], get_messages()),

    {ok, Ms3} = update(Ms2, master1, "host11", 1),
    ?assertMatch(ok, get_message({sentinel, {reconnect, master1, "host11", 1}})),
    ?assertMatch([], get_messages()),

    {ok, _} = update(Ms3, master1, "host11", 2),
    ?assertMatch(ok, get_message({sentinel, {reconnect, master1, "host11", 2}})),
    ?assertMatch([], get_messages()).


%%% Test helpers ------------------------------------------------------

ok({ok,S})-> S.

get_message(Message) ->
    receive
        Message ->
            ok
    after 0 ->
            {no_message, Message}
    end.

get_messages() ->
    {messages, Res} = erlang:process_info(self(), messages),
    Res.


-endif.
