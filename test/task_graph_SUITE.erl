-module(task_graph_SUITE).

%% Common test callbacks:
-export([ suite/0
        , all/0
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

%% Testcases:
-export([ t_evt_draw_deps/1
        , t_evt_build_flow/1
        , t_topology_succ/1
        , t_topology/1
        , t_error_handling/1
        , t_resources/1
        , t_deferred/1
        , t_guards/1
        , t_no_guards/1
        , t_proper_only/1
        , t_keep_going/1
        ]).

%% gen_event callbacks:
-export([ init/1
        , handle_event/2
        , handle_call/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%%===================================================================
%%% Macros
%%%===================================================================

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("task_graph/src/task_graph_int.hrl").
-include("task_graph_test.hrl").

-define(TIMEOUT, 1200).
-define(NUMTESTS, 1000).
-define(SIZE, 1000).

-define(SHEDULE_STATS_TABLE, tg_SUITE_stats_sched_table).
-define(EXPAND_STATS_TABLE, tg_SUITE_stats_ext_table).
-define(STATS_RESOLUTION, 200).

-define(RUN_PROP(PROP, SIZE),
        begin
            %% Workaround against CT's "wonderful" features:
            OldGL = group_leader(),
            group_leader(whereis(user), self()),
            T0 = erlang:system_time(?tg_timeUnit),
            io:format(user, ??PROP, []),
            catch ets:delete(?SHEDULE_STATS_TABLE),
            ets:new(?SHEDULE_STATS_TABLE, [named_table]),
            catch ets:delete(?EXPAND_STATS_TABLE),
            ets:new(?EXPAND_STATS_TABLE,  [named_table]),
            Result = proper:quickcheck( PROP()
                                      , [ {numtests, ?NUMTESTS}
                                        , {max_size, SIZE}
                                        ]
                                      ),
            group_leader(OldGL, self()),
            catch analyse_statistics(),
            T1 = erlang:system_time(?tg_timeUnit),
            io:format(user, "Testcase ran for ~p ms~n", [T1 - T0]),
            true = Result
        end).

-define(RUN_PROP(PROP), ?RUN_PROP(PROP, ?SIZE)).

%%%===================================================================
%%% Testcases and properties
%%%===================================================================

%% Test that dependencies are resolved in a correct order and all
%% tasks are executed for any proper acyclic graph
t_topology_succ(_Config) ->
    ?RUN_PROP(topology_succ),
    ok.

topology_succ() ->
    ?FORALL(DAG, dag(),
            begin
                Opts = #{event_handlers => [send_back()]},
                {ok, _} = task_graph:run_graph(foo, Opts, DAG),
                Events = collect_events(),
                collect_statistics(Events),
                Tasks = collect_all_tasks(DAG),
                check_topology(Tasks, Events)
            end).

%% Test that all dynamic tasks are executed in a correct order
t_deferred(_Config) ->
    ?RUN_PROP(deferred, 100),
    ok.

deferred() ->
    ?FORALL(DAG0, dag(inject_deferred()),
            begin
                {Vertices0, Edges} = DAG0,
                Vertices = lists:map( fun(T = #tg_task{ id = Id
                                                      , data = {deferred, Data}
                                                      }) ->
                                              T#tg_task{data = {deferred, tag_ids(Id, Data)}
                                                       };
                                         (T) ->
                                              T
                                      end
                                    , Vertices0
                                    ),
                DAG = {Vertices, Edges},
                Opts = #{event_handlers => [send_back()]},
                {ok, _} = task_graph:run_graph(foo, Opts, DAG),
                Events = collect_events(),
                collect_statistics(Events),
                Tasks = collect_all_tasks(DAG),
                check_topology(Tasks, Events)
            end).

%% Test that cyclic dependencies can be detected
t_topology(_Config) ->
    ?RUN_PROP(topology),
    ok.

topology() ->
    ?FORALL(L0, list({nat(), nat()}),
            begin
                %% Shrink vertex space a little to get more interesting graphs:
                L1 = [{N div 2, M div 2} || {N, M} <- L0],
                Edges = lists:filter(fun({A, B}) -> A /= B end, L1),
                Vertices = lists:flatten([tuple_to_list(I) || I <- Edges]),
                DG = digraph:new(),
                lists:foreach( fun(V) ->
                                       digraph:add_vertex(DG, V)
                               end
                             , Vertices
                             ),
                lists:foreach( fun({From, To}) ->
                                       digraph:add_edge(DG, From, To)
                               end
                             , Edges
                             ),
                Acyclic = digraph_utils:is_acyclic(DG),
                digraph:delete(DG),
                Vertices2 = [#tg_task{ id = I
                                     , execute = test_worker
                                     , data = #{deps => []}
                                     } || I <- Vertices],
                Result = task_graph:run_graph(foo, {Vertices2, Edges}),
                case {Acyclic, Result} of
                    {true, {ok, _}} ->
                        true;
                    {false, {error, {topology_error, _}}} ->
                        true
                end
            end).

%% Check that errors in the tasks are reported
t_error_handling(_Config) ->
    ?RUN_PROP(error_handling),
    ok.

error_handling() ->
    ?FORALL(DAG, dag(inject_errors()),
            ?IMPLIES(expected_errors(DAG) /= [],
            begin
                ExpectedErrors = expected_errors(DAG),
                {error, Result} = task_graph:run_graph(foo, DAG),
                map_sets:is_subset( Result
                                  , map_sets:from_list(ExpectedErrors)
                                  ) orelse
                    error({Result, 'is not a subset of', ExpectedErrors})
            end)).

t_keep_going(_Config) ->
    N = 10,
    Vertices = [ #tg_task{id = I, execute = test_worker, data = error}
                 || I <- lists:seq(1, N)
               ],
    Edges = [],
    Settings = #{ keep_going => true
                , event_handlers => [send_back()]
                },
    {error, _} = task_graph:run_graph(foo, Settings, {Vertices, Edges}),
    Events = collect_events(),
    %% Assert:
    N = length(events_of_kind([spawn_task], Events)),
    ok.


%% Check that resource constraints are respected
t_resources(_Config) ->
    ?RUN_PROP(resources),
    ok.

resources() ->
    MaxCapacity = 20,
    MaxNumberOfResorces = 10,
    ?FORALL(
       {NResources, Capacity, DAG0},
       ?LET(NResources, range(1, MaxNumberOfResorces),
            { NResources
            , range(1, MaxCapacity)
            , dag(list(range(1, NResources * 2)))
            }),
       begin
           ResourceIds = lists:seq(1, NResources),
           Limits = maps:from_list([{I, Capacity} || I <- ResourceIds]),
           {Vertices0, Edges} = DAG0,
           Vertices =
               [T0#tg_task{ data = #{}
                          , resources = RR
                          }
                || T0 = #tg_task{data = RR} <- Vertices0],
           DAG = {Vertices, Edges},
           Opts = #{ event_handlers => [send_back()]
                   , resources => Limits
                   },
           {ok, _} = task_graph:run_graph(foo, Opts, DAG),
           Events = collect_events(),
           collect_statistics(Events),
           Tasks = collect_all_tasks(DAG),
           lists:foldl( check_resource_usage(Tasks, Limits)
                      , Limits
                      , Events
                      ),
           true
       end).

%% Check that task spawn events are reported, collected and processed
%% to nice graphs
t_evt_draw_deps(_Config) ->
    Filename = "test.dot",
    DynamicTask = #tg_task{ id = dynamic
                          , execute = fun(_,_,_) -> {ok, {}} end
                          },
    Exec = fun(1, _, _) ->
                   {ok, {}, {[DynamicTask], [{1, dynamic}]}};
              (_, _, _) ->
                   {ok, {}}
           end,
    Opts = #{event_handlers =>
                 [{task_graph_draw_deps, #{ filename => Filename
                                          , style    => fun(_) -> "color=green shape=oval" end
                                          , preamble => "preamble"
                                          }}]},
    Tasks = [ #tg_task{id="foo", execute=Exec}
            , #tg_task{id=1, execute=Exec}
            ],
    Deps = [{"foo", 1}],
    {ok, _} = task_graph:run_graph(foo, Opts, {Tasks, Deps}),
    %% TODO: Make it order-agnostic
    Bin = <<"digraph task_graph {\n"
            "preamble\n"
            "  1[color=green shape=oval];\n"
            "  \"foo\"[color=green shape=oval];\n"
            "  \"foo\" -> 1;\n"
            "  dynamic[color=green shape=oval];\n"
            "  1 -> dynamic;\n"
            "}\n">>,
    {ok, Bin} = file:read_file(Filename),
    ok.

%% Check that build log is written
t_evt_build_flow(_Config) ->
    Filename = "build_flow.log",
    Opts = #{event_handlers =>
                 [{task_graph_flow, #{ filename => Filename
                                     }}]},
    Tasks = [#tg_task{ id = I
                     , execute = fun(_,_,_) -> {ok, {}} end
                     , data = #{}
                     }
             || I <- lists:seq(1, 3)],
    Deps = [{1, 2}, {2, 3}, {1, 3}],
    {ok, _} = task_graph:run_graph(foo, Opts, {Tasks, Deps}),
    Bin = <<"TS spawn 1\n"
            "TS complete_task 1\n"
            "TS spawn 2\n"
            "TS complete_task 2\n"
            "TS spawn 3\n"
            "TS complete_task 3\n"
          >>,
    {ok, Bin0} = file:read_file(Filename),
    Bin = re:replace( Bin0
                    , <<"^[ 0-9]+:">>
                    , <<"TS">>
                    , [multiline, global, {return, binary}]
                    ),
    ok.

t_guards(_Config) ->
    ?RUN_PROP(guards),
    ok.

guards() ->
    ?FORALL(DAG, dag({guard, boolean()}),
            begin
                Opts = #{event_handlers => [send_back()]},
                {ok, _} = task_graph:run_graph(foo, Opts, DAG),
                Events = collect_events(),
                Tasks = collect_all_tasks(DAG),
                Executed = lists:sort([Id || #tg_event{kind = spawn_task, data = Id} <- Events]),
                Expected = lists:sort([Id || Id <- maps:keys(Tasks),
                                             is_task_changed(Id, Tasks)
                                      ]),
                Expected == Executed orelse error({ 'Expected:', Expected
                                                  , ', got:', Executed
                                                  }),
                true
            end).

%% Test that `disable_guards' flag forces running all jobs:
t_no_guards(_Config) ->
    DAG = { [#tg_task{ id = 0
                     , execute = test_worker
                     , data = {guard, true}
                     }]
          , []
          },
    Opts = #{ event_handlers => [send_back()]
            , disable_guards => true
            },
    {ok, _} = task_graph:run_graph(foo, Opts, DAG),
    Events = collect_events(),
    %% Assert:
    [ok] = [ok || #tg_event{kind = spawn_task, data = 0} <- Events],
    ok.

proper_only() ->
    ?FORALL(DAG, dag({guard, boolean()}), true).

t_proper_only(_Config) ->
    ?RUN_PROP(proper_only),
    ok.

%%%===================================================================
%%% Proper generators
%%%===================================================================

%% Proper generator of directed acyclic graphs:
dag() ->
    dag(default).
%% ...supply custom data to tasks (`Payload' is a proper generator)
dag(Payload) ->
    dag(Payload, 2, 4).
%% ...adjust magic parameters X and Y governing graph topology:
dag(Payload, X, Y) ->
    ?LET(Mishmash, list({{nat(), nat()}, {Payload, Payload}}),
          begin
              {L0, Data0} = lists:unzip(Mishmash), %% Separate vertex IDs and data
              %% Shrink vertex space to get more interesting graphs:
              L = [{N div X, M div Y} || {N, M} <- L0],
              Edges = [{N, N + M} || {N, M} <- L, M>0],
              Singletons = [N || {N, 0} <- L],
              Vertices = lists:usort( lists:append([tuple_to_list(I) || I <- Edges]) ++
                                      Singletons
                                    ),
              Data = lists:sublist( lists:append([tuple_to_list(I) || I <- Data0])
                                  , length(Vertices)
                                  ),
              Tasks = [#tg_task{ id = I
                               , data = P
                               , execute = test_worker
                               }
                       || {I, P} <- lists:zip(Vertices, Data)],
              {Tasks, Edges}
          end).

inject_errors() ->
    frequency([ {1, error}
              , {1, exception}
              , {5, ok}
              ]).

inject_deferred() ->
    frequency([ {2, ok}
              , {1, {deferred, dag()}}
              ]).

%%%===================================================================
%%% Utility functions:
%%%===================================================================

collect_statistics(Events) ->
    NTasks = length(events_of_kind([spawn_task], Events)),
    Key = (NTasks div ?STATS_RESOLUTION) * ?STATS_RESOLUTION,
    push_event_duration(shed_begin, shed_end, ?SHEDULE_STATS_TABLE, Key, Events),
    push_event_duration(extend_begin, extend_end, ?EXPAND_STATS_TABLE, Key, Events),
    ok.

analyse_statistics() ->
    case ets:first(?SHEDULE_STATS_TABLE) of
        '$end_of_table' ->
            io:format(user, "No statistics.~n", []),
            ok;
        _ ->
            io:format(user, "Scheduling:~n", []),
            analyse_statistics(?SHEDULE_STATS_TABLE),
            io:format(user, "Extention:~n", []),
            analyse_statistics(?EXPAND_STATS_TABLE)
    end.

analyse_statistics(Table) ->
    Stats0 = [{Key, bear:get_statistics(Vals)}
              || {Key, Vals} <- lists:keysort(1, ets:tab2list(Table))
             ],
    Stats = lists:filter( fun({_, Val}) ->
                              proplists:get_value(n, Val) > 0
                          end
                        , Stats0
                        ),
    io:format(user, "     N    min      max       avg~n", []),
    lists:foreach( fun({Key, Stats}) ->
                       io:format(user, "~6b ~f ~f ~f~n",
                                 [ Key
                                 , proplists:get_value(min, Stats) * 1.0
                                 , proplists:get_value(max, Stats) * 1.0
                                 , proplists:get_value(arithmetic_mean, Stats) * 1.0
                                 ])
                   end
                 , Stats
                 ),
    {_, Last} = lists:last(Stats),
    io:format(user, "Stats:~n~p~n", [Last]).

push_event_duration(K1, K2, Table, Key, Events) ->
    L1 = events_of_kind([K1], Events),
    L2 = events_of_kind([K2], Events),
    Dt = lists:sum(lists:zipwith( fun( #tg_event{timestamp = T1}
                                     , #tg_event{timestamp = T2}
                                     ) ->
                                      T2 - T1
                                  end
                                , L1
                                , L2
                                )),
    true = Dt >= 0,
    case ets:lookup(Table, Key) of
      [{Key, OldVals}] ->
          ok;
      [] ->
          OldVals = []
    end,
    ets:insert(Table, {Key, [Dt] ++ OldVals}).

events_of_kind(Kinds, Events) ->
    [E || E = #tg_event{kind = Kind} <- Events, lists:member(Kind, Kinds)].

collect_all_tasks(DAG) ->
    collect_all_tasks(DAG, #{}).

collect_all_tasks({Vertices, Edges}, Acc0) ->
    Acc1 = maps:merge( Acc0
                     , maps:from_list([{Task#tg_task.id, {Task, #{}}}
                                       || Task <- Vertices
                                      ])
                     ),
    Acc2 = lists:foldl( fun({From, To}, Acc) ->
                                AddDeps =
                                    fun({T, Old}) when is_map(Old) ->
                                            {T, map_sets:add_element(From, Old)}
                                    end,
                                maps:update_with(To, AddDeps, Acc)
                        end
                      , Acc1
                      , Edges
                      ),
    Deferred = [DAG || #tg_task{data = {deferred, DAG}} <- Vertices],
    lists:foldl( fun collect_all_tasks/2
               , Acc2
               , Deferred
               ).

check_resource_usage(Tasks, Limits) ->
    fun(#tg_event{kind = Kind, data = EvtData}, Resources) ->
            case Kind of
                spawn_task ->
                    #{EvtData := {Task, _Deps}} = Tasks,
                    RR = lists:usort(Task#tg_task.resources),
                    Resources2 = dec_counters(RR, Resources),
                    %% Check that resource capacities are always respected
                    %% Assert:
                    lists:all( fun(V) -> V >= 0 end
                             , [V || {K, V} <- maps:to_list(Resources2)
                                             , maps:is_key(K, Limits)
                               ]
                             ) orelse error({ 'All resources should be >= 0:'
                                            , Resources2
                                            }),
                    Resources2;
                complete_task ->
                    #{EvtData := {Task, _Deps}} = Tasks,
                    RR = lists:usort(Task#tg_task.resources),
                    Resources2 = inc_counters(RR, Resources),
                    Resources2;
                _ ->
                    Resources
            end
    end.

check_topology(Tasks, Events) ->
    {Tasks, RanTimes} = lists:foldl( fun check_topology_/2
                                   , {Tasks, #{}}
                                   , Events
                                   ),
    %% Check that all tasks have been executed exactly once:
    lists:foreach( fun(Key) ->
                           Val = maps:get(Key, RanTimes, 0),
                           1 == Val orelse
                               begin
                                   io:format(user, "Events: ~p~n", [Events]),
                                   error({'Task', Key, 'ran', Val, 'times instead of 1'})
                               end
                   end
                 , maps:keys(Tasks)
                 ),
    true.
check_topology_(#tg_event{kind = Kind, data = Evt}, {Tasks, RanTimes}) ->
    case Kind of
        spawn_task ->
            #{Evt := {_, Deps}} = Tasks,
            %% Assert:
            lists:foreach( fun(DepId) ->
                                   Val = maps:get(DepId, RanTimes, undefined),
                                   Val == 1 orelse
                                       error({ DepId, ', dependency of'
                                             , Evt, 'ran'
                                             , Val, 'times instead of 1'
                                             })
                           end
                         , map_sets:to_list(Deps)
                         ),
            {Tasks, RanTimes};
        complete_task ->
            RanTimes2 = inc_counters([Evt], RanTimes),
            {Tasks, RanTimes2};
        _ ->
            {Tasks, RanTimes}
    end.

expected_errors(DAG) ->
    {Vertices, _} = DAG,
    [Id || #tg_task{id = Id, data = D} <- Vertices,
           D =:= error orelse D =:= exception].

send_back() ->
    {?MODULE, [self()]}.

collect_events() ->
    collect_events([]).

collect_events(L) ->
    receive
        Evt = #tg_event{} ->
            collect_events([Evt|L])
    after 0 ->
            lists:reverse(L)
    end.

inc_counters(Keys, Map) ->
    lists:foldl( fun(Key, Acc) ->
                         maps:update_with(Key, fun(V) -> V + 1 end, 1, Acc)
                 end
               , Map
               , Keys
               ).

dec_counters(Keys, Map) ->
    lists:foldl( fun(Key, Acc) ->
                         maps:update_with(Key, fun(V) -> V - 1 end, -1, Acc)
                 end
               , Map
               , Keys
               ).

tag_ids(Tag, {Vertices, Edges}) ->
    { [T#tg_task{ id = {Tag, T#tg_task.id}} || T <- Vertices]
    , [{{Tag, A}, {Tag, B}} || {A, B} <- Edges]
    }.

is_task_changed(Id, Tasks) ->
    case maps:get(Id, Tasks) of
        {#tg_task{data = {guard, true}}, Deps} ->
            lists:any( fun(Dep) -> is_task_changed(Dep, Tasks) end
                     , maps:keys(Deps)
                     );
        _ ->
            true
    end.

halp() ->
    dbg:stop(),
    io:format(user, "HALP!!!~n", []),
    dbg:start(),
    dbg:tracer(),
    dbg:p(new_processes, [c]),
    dbg:tpl(task_graph_actor, []),
    dbg:tpl(task_graph_server, []).

%%%===================================================================
%%% gen_event boilerplate
%%%===================================================================

init([Pid]) ->
    {ok, Pid}.

handle_event(#tg_event{} = Evt, Parent) ->
    Parent ! Evt,
    {ok, Parent};
handle_event(_, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% CT boilerplate
%%%===================================================================

suite() ->
    [{timetrap,{seconds, ?TIMEOUT}}].

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

all() ->
    [F || {F, _A} <- module_info(exports),
          case atom_to_list(F) of
              "t_" ++ _ -> true;
              _         -> false
          end].

%% Quick view: cat > graph.dot ; dot -Tpng -O graph.dot; xdg-open graph.dot.png
