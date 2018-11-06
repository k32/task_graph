-module(resources_SUITE).

%% Common test callbacks:
-export([ suite/0
        , all/0
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

%% Testcases:
-export([ t_resources/1
        ]).

%%%===================================================================
%%% Macros
%%%===================================================================

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("task_graph/src/task_graph_int.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(CT_TIMEOUT, 120).
-define(NUMTESTS, 1000).
-define(SIZE, 500).

-define(RUN_PROP(PROP, SIZE),
        begin
            %% Workaround against CT's "wonderful" features:
            OldGL = group_leader(),
            group_leader(whereis(user), self()),
            T0 = erlang:system_time(?tg_timeUnit),
            io:format(user, ??PROP, []),
            Result = proper:quickcheck( PROP()
                                      , [ {numtests, ?NUMTESTS}
                                        , {max_size, SIZE}
                                        ]
                                      ),
            group_leader(OldGL, self()),
            T1 = erlang:system_time(?tg_timeUnit),
            io:format(user, "Testcase ran for ~p ms~n", [T1 - T0]),
            true = Result
        end).

-define(RUN_PROP(PROP), ?RUN_PROP(PROP, ?SIZE)).

-define(MAX_RESOURCES, 10).

-define(MAX_CAPACITY, 10).

%%%===================================================================
%%% Testcases and properties
%%%===================================================================
t_resources(_Config) ->
    ?RUN_PROP(resources),
    ok.

resources() ->
    ?FORALL(
       {Limits, Tasks},
       ?LET(Limits, resource_limits(),
            {Limits, tasks(Limits)}),
       begin
           %%halp(),
           S0 = task_graph_resource:init_state(Limits),
           S1 = maps:fold( fun(Tid, RR0, State) ->
                                   RR = task_graph_resource:to_resources(S0, RR0),
                                   task_graph_resource:push_task(State, Tid, RR)
                           end
                         , S0
                         , Tasks
                         ),
           RanTasks = resources_check(S1, Limits, Tasks, []),
           TaskIds = maps:keys(Tasks),
           %% Check that all tasks have been popped exactly once:
           ?assertEqual(lists:sort(TaskIds), lists:sort(RanTasks)),
           true
       end).

resources_check(S0, Limits0, Tasks, Acc) ->
    {Candidates, S1} = task_graph_resource:pop_alloc(S0),
    {[], S1} = task_graph_resource:pop_alloc(S1),
    CounterKeys = lists:append([lists:usort(maps:get(I, Tasks)) || I <- Candidates]),
    Limits1 = dec_counters(CounterKeys, Limits0),
    check_counters(Limits1),
    {S, CounterKeys2}
        = lists:foldl( fun(Tid, {State, Counters}) ->
                               #{Tid := RR} = Tasks,
                               RR2 = task_graph_resource:to_resources(State, RR),
                               { task_graph_resource:free(State, RR2)
                               , lists:usort(RR) ++ Counters
                               }
                       end
                     , {S1, []}
                     , Candidates
                     ),
    Limits = inc_counters(CounterKeys2, Limits1),
    case Candidates of
        [] ->
            Acc;
        _ ->
            resources_check(S, Limits, Tasks, Candidates ++ Acc)
    end.

%%%===================================================================
%%% Proper generators
%%%===================================================================
task_id() ->
    integer().

resource_id() ->
    range(255, 255 + ?MAX_RESOURCES).

resource_limits() ->
    ?LET(L, list({resource_id(), range(1, ?MAX_CAPACITY)}),
         maps:from_list(L)).

task_resource(Limits) ->
    K = maps:keys(Limits),
    case K of
        [] -> resource_id();
        _ ->
            frequency([ {1, resource_id()}
                      , {1, oneof(K)}
                      ])
    end.

task(Limits) ->
    {task_id(), list(task_resource(Limits))}.

tasks(Limits) ->
    ?LET(L, list(task(Limits)),
         begin
             maps:from_list(L)
         end).

%%%===================================================================
%%% Utility functions:
%%%===================================================================
inc_counters(Keys, Map) ->
    lists:foldl( fun(Key, Acc) ->
                         case Acc of
                             #{Key := Val} ->
                                 Acc#{Key => Val + 1};
                             _ ->
                                 Acc
                         end
                 end
               , Map
               , Keys
               ).

dec_counters(Keys, Map) ->
    lists:foldl( fun(Key, Acc) ->
                         case Acc of
                             #{Key := Val} ->
                                 Acc#{Key => Val - 1};
                             _ ->
                                 Acc
                         end
                 end
               , Map
               , Keys
               ).

check_counters(Map) ->
    lists:all(fun(A) -> A >= 0 end, maps:values(Map))
        orelse error({'All counters should be non-negative, but got ', Map}).

halp() ->
    dbg:stop(),
    io:format(user, "HALP!!!~n", []),
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, [c]),
    dbg:tpl(task_graph_resource, []).

%%%===================================================================
%%% CT boilerplate
%%%===================================================================

suite() ->
    [{timetrap,{seconds, ?CT_TIMEOUT}}].

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
