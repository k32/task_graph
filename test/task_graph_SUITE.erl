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
        %% , t_resources_nodeps/1 %% FIXME: Unstable
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
-include_lib("task_graph/include/task_graph.hrl").
-include("task_graph_test.hrl").

-define(TIMEOUT, 600).
-define(NUMTESTS, 10).
-define(SIZE, 100).

-define(RUN_PROP(PROP),
        begin
            %% Workaround against CT's "wonderful" features:
            OldGL = group_leader(),
            group_leader(whereis(user), self()),
            io:format(??PROP),
            Result = proper:quickcheck( PROP()
                                      , [ {numtests, ?NUMTESTS}
                                        , {max_size, ?SIZE}
                                        ]
                                      ),
            group_leader(OldGL, self()),
            true = Result
        end).

%%%===================================================================
%%% Testcases and properties
%%%===================================================================

%% Test that dependencies are resolved in a correct order and all
%% tasks are executed for any proper acyclic graph
t_topology_succ(_Config) ->
    ?RUN_PROP(topology_succ),
    ok.

topology_succ() ->
    ?FORALL(DAG0, dag(),
            begin
                {Vertices0, Edges} = DAG0,
                Vertices =
                    [ T0#tg_task{ data = #{deps => [From || {From, To} <- Edges, To =:= I]}
                                }
                      || T0 = #tg_task{task_id = I} <- Vertices0],
                reset_table(),
                DAG = {Vertices, Edges},
                {ok, _} = task_graph:run_graph(foo, DAG),
                %% Check that all tasks have been executed:
                _AllRun = lists:foldl( fun(#tg_task{task_id = Task}, Acc) ->
                                               Acc andalso ets:member(?TEST_TABLE, {task, Task})
                                       end
                                     , true
                                     , element(1, DAG)
                                     )
            end).

%% Test that cyclic dependencies can be detected
t_topology(_Config) ->
    ?RUN_PROP(topology),
    ok.

topology() ->
    ?FORALL(L0, list({nat(), nat()}),
            begin
                reset_table(),
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
                Vertices2 = [#tg_task{ task_id = I
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
                reset_table(),
                ExpectedErrors = expected_errors(DAG),
                {error, Result} = task_graph:run_graph(foo, DAG),
                map_sets:is_subset( Result
                                  , map_sets:from_list(ExpectedErrors)
                                  )
            end)).

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
            {NResources, range(1, MaxCapacity), dag(list(range(1, NResources)))}),
       begin
           ResourceIds = lists:seq(1, NResources),
           Limits = maps:from_list([{I, Capacity} || I <- ResourceIds]),
           {Vertices0, Edges} = DAG0,
           Vertices =
               [T0#tg_task{ resources = RR
                          , data = #{}
                          }
                || T0 = #tg_task{data = RR} <- Vertices0],
           DAG = {Vertices, Edges},
           Opts = #{ resources => Limits
                   , event_handlers => [send_back()]
                   },
           {ok, _} = task_graph:run_graph(foo, Opts, DAG),
           Events = collect_events(),
           lists:foldl(check_resource_usage(false), {#{}, Limits, 0}, Events),
           true
       end).

%% Check that resource constraints are respected (no dependencies)
t_resources_nodeps(_Config) ->
    ?RUN_PROP(resources_nodeps),
    ok.

resources_nodeps() ->
    MaxCapacity = 20,
    MaxNumberOfResorces = 10,
    ?FORALL(
       {NResources, Capacity, TasksData},
       ?LET(NResources, range(1, MaxNumberOfResorces),
            {NResources, range(1, MaxCapacity), list(list(range(1, NResources)))}),
       begin
           ResourceIds = lists:seq(1, NResources),
           Limits = maps:from_list([{I, Capacity} || I <- ResourceIds]),
           Vertices =
               [#tg_task{ task_id = Id
                        , resources = RR
                        , data = #{}
                        , execute = test_worker
                        }
                || {Id, RR} <- lists:zip( lists:seq(1, length(TasksData))
                                        , TasksData
                                        )],
           DAG = {Vertices, []},
           Opts = #{ resources => Limits
                   , event_handlers => [send_back()]
                   },
           {ok, _} = task_graph:run_graph(foo, Opts, DAG),
           Events = collect_events(),
           io:format("Evts: ~p", [Events]),
           lists:foldl(check_resource_usage(Capacity), {#{}, Limits, 0}, Events),
           true
       end).

check_resource_usage(Capacity) ->
    fun(#tg_event{kind = Kind, data = EvtData}, {Tasks, Resources, N}) ->
            case Kind of
                add_tasks ->
                    NewTasks = maps:from_list([{Id, lists:usort(Res)}
                                               || #tg_task{ task_id = Id
                                                          , resources = Res
                                                          } <- EvtData
                                              ]),
                    {maps:merge(Tasks, NewTasks), Resources, N};
                spawn_task ->
                    #{EvtData := RR} = Tasks,
                    Resources2 = dec_counters(RR, Resources),
                    %% Check that resource capacities are always respected
                    %% Assert:
                    lists:all( fun(V) -> V >= 0 end
                             , maps:values(Resources2)
                             ) orelse error(Resources2),
                    %% Check that resources are fully utilized:
                    NumTasks = maps:size(Tasks),
                    if is_integer(Capacity), N > Capacity, N < NumTasks - Capacity - 1 ->
                            %% Assert:
                            true = lists:any( fun(V) -> V == 0 end
                                            , maps:values(Resources2)
                                            ) orelse error({Resources2, N, NumTasks});
                       true ->
                            true
                    end,
                    {Tasks, Resources2, N+1};
                complete_task ->
                    #{EvtData := RR} = Tasks,
                    Resources2 = inc_counters(RR, Resources),
                    {Tasks, Resources2, N};
                _ ->
                    {Tasks, Resources, N}
            end
    end.

%% Check that task spawn events are reported, collected and processed
%% to nice graphs
t_evt_draw_deps(_Config) ->
    Filename = "test.dot",
    DynamicTask = #tg_task{ task_id = dynamic
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
    Tasks = [ #tg_task{task_id="foo", execute=Exec}
            , #tg_task{task_id=1, execute=Exec}
            ],
    Deps = [{"foo", 1}],
    {ok, _} = task_graph:run_graph(foo, Opts, {Tasks, Deps}),
    Bin = <<"digraph task_graph {\n"
            "preamble\n"
            "  \"foo\"[color=green shape=oval];\n"
            "  1[color=green shape=oval];\n"
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
    Tasks = [#tg_task{ task_id = I
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

%%%===================================================================
%%% Proper generators
%%%===================================================================

%% Proper generator of directed acyclic graphs:
dag() ->
    dag(#{deps => []}).
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
              Tasks = [#tg_task{ task_id = I
                               , data = P
                               , execute = test_worker
                               }
                       || {I, P} <- lists:zip(Vertices, Data)],
              {Tasks, Edges}
          end).

inject_errors() ->
    frequency([ {1, error}
              , {1, exception}
              , {5, #{deps => []}}
              ]).

%%%===================================================================
%%% Utility functions:
%%%===================================================================

expected_errors(DAG) ->
    {Vertices, _} = DAG,
    [Id || #tg_task{task_id = Id, data = D} <- Vertices,
           D =:= error orelse D =:= exception].

reset_table() ->
    ets:delete_all_objects(?TEST_TABLE).

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
                         maps:update_with(Key, fun(V) -> V + 1 end, Acc)
                 end
               , Map
               , Keys
               ).

dec_counters(Keys, Map) ->
    lists:foldl( fun(Key, Acc) ->
                         maps:update_with(Key, fun(V) -> V - 1 end, Acc)
                 end
               , Map
               , Keys
               ).

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
    ets:new(?TEST_TABLE, [ set
                         , public
                         , named_table
                         , {keypos, #test_table.id}
                         ]),
    Config.

end_per_testcase(_, Config) ->
    ets:delete(?TEST_TABLE),
    Config.

all() ->
    [F || {F, _A} <- module_info(exports),
          case atom_to_list(F) of
              "t_" ++ _ -> true;
              _         -> false
          end].

%% Quick view: cat > graph.dot ; dot -Tpng -O graph.dot; xdg-open graph.dot.png
