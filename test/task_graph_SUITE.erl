-module(task_graph_SUITE).
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
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("task_graph/include/task_graph.hrl").
-include("task_graph_test.hrl").

all() ->
    [t_evt_draw_deps, t_evt_build_flow, t_error_handling, t_topology, t_topology_succ].

-define(TIMEOUT, 60).
-define(NUMTESTS, 1000).
-define(SIZE, 1000).

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
                Vertices2 = [#task{ task_id = I
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

topology_succ() ->
    ?FORALL(DAG, dag(),
            begin
                reset_table(),
                Filename = integer_to_list(erlang:monotonic_time()) ++ ".log",
                {ok, _} = task_graph:run_graph(foo, DAG),
                %% Check that all tasks have been executed:
                AllRun = lists:foldl( fun(#task{task_id = Task}, Acc) ->
                                              Acc andalso ets:member(?TEST_TABLE, Task)
                                      end
                                    , true
                                    , element(1, DAG)
                                    )
            end).

%% Test that dependencies are resolved in a correct order and all
%% tasks are executed for any proper acyclic graph
t_topology_succ(_Config) ->
    ?RUN_PROP(topology_succ),
    ok.

%% Test that cyclic dependencies can be detected
t_topology(_Config) ->
    ?RUN_PROP(topology),
    ok.

%% Check that errors in the tasks are reported
t_error_handling(_Config) ->
    ?RUN_PROP(error_handling),
    ok.

%% Check that task spawn events are reported, collected and processed
%% to nice graphs
t_evt_draw_deps(_Config) ->
    Filename = "test.dot",
    DynamicTask = #task{ task_id = dynamic
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
    Tasks = [ #task{task_id="foo", execute=Exec}
            , #task{task_id=1, execute=Exec}
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
    Tasks = [#task{ task_id = I
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

%% Proper generator for directed acyclic graphs:
dag() ->
    dag(static_deps_check()).
%% ...supply custom data to tasks:
dag(Fun) ->
    dag(Fun, 2, 4).
%% ...adjust magic parameters X and Y governing graph topology:
dag(Fun, X, Y) ->
    ?LET(Mishmash, list({{nat(), nat()}, {Fun, Fun}}),
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
              Tasks = [#task{ task_id = I
                            , execute = test_worker
                            , data = Fun(I, [From || {From, To} <- Edges, To =:= I])
                            }
                       || {I, Fun} <- lists:zip(Vertices, Data)],
              {Tasks, Edges}
          end).

static_deps_check() ->
    fun(_Self, Deps) ->
            #{deps => Deps}
    end.

skip_deps_check() ->
    fun(_Self, _Deps) ->
            #{deps => []}
    end.

inject_errors() ->
    frequency([ {1, fun(_, _) -> error end}
              , {1, fun(_, _) -> exception end}
              , {5, skip_deps_check()}
              ]).

expected_errors(DAG) ->
    {Vertices, _} = DAG,
    [Id || #task{task_id = Id, data = D} <- Vertices,
           D =:= error orelse D =:= exception].

suite() ->
    [{timetrap,{seconds, ?TIMEOUT}}].

reset_table() ->
    ets:delete_all_objects(?TEST_TABLE).

init_per_testcase(_, Config) ->
    ets:new(?TEST_TABLE, [ set
                         , public
                         , named_table
                         , {keypos, #test_table.task_id}
                         ]),
    Config.

end_per_testcase(_, Config) ->
    ets:delete(?TEST_TABLE),
    Config.

%% Quick view: cat > graph.dot ; dot -Tpng -O graph.dot; xdg-open graph.dot.png
