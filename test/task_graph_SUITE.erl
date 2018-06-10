-module(task_graph_SUITE).
-export([ suite/0
        , all/0
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

%% Testcases:
-export([ t_topology_succ/1
        , t_topology/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("task_graph/include/task_graph.hrl").
-include("task_graph_test.hrl").

all() ->
    [t_topology, t_topology_succ].

-define(TIMEOUT, 60).
-define(NUMTESTS, 1000).
-define(SIZE, 1000).

-define(DAG, list({nat(), nat()})).

-define(RUN_PROP(PROP),
        begin
            %% Workaround against CT's "wonderful" features:
            OldGL = group_leader(),
            group_leader(whereis(user), self()),
            Result = proper:quickcheck( PROP()
                                      , [ {numtests, ?NUMTESTS}
                                        , {max_size, ?SIZE}
                                        ]
                                      ),
            group_leader(OldGL, self()),
            true = Result
        end).

error_handling() ->
    ?FORALL({L, NumErrors}, {?DAG, range(1, 10)},
            begin
                DAG = make_DAG(L),
                %% Inject errors:
                todo
            end).

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
                             , Vertices),
                lists:foreach( fun({From, To}) ->
                                       digraph:add_edge(DG, From, To)
                               end
                             , Edges),
                Acyclic = digraph_utils:is_acyclic(DG),
                digraph:delete(DG),
                Vertices2 = [#task{ task_id = I
                                  , worker_module = test_worker
                                  , data = #{deps => []}
                                  } || I <- Vertices],
                Result = task_graph:run_graph( foo
                                             , undefined
                                             , {Vertices2, Edges}
                                             ),
                case {Acyclic, Result} of
                    {true, {ok, _}} ->
                        true;
                    {false, {error, {topology_error, _}}} ->
                        true
                end
            end).

topology_succ() ->
    ?FORALL(L, ?DAG,
            begin
                DAG = make_DAG(L),
                {ok, _} = task_graph:run_graph( foo
                                              , undefined
                                              , DAG
                                              ),
                %% Check that all tasks have been executed:
                AllRun = lists:foldl( fun(#task{task_id = Task}, Acc) ->
                                              Acc andalso ets:member(?TEST_TABLE, Task)
                                      end
                                    , true
                                    , element(1, DAG))
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

make_DAG(L0) ->
    %% Shrink vertex space a little to get more interesting graphs:
    L = [{N div 2, M div 4} || {N, M} <- L0],
    Edges = [{N, N + M} || {N, M} <- L, M>0],
    Singletons = [N || {N, 0} <- L],
    Dependent = lists:flatten(lists:map(fun tuple_to_list/1, Edges)),
    Vertices = [#task{ task_id = I
                     , worker_module = test_worker
                     , data = #{ deps => [From || {From, To} <- Edges, To =:= I]
                               }
                     }
                || I <- lists:usort(Singletons ++ Dependent)],
    %% io:format(user, "Vertices=~p~nEdges=~p~n", [Vertices, Edges]),
    {Vertices, Edges}.

suite() ->
    [{timetrap,{seconds, ?TIMEOUT}}].

init_per_testcase(_, Config) ->
    ets:new(?TEST_TABLE, [ set
                         , public
                         , named_table
                         , {keypos, #test_table.task_id}
                         ]),
    Config.

end_per_testcase(_, Config) ->
    ets:delete(?TEST_TABLE),
    ok.

%% Quick view: cat > graph.dot ; dot -Tpng -O graph.dot; xdg-open graph.dot.png
