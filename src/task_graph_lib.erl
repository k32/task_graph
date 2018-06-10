%%% This is free and unencumbered software released into the public
%%% domain.
-module(task_graph_lib).

-include_lib("task_graph/include/task_graph.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([ task/0
             , tasks/0
             , task_id/0
             , worker_module/0
             , task_graph/0
             ]).

-export([ new_graph/1
        , is_empty/1
        , delete_graph/1
        , add_tasks/2
        , expand/2
        , complete_task/2
        , add_dependencies/2
        , search_tasks/3
        , print_graph/1
        ]).

-type worker_module() :: atom().

-type task_id() :: term().

-type task() :: #task{}.

-type tasks() :: {[task()], [{task_id(), task_id()}]}.

-record(vertex,
        { task_id :: task_id()
        , worker_module :: worker_module()
        , data :: term()
        , rank = 0 :: non_neg_integer()         %% How many tasks depend on this one
        , dependencies = 0 :: non_neg_integer() %% How many dependencies this task has
        }).

-define(INDEPENDENT,
        {vertex, _task_id = '$1'
               , _worker_module = '_'
               , _data = '_'
               , _rank = '_'
               , _dependencies = 0
               }).

-record(task_graph,
        { vertices :: ets:tid()
        , edges    :: ets:tid()
        }).

%% -type edge() :: {task_id(), map_sets:set(task_id())}.

-opaque task_graph() :: #task_graph{}.

-spec new_graph(atom()) -> task_graph().
new_graph(_Name) ->
    #task_graph{ vertices = ets:new(tg_vertices,
                                    [ set
                                    , {keypos, #vertex.task_id}
                                    ])
               , edges    = ets:new(tg_edges,
                                    [ set
                                    , {keypos, 1}
                                    ])
               }.

-spec delete_graph(task_graph()) -> ok.
delete_graph(#task_graph{vertices = V, edges = E}) ->
    ets:delete(V),
    ets:delete(E),
    ok.

-spec add_tasks(task_graph(), [task()]) ->
                      {ok, task_graph()}
                    | {error, duplicate_task}.
add_tasks(G, Tasks) ->
    VV = [#vertex{ task_id = Ref
                 , worker_module = M
                 , data = D
                 }
          || #task{ task_id = Ref
                  , worker_module = M
                  , data = D
                  } <- Tasks],
    case ets:insert_new(G#task_graph.vertices, VV) of
        false ->
            {error, duplicate_task};
        true ->
            {ok, G}
    end.

%% Note: leaves graph in inconsistent state upon error!
-spec add_dependencies(task_graph(), [{task_id(), task_id()}]) ->
                              {ok, task_graph()}
                            | {error, circular_dependencies}
                            | {error, missing_vertex, task_id()}.
add_dependencies(G = #task_graph{edges = EE, vertices = VV}, Deps) ->
    Independent0 = ets:match(VV, ?INDEPENDENT),
    Independent = map_sets:from_list(lists:flatten(Independent0)),
    try
        lists:foldl(
          fun({From, To}, Acc) ->
              Acc1 = map_sets:del_element(To, Acc),
              case map_sets:size(Acc1) of
                  0 ->
                      throw(circular_dependencies);
                  _ ->
                      OldDeps = case ets:take(EE, From) of
                                    [] -> map_sets:new();
                                    [{From, S}] -> S
                                end,
                      ets:insert(EE, {From, map_sets:add_element(To, OldDeps)}),
                      %% Increase rank of the parent task
                      ets:update_counter(VV, From, {#vertex.rank, 1}),
                      %% Increase the number of dependencies
                      ets:update_counter(VV, To, {#vertex.dependencies, 1})
              end,
              Acc1
          end,
          Independent,
          Deps),
        {ok, G}
    catch
        circular_dependencies ->
            {error, circular_dependencies};
        missing_vertex ->
            {error, missing_vertex}
    end.

-spec expand(task_graph(), tasks()) -> {ok, task_graph()} | {error, term()}.
expand(G, {Vertices, Edges}) ->
    case add_tasks(G, Vertices) of
        {ok, G1} ->
            add_dependencies(G1, Edges);
        Err ->
            Err
    end.

-spec is_empty(task_graph()) -> boolean().
is_empty(G) ->
    case ets:first(G#task_graph.vertices) of
        '$end_of_table' ->
            true;
        _Val ->
            false
    end.

-spec search_tasks( task_graph()
                  , map_sets:set(worker_module())
                  , map_sets:set(task_id())
                  ) -> {ok, [task()]}.
search_tasks(#task_graph{vertices = VV}, ExcludedModules, ExcludedTasks) ->
    Matches = [{ -Rank
               , #task{ task_id = Ref
                      , worker_module = Mod
                      , data = Data
                      }
               }
               || #vertex{ task_id = Ref
                         , worker_module = Mod
                         , data = Data
                         , rank = Rank
                         } <- ets:match_object(VV, ?INDEPENDENT),
                  not map_sets:is_element(Ref, ExcludedTasks),
                  not map_sets:is_element(Mod, ExcludedModules)
              ],
    {ok, [T || {_, T} <- lists:keysort(1, Matches)]}.

-spec print_graph(task_graph()) -> iolist().
print_graph(#task_graph{vertices = VV, edges = EE}) ->
    Vertices = [I#vertex.task_id || I <- ets:tab2list(VV)],
    Edges = [{From, To} || {From, Deps} <- ets:tab2list(EE)
                         , To <- map_sets:to_list(Deps)],
    [ "digraph G {~n  "
    , lists:map( fun(A) -> io_lib:format("~p; ", [A]) end, Vertices),
    , "\n"
    , lists:map( fun({A, B}) -> io_lib:format("  ~p -> ~p;~n", [A, B]) end
               , Edges)
    , "}\n"
    ].


-spec complete_task(task_graph(), task_id()) -> {ok, task_graph()}.
complete_task(G = #task_graph{vertices = V, edges = E}, Ref) ->
    %% Remove the task itself
    ets:delete(V, Ref),
    %% Remove it from the dependencies
    case ets:take(E, Ref) of
        [] ->
            ok;
        [{Ref, Deps}] ->
            [ets:update_counter(V, I, {#vertex.dependencies, -1})
             || I <- map_sets:to_list(Deps)]
    end,
    {ok, G}.

-ifdef(TEST).
search_tasks_test() ->
    G0 = new_graph(foo),
    {ok, G1} = add_tasks(G0, [ #task{task_id = 0}
                             , #task{task_id = 1}
                             , #task{task_id = 2}
                             ]),
    ?assertEqual( [ #task{task_id=0}
                  , #task{task_id=1}
                  , #task{task_id=2}
                  ]
                , lists:sort(element( 2
                                    , search_tasks(G1, #{}, #{})
                                    ))
                ),
    {ok, G2} = add_dependencies(G1, [ {0, 1}
                                    , {0, 2}
                                    , {1, 2}
                                    ]),
    ?assertEqual( {ok, [#task{task_id=0}]}
                , search_tasks(G2, #{}, #{})
                ),
    ?assertEqual( {ok, []}
                , search_tasks(G2, map_sets:from_list([undefined]), #{})
                ),
    ?assertEqual( {ok, []}
                , search_tasks(G2, #{}, map_sets:from_list([0]))
                ),
    {ok, G3} = complete_task(G2, 0),
    ?assertEqual( {ok, [#task{task_id=1}]}
                , search_tasks(G3, #{}, #{})
                ),
    {ok, G4} = complete_task(G3, 1),
    ?assertEqual( {ok, [#task{task_id=2}]}
                , search_tasks(G4, #{}, #{})
                ).
-endif.
