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
             , task_execute/0
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

-type task_execute() :: atom() | task_runner:run().

-type task_id() :: term().

-type task() :: #task{}.

-type tasks() :: {[task()], [{task_id(), task_id()}]}.

-record(vertex,
        { task_id :: task_id()
        , execute :: task_execute()
        , data :: term()
        , rank = 0 :: non_neg_integer()         %% How many tasks depend on this one
        , dependencies = 0 :: non_neg_integer() %% How many dependencies this task has
        , complete = false :: boolean()
        }).

-define(READY_TO_GO,
        {vertex, _task_id = '$1'
               , _execute = '_'
               , _data = '_'
               , _rank = '_'
               , _dependencies = 0
               , _complete = false
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
    VV = [task_to_vertex(I) || I <- Tasks],
    try
      lists:foreach(
        fun(V) ->
           case ets:insert_new(G#task_graph.vertices, V) of
               false ->
                   %% Compare old and new task definitions
                   [OldV] = ets:lookup(G#task_graph.vertices, V#vertex.task_id),
                   case vertex_to_task(OldV) =:= vertex_to_task(V) of
                       true ->
                           %% New definition is the same, I'll allow it
                           ok;
                       _ ->
                           throw(duplicate_task)
                   end;
               true ->
                   ok
           end
        end,
        VV),
      {ok, G}
    catch
        duplicate_task ->
            {error, duplicate_task}
    end.

%% Note: leaves graph in inconsistent state upon error!
-spec add_dependencies(task_graph(), [{task_id(), task_id()}]) ->
                              {ok, task_graph()}
                            | {error, circular_dependencies}
                            | {error, missing_vertex, task_id()}.
add_dependencies(G = #task_graph{edges = EE, vertices = VV}, Deps) ->
    try
        %% Add edges
        lists:foreach(
          fun({From, To}) ->
                  Complete =
                      case ets:lookup(VV, From) of
                          [] ->
                              throw({missing_vertex, From});
                          [#vertex{complete = C}] ->
                              C
                      end,
                  case ets:lookup(VV, To) of
                      [] ->
                          throw({missing_vertex, To});
                      _ ->
                          ok
                  end,
                  OldDeps = case ets:take(EE, From) of
                                [] -> map_sets:new();
                                [{From, S}] -> S
                            end,
                  case map_sets:is_element(To, OldDeps) orelse Complete of
                      false ->
                          %% Increase rank of the parent task
                          ets:update_counter(VV, From, {#vertex.rank, 1}),
                          %% Increase the number of dependencies
                          ets:update_counter(VV, To, {#vertex.dependencies, 1});
                      true ->
                          %% Avoid double-increasing counters and
                          %% ignore dependencies that are already
                          %% complete
                          ok
                  end,
                  ets:insert(EE, {From, map_sets:add_element(To, OldDeps)}),
                  %% TODO: This is slow and inefficient...
                  check_cycles(EE, map_sets:new(), From)
          end,
          Deps),
        {ok, G}
    catch
        {circular_dependencies, Cycle} ->
            {error, circular_dependencies, Cycle};
        {missing_vertex, Id} ->
            {error, missing_vertex, Id}
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

%% TODO make it more efficient
check_cycles(Edges, Visited, Vertex) ->
    case map_sets:is_element(Vertex, Visited) of
        true ->
            throw({circular_dependencies, [Vertex|map_sets:to_list(Visited)]});
        false ->
            Visited2 = map_sets:add_element(Vertex, Visited),
            Children = case ets:lookup(Edges, Vertex) of
                           [] ->
                               [];
                           [{Vertex, Set}] ->
                               map_sets:to_list(Set)
                       end,
            lists:foreach( fun(Child) ->
                                   check_cycles(Edges, Visited2, Child)
                           end
                         , Children)
    end.

-spec search_tasks( task_graph()
                  , map_sets:set(task_execute())
                  , map_sets:set(task_id())
                  ) -> {ok, [task()]}.
search_tasks(#task_graph{vertices = VV}, ExcludedModules, ExcludedTasks) ->
    Matches = [{ -Rank
               , #task{ task_id = Ref
                      , execute = Mod
                      , data = Data
                      }
               }
               || #vertex{ task_id = Ref
                         , execute = Mod
                         , data = Data
                         , rank = Rank
                         } <- ets:match_object(VV, ?READY_TO_GO),
                  not map_sets:is_element(Ref, ExcludedTasks),
                  not map_sets:is_element(Mod, ExcludedModules)
              ],
    {ok, [T || {_, T} <- lists:keysort(1, Matches)]}.

-spec print_graph(task_graph()) -> iolist().
print_graph(#task_graph{vertices = VV, edges = EE}) ->
    Vertices = [[I, I, R, D] || #vertex{ task_id = I
                                       , rank = R
                                       , dependencies = D
                                       } <- ets:tab2list(VV)
               ],
    Edges = [{From, To} || {From, Deps} <- ets:tab2list(EE)
                         , To <- map_sets:to_list(Deps)],
    [ "digraph G {\n"
    , lists:map( fun(A) ->
                         io_lib:format("  ~p[label=\"~p[rank=~p,deps=~p]\"];\n", A)
                 end
               , Vertices)
    , "\n"
    , lists:map( fun({A, B}) -> io_lib:format("  ~p -> ~p;~n", [A, B]) end
               , Edges)
    , "}\n"
    ].

-spec complete_task(task_graph(), task_id()) -> {ok, task_graph()}.
complete_task(G = #task_graph{vertices = VV, edges = EE}, Ref) ->
    %% Mark task complete
    [Old] = ets:lookup(VV, Ref),
    ets:insert(VV, Old#vertex{complete = true}),
    %% Remove it from the dependencies
    case ets:take(EE, Ref) of
        [] ->
            ok;
        [{Ref, Deps}] ->
            [ets:update_counter(VV, I, {#vertex.dependencies, -1})
             || I <- map_sets:to_list(Deps)]
    end,
    {ok, G}.

-spec task_to_vertex(#task{}) -> #vertex{}.
task_to_vertex(#task{task_id = Ref, execute = E, data = D}) ->
    #vertex{ task_id = Ref
           , execute = E
           , data = D
           }.

-spec vertex_to_task(#vertex{}) -> #task{}.
vertex_to_task(#vertex{task_id = Ref, execute = E, data = D}) ->
    #task{ task_id = Ref
         , execute = E
         , data = D
         }.

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
