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
             , resource_id/0
             , task_execute/0
             , task_graph/0
             , maybe/1
             ]).

-export([ new_graph/1
        , new_graph/2
        , is_empty_graph/1
        , is_complete/2
        , delete_graph/1
        , add_tasks/2
        , expand/2
        , expand/3
        , complete_task/3
        , add_dependencies/2
        , pre_schedule_tasks/2
        , print_graph/1
        , get_task_result/2
        ]).

-type task_execute() :: atom() | task_runner:run().

-type task_id() :: term().

-type task() :: #tg_task{}.

-type resource_id() :: term().

-type maybe(A) :: {just, A} | undefined.

-type tasks() :: {[task()], [{task_id(), task_id()}]}.

-record(vertex,
        { task_id :: task_id()
        , execute :: task_execute()
        , data :: term()
        , rank = 0 :: non_neg_integer()         %% How many tasks depend on this one
        , dependencies = 0 :: non_neg_integer() %% How many dependencies this task has
        , complete = false :: boolean()
        , resources :: [resource_id()]
        }).

-record(result,
        { task_id :: task_id()
        , result  :: term()
        }).

-record(resource,
        { resource_id :: resource_id()
        , quantity    :: non_neg_integer()
        }).

-define(READY_TO_GO,
        {vertex, _task_id = '$1'
               , _execute = '_'
               , _data = '_'
               , _rank = '_'
               , _dependencies = 0
               , _complete = false
               , _resources = '_'
               }).

-record(task_graph,
        { vertices   :: ets:tid() %% #vertex{}
        , edges      :: ets:tid() %% {From :: task_id(), To :: set(task_id())}
        , results    :: ets:tid() %% #result{}
        , resources  :: ets:tid() %% #resource{}
        }).

%% -type edge() :: {task_id(), map_sets:set(task_id())}.

-opaque task_graph() :: #task_graph{}.


-spec new_graph(atom()) -> task_graph().
new_graph(Name) ->
    new_graph(Name, #{}).

-spec new_graph( atom()
               , #{resource_id() => non_neg_integer()}
               ) -> task_graph().
new_graph(_Name, Resources) ->
    RR = ets:new(tg_resources,
                 [ set
                 , {keypos, #resource.resource_id}
                 ]),
    maps:map( fun(K, V) when is_integer(V), V > 0 ->
                      ets:insert(RR, #resource{ resource_id = K
                                              , quantity    = V
                                              })
              end
            , Resources
            ),
    #task_graph{ vertices  = ets:new(tg_vertices,
                                     [ set
                                     , {keypos, #vertex.task_id}
                                     ])
               , edges     = ets:new(tg_edges,
                                     [ set
                                     , {keypos, 1}
                                     ])
               , results   = ets:new(tg_results,
                                     [ set
                                     , {keypos, #result.task_id}
                                     ])
               , resources = RR
               }.

-spec delete_graph(task_graph()) -> ok.
delete_graph(#task_graph{vertices = V, edges = E, resources = R}) ->
    ets:delete(V),
    ets:delete(E),
    ets:delete(R),
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
expand(G, Tasks) ->
    expand(G, Tasks, undefined).

-spec expand(task_graph(), tasks(), maybe(task_id())) -> {ok, task_graph()} | {error, term()}.
expand(G, {Vertices, Edges} = Tasks, Parent) ->
    case check_new_tasks(Tasks, Parent) of
        true ->
            case add_tasks(G, Vertices) of
                {ok, G1} ->
                    add_dependencies(G1, Edges);
                Err ->
                    Err
            end;
        false ->
            {error, causality_error}
    end.

%% Dynamically added tasks should not introduce new dependencies for
%% any of existing pending tasks.
-spec check_new_tasks( tasks()
                     , {just, task_id()} | undefined
                     ) -> boolean().
check_new_tasks({Vertices, Edges}, ParentTask) ->
    L0 = [Ref || #tg_task{task_id = Ref} <- Vertices],
    New = case ParentTask of
              {just, PT} ->
                  map_sets:from_list([PT|L0]);
              undefined ->
                  map_sets:from_list(L0)
          end,
    %% Tasks with new dependencies:
    Deps = maps:from_list([{To, From} || {From, To} <- Edges]),
    %% Disregard the new tasks:
    map_sets:size(map_sets:subtract(Deps, New)) == 0.

-spec is_empty_graph(task_graph()) -> boolean().
is_empty_graph(G) ->
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

-spec pre_schedule_tasks( task_graph()
                        , map_sets:set(task_id())
                        ) -> {ok, [task()]}.
pre_schedule_tasks(Graph, ExcludedTasks) ->
    #task_graph{vertices = VV, resources = RR} = Graph,
    Matches = [{ -Rank
               , #tg_task{ task_id = Ref
                         , execute = Mod
                         , data = Data
                         }
               }
               || #vertex{ task_id = Ref
                         , execute = Mod
                         , data = Data
                         , rank = Rank
                         , resources = Resources
                         } <- ets:match_object(VV, ?READY_TO_GO),
                  not map_sets:is_element(Ref, ExcludedTasks),
                  %% Note: the below check actually has side effect,
                  %% it allocates resources:
                  maybe_alloc_resources(RR, Resources)
              ],
    {ok, [T || {_, T} <- lists:keysort(1, Matches)]}.

-spec alloc_resources( ets:tid()
                     , [resource_id()]
                     ) -> ok.
alloc_resources(RT, RR) ->
    lists:foreach( fun(I) ->
                           ets:update_counter(RT, I, {#resource.quantity, -1})
                   end
                 , RR).

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

-spec is_complete(task_graph(), task_id()) -> boolean().
is_complete(#task_graph{vertices=VV}, Ref) ->
    case ets:lookup(VV, Ref) of
        [#vertex{complete=Complete}] ->
            Complete;
        [] ->
            false
    end.

-spec complete_task(task_graph(), task_id(), term()) -> ok.
complete_task(#task_graph{ vertices = VV
                         , edges = EE
                         , results = RR
                         , resources = RSR
                         }, Ref, Result) ->
    %% Mark task complete
    [Old] = ets:lookup(VV, Ref),
    ets:insert(VV, Old#vertex{complete = true}),
    case Result of
        undefined ->
            ok;
        _ ->
            ets:insert(RR, #result{ task_id = Ref
                                  , result = Result
                                  })
    end,
    %% Remove it from the dependencies
    case ets:take(EE, Ref) of
        [] ->
            ok;
        [{Ref, Deps}] ->
            [ets:update_counter(VV, I, {#vertex.dependencies, -1})
             || I <- map_sets:to_list(Deps)]
    end,
    %% Free resources:
    lists:foreach( fun(I) ->
                           ets:update_counter(RSR, I, {#resource.quantity, 1})
                   end
                 , Old#vertex.resources),
    ok.

-spec task_to_vertex(task()) -> #vertex{}.
task_to_vertex(#tg_task{task_id = Ref, execute = E, data = D, resources = R}) ->
    #vertex{ task_id = Ref
           , execute = E
           , data = D
           , resources = lists:usort(R)
           }.

-spec vertex_to_task(#vertex{}) -> task().
vertex_to_task(#vertex{task_id = Ref, execute = E, data = D}) ->
    #tg_task{ task_id = Ref
            , execute = E
            , data = D
            }.

-spec get_task_result(task_graph(), task_id()) -> {ok, term()} | error.
get_task_result(#task_graph{vertices = VV, results = RR}, Ref) ->
    case ets:lookup(VV, Ref) of
        [_] ->
            {ok, case ets:match(RR, #result{task_id = Ref, result = '$1'}) of
                     [[Val]] ->
                         Val;
                     [] ->
                         undefined
                 end};
        [] ->
            error
    end.

-spec maybe_alloc_resources( ets:tid()
                           , [resource_id()]
                           ) -> boolean().
maybe_alloc_resources(RR, Resources) ->
    lists:all( fun(I) ->
                       case ets:lookup(RR, I) of
                           [] ->
                               true;
                           [#resource{quantity = V}] ->
                               Ret = V > 0,
                               if Ret ->
                                       alloc_resources(RR, Resources);
                                  true ->
                                       ok
                               end,
                               Ret
                       end
               end
             , Resources).

-ifdef(TEST).
search_tasks_test() ->
    G = new_graph(foo),
    {ok, _} = add_tasks(G, [ #tg_task{task_id = 0}
                           , #tg_task{task_id = 1}
                           , #tg_task{task_id = 2}
                           ]),
    ?assertEqual( [ #tg_task{task_id=0}
                  , #tg_task{task_id=1}
                  , #tg_task{task_id=2}
                  ]
                , lists:sort(element( 2
                                    , pre_schedule_tasks(G, #{})
                                    ))
                ),
    {ok, _} = add_dependencies(G, [ {0, 1}
                                  , {0, 2}
                                  , {1, 2}
                                  ]),
    ?assertEqual( {ok, [#tg_task{task_id=0}]}
                , pre_schedule_tasks(G, #{})
                ),
    ?assertEqual( {ok, []}
                , pre_schedule_tasks(G, map_sets:from_list([0]))
                ),
    ok = complete_task(G, 0, undefined),
    ?assertEqual( {ok, [#tg_task{task_id=1}]}
                , pre_schedule_tasks(G, #{})
                ),
    ok = complete_task(G, 1, undefined),
    ?assertEqual( {ok, [#tg_task{task_id=2}]}
                , pre_schedule_tasks(G, #{})
                ).
-endif.
