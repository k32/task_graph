%%% This is free and unencumbered software released into the public
%%% domain.
-module(task_graph_digraph).

-include_lib("task_graph/include/task_graph.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([ digraph/0
             ]).


-export([ new_graph/1
        , new_graph/2
        , is_empty_graph/1
        , is_complete/2
        , delete_graph/1
        , add_tasks/2
        , expand/2
        , expand/3
        , complete_task/4
        , add_dependencies/2
        , pre_schedule_tasks/2
        , print_graph/1
        , get_task_result/2
        ]).

-record(vertex,
        { task_id :: task_graph:task_id()
        , execute :: task_graph:task_execute()
        , data :: term()
        , rank = 0 :: non_neg_integer()           %% How many tasks depend on this one
        , dependencies = 0 :: non_neg_integer()   %% How many dependencies this task has
        , changed_deps = 0 :: non_neg_integer()   %% How many dependencies have changed
        , complete = false :: boolean()
        , resources :: [task_graph:resource_id()]
        , rmask     :: non_neg_integer()
        }).

-record(result,
        { task_id :: task_graph:task_id()
        , result  :: term()
        }).

-record(resource,
        { resource_id :: task_graph:resource_id()
        , quantity    :: non_neg_integer()
        , bitpos      :: non_neg_integer()
        }).

-record(task_graph,
        { vertices      :: ets:tid() %% #vertex{}
        , done_vertices :: ets:tid() %% #vertex{}
        , edges         :: ets:tid() %% {From :: task_id(), To :: set(task_id())}
        , results       :: ets:tid() %% #result{}
        , resources     :: ets:tid() %% #resource{}
        }).

%% -type edge() :: {task_id(), map_sets:set(task_id())}.

-opaque digraph() :: #task_graph{}.

-spec ready_to_go(non_neg_integer()) -> ets:match_spec().
ready_to_go(Mask) ->
    ets:fun2ms(fun(V = #vertex{ dependencies = 0
                              , complete = false
                              , rmask = M
                              }
                  ) when M band Mask == 0 ->
                       V
               end).

-spec new_graph(atom()) -> digraph().
new_graph(Name) ->
    new_graph(Name, #{}).

-spec new_graph( atom()
               , #{task_graph:resource_id() => non_neg_integer()}
               ) -> digraph().
new_graph(_Name, Resources) ->
    RR = ets:new(tg_resources,
                 [ set
                 , {keypos, #resource.resource_id}
                 ]),
    maps:fold( fun(K, V, N) when is_integer(V), V > 0 ->
                       ets:insert(RR, #resource{ resource_id = K
                                               , quantity    = V
                                               , bitpos      = N
                                               }),
                       N + 1
               end
             , 0
             , Resources
             ),
    #task_graph{ vertices      = ets:new(tg_vertices,
                                         [ set
                                         , {keypos, #vertex.task_id}
                                         ])
               , done_vertices = ets:new(tg_vertices,
                                         [ set
                                         , {keypos, #vertex.task_id}
                                         ])
               , edges         = ets:new(tg_edges,
                                         [ set
                                         , {keypos, 1}
                                         ])
               , results       = ets:new(tg_results,
                                         [ set
                                         , {keypos, #result.task_id}
                                         ])
               , resources     = RR
               }.

-spec delete_graph(digraph()) -> ok.
delete_graph(#task_graph{ vertices = VV
                        , done_vertices = VD
                        , results = RR
                        , edges = E
                        , resources = R
                        }) ->
    lists:foreach(fun ets:delete/1, [VV, VD, RR, E, R]).

-spec add_tasks(digraph(), [task_graph:task()]) ->
                      ok
                    | {error, duplicate_task}.
add_tasks(G, Tasks) ->
    VV = [task_to_vertex(I, G) || I <- Tasks],
    try
      lists:foreach(
        fun(V) ->
           case ets:insert_new(G#task_graph.vertices, V) of
               false ->
                   %% Compare old and new task definitions
                   [OldV] = lookup_vertex(G, V#vertex.task_id),
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
        VV)
    catch
        duplicate_task ->
            {error, duplicate_task}
    end.

%% Note: leaves graph in inconsistent state upon error!
-spec add_dependencies(digraph(), [{task_graph:task_id(), task_graph:task_id()}]) ->
                              ok
                            | {error, circular_dependencies}
                            | {error, missing_vertex, task_graph:task_id()}.
add_dependencies(G = #task_graph{edges = EE, vertices = VV}, Deps) ->
    try
        %% Add edges
        lists:foreach(
          fun({From, To}) ->
                  Complete =
                      case lookup_vertex(G, From) of
                          [] ->
                              throw({missing_vertex, From});
                          [#vertex{complete = C}] ->
                              C
                      end,
                  case lookup_vertex(G, To) of
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
                          ets:update_counter(VV, To, [ {#vertex.dependencies, 1}
                                                     , {#vertex.changed_deps, 1}
                                                     ]);
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
        ok
    catch
        {circular_dependencies, Cycle} ->
            {error, circular_dependencies, Cycle};
        {missing_vertex, Id} ->
            {error, missing_vertex, Id}
    end.

-spec expand(digraph(), task_graph:digraph()) -> ok | {error, term()}.
expand(G, Tasks) ->
    expand(G, Tasks, undefined).

-spec expand( digraph()
            , task_graph:digraph()
            , task_graph:maybe(task_graph:task_id())
            ) -> ok | {error, term()}.
expand(G, {Vertices, Edges} = Tasks, Parent) ->
    case check_new_tasks(Tasks, Parent) of
        true ->
            case add_tasks(G, Vertices) of
                ok ->
                    add_dependencies(G, Edges);
                Err ->
                    Err
            end;
        false ->
            {error, causality_error}
    end.

%% Dynamically added tasks should not introduce new dependencies for
%% any of existing pending tasks.
-spec check_new_tasks( task_graph:digraph()
                     , task_graph:maybe(task_graph:task_id())
                     ) -> boolean().
check_new_tasks({Vertices, Edges}, ParentTask) ->
    L0 = [Ref || #tg_task{id = Ref} <- Vertices],
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

-spec is_empty_graph(digraph()) -> boolean().
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

-spec pre_schedule_tasks( digraph()
                        , map_sets:set(task_graph:task_id())
                        ) -> {ok, [ { task_graph:task()
                                    , DepsChanged :: boolean()
                                    }
                                  ]}.
pre_schedule_tasks(Graph, ExcludedTasks) ->
    #task_graph{vertices = VV, done_vertices = VD, resources = RR} = Graph,
    RMask = make_resource_bitmask(RR),
    Matches = [{ -Rank
               , {V, ChangedDeps == 0}
               }
               || V = #vertex{ task_id = Ref
                             , rank = Rank
                             , changed_deps = ChangedDeps
                             , resources = Resources
                             } <- ets:select(VV, ready_to_go(RMask)),
                  not map_sets:is_element(Ref, ExcludedTasks),
                  %% Note: the below check actually has side effect
                  maybe_alloc_resources(RR, Resources)
              ],
    {ok, [{vertex_to_task(Vtx), Changed} || {_, {Vtx, Changed}} <- lists:keysort(1, Matches)]}.

-spec alloc_resources( ets:tid()
                     , [task_graph:resource_id()]
                     ) -> ok.
alloc_resources(RT, RR) ->
    lists:foreach( fun(I) ->
                           Res = ets:update_counter(RT, I, {#resource.quantity, -1}),
                           %% Assert:
                           true = Res >= 0
                   end
                 , RR).

-spec print_graph(digraph()) -> iolist().
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

-spec is_complete(digraph(), task_graph:task_id()) -> boolean().
is_complete(#task_graph{done_vertices=VD}, Ref) ->
    case ets:lookup(VD, Ref) of
        [#vertex{complete=Complete}] ->
            Complete;
        [] ->
            false
    end.

-spec complete_task( digraph()
                   , task_graph:task_id()
                   , boolean()
                   , term()
                   ) -> ok.
complete_task(#task_graph{ vertices = VV
                         , done_vertices = VD
                         , edges = EE
                         , results = RR
                         , resources = RSR
                         }, Ref, Changed, Result) ->
    %% Mark task complete
    [Old] = ets:take(VV, Ref),
    ets:insert(VD, Old#vertex{complete = true}),
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
            UpOp = if Changed ->
                           {#vertex.dependencies, -1};
                      true ->
                           [ {#vertex.dependencies, -1}
                           , {#vertex.changed_deps, -1}
                           ]
                   end,
            [ets:update_counter(VV, I, UpOp)
             || I <- map_sets:to_list(Deps)
            ]
    end,
    %% Free resources:
    lists:foreach( fun(I) ->
                           ets:update_counter(RSR, I, {#resource.quantity, 1})
                   end
                 , Old#vertex.resources
                 ),
    ok.

-spec task_to_vertex(task_graph:task(), digraph()) -> #vertex{}.
task_to_vertex( #tg_task{ id = Ref
                        , execute = E
                        , data = D
                        , resources = R
                        }
              , #task_graph{resources = RT}
              ) ->
    RMask = lists:foldl( fun(Key, Acc) ->
                                 case ets:lookup(RT, Key) of
                                     [#resource{bitpos = P}] ->
                                         Acc + 1 bsl P;
                                     [] ->
                                         Acc
                                 end
                         end
                       , 0
                       , lists:usort(R)
                       ),
    Resources = lists:filter( fun(I) -> ets:member(RT, I) end
                            , lists:usort(R)
                            ),
    #vertex{ task_id = Ref
           , execute = E
           , data = D
           , resources = Resources
           , rmask = RMask
           }.

-spec vertex_to_task(#vertex{}) -> task_graph:task().
vertex_to_task(#vertex{task_id = Ref, execute = E, data = D}) ->
    #tg_task{ id = Ref
            , execute = E
            , data = D
            }.

-spec get_task_result(digraph(), task_graph:task_id()) -> {ok, term()} | error.
get_task_result(G = #task_graph{results = RR}, Ref) ->
    case lookup_vertex(G, Ref) of
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
                           , [task_graph:resource_id()]
                           ) -> boolean().
maybe_alloc_resources(RR, Resources) ->
    Ret = lists:all( fun(I) ->
                             case ets:lookup(RR, I) of
                                 [] ->
                                     true;
                                 [#resource{quantity = V}] ->
                                     V > 0
                             end
                     end
                   , Resources),
    if Ret ->
            alloc_resources(RR, Resources);
       true ->
            ok
    end,
    Ret.

-spec lookup_vertex(digraph(), task_graph:task_id()) -> [#vertex{}].
lookup_vertex(#task_graph{vertices = VV, done_vertices = VD}, Id) ->
    case ets:lookup(VV, Id) of
        [] ->
            ets:lookup(VD, Id);
        A ->
            A
    end.

-spec make_resource_bitmask(ets:tid()) -> non_neg_integer().
make_resource_bitmask(ResourcesTab) ->
    ets:foldl( fun(#resource{bitpos = P, quantity = 0}, Acc) ->
                       Acc + 1 bsl P;
                  (#resource{}, Acc) ->
                       Acc
               end
             , 0
             , ResourcesTab
             ).

-ifdef(TEST).

-define(MATCH_TASK(ID),  {#tg_task{id=ID}, _}).

search_tasks_test() ->
    G = new_graph(foo),
    ok = add_tasks(G, [ #tg_task{id = 0}
                      , #tg_task{id = 1}
                      , #tg_task{id = 2}
                      ]),
    ?assertMatch( [ ?MATCH_TASK(0)
                  , ?MATCH_TASK(1)
                  , ?MATCH_TASK(2)
                  ]
                , lists:sort(element( 2
                                    , pre_schedule_tasks(G, #{})
                                    ))
                ),
    ok = add_dependencies(G, [ {0, 1}
                             , {0, 2}
                             , {1, 2}
                             ]),
    ?assertMatch( {ok, [?MATCH_TASK(0)]}
                , pre_schedule_tasks(G, #{})
                ),
    ?assertMatch( {ok, []}
                , pre_schedule_tasks(G, map_sets:from_list([0]))
                ),
    ok = complete_task(G, 0, true, undefined),
    ?assertMatch( {ok, [?MATCH_TASK(1)]}
                , pre_schedule_tasks(G, #{})
                ),
    ok = complete_task(G, 1, true, undefined),
    ?assertMatch( {ok, [?MATCH_TASK(2)]}
                , pre_schedule_tasks(G, #{})
                ).

-endif.
