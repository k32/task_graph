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
        , add_task/2
        , complete_task/2
        , add_dependencies/2
        , search_task/3
        , search_tasks/3
        , print_graph/1
        ]).

-type worker_module() :: atom().

-type task_id() :: term().

-type task() :: #task{}.

-type tasks() :: {[task()], [{task_id(), task_id()}]}.

-opaque task_graph() :: ets:tid().

-record(vertex,
        { task_id :: task_id() | '_'
        , worker_module :: worker_module() | '_'
        , data :: term() | '_'
        , rank = 0 :: non_neg_integer() | '_'
        , dependencies :: map_sets:set(task_id()) | '_'
        }).

-spec new_graph(atom()) -> task_graph().
new_graph(Name) ->
    ets:new(Name, [set, {keypos, #vertex.task_id}]).

-spec delete_graph(task_graph()) -> ok.
delete_graph(G) ->
    ets:delete(G),
    ok.

-spec add_task(task_graph(), task()) ->
                      {ok, task_graph()}
                    | {error, duplicate_task}.
add_task(G, #task{task_id = Ref, worker_module = M, data = D}) ->
    V = #vertex{ task_id = Ref
               , worker_module = M
               , data = D
               , dependencies = map_sets:new()
               },
    case ets:insert_new(G, V) of
        true -> {ok, G};
        false -> {error, duplicate_task}
    end.

%% NOTE: Upon error this function leaves the graph in a corrupt state!
-spec add_dependencies(task_graph(), [{task_id(), task_id()}]) ->
                              {ok, task_graph()}
                            | {error, circular_dependencies}
                            | {error, missing_vertex, task_id()}.
add_dependencies(G, Deps) ->
    Independent0 = ets:match(G, #vertex{ task_id = '$1'
                                       , worker_module = '_'
                                       , data = '_'
                                       , rank = '_'
                                       , dependencies = map_sets:new()
                                       }),
    Independent = map_sets:from_list(lists:flatten(Independent0)),
    try
        lists:foldl(
          fun({From, To}, Acc) ->
              Acc1 = map_sets:del_element(To, Acc),
              case map_sets:size(Acc1) of
                  0 ->
                      throw(circular_dependencies);
                  _ ->
                      [V = #vertex{dependencies = Deps1}] = ets:lookup(G, To),
                      Deps2 = map_sets:add_element(From, Deps1),
                      ets:insert(G, V#vertex{dependencies = Deps2}),
                      ets:update_counter(G, From, [{#vertex.rank, 1}])
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

-spec is_empty(task_graph()) -> boolean().
is_empty(G) ->
    case ets:first(G) of
        '$end_of_table' ->
            true;
        _Val ->
            io:format("OHAYOOOOO ~p~n", [_Val]),
            false
    end.

-spec search_tasks( task_graph()
                  , map_sets:set(worker_module())
                  , map_sets:set(task_id())
                  ) -> {ok, [task()]}.
search_tasks(G, ExcludedModules, ExcludedTasks) ->
    Pattern = #vertex{ task_id = '_'
                     , worker_module = '_'
                     , data = '_'
                     , dependencies = map_sets:new()
                     },
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
                         } <- ets:match_object(G, Pattern),
                  not map_sets:is_element(Ref, ExcludedTasks),
                  not map_sets:is_element(Mod, ExcludedModules)
              ],
    {ok, [T || {_, T} <- lists:keysort(1, Matches)]}.

-spec search_task(task_graph(), '_' | worker_module(), [task_id()]) ->
                         {ok, task()}
                       | false.
search_task(G, Module, Exclude) ->
    Pattern = #vertex{ task_id = '_'
                     , worker_module = Module
                     , data = '_'
                     , dependencies = map_sets:new()
                     },
    search_task( G
               , Pattern
               , map_sets:from_list(Exclude)
               , ets:match_object(G, Pattern, 1)
               ).
search_task(_, _, _, '$end_of_table') ->
    false;
search_task(G, Pattern, ExcludedTasks, {[Vtx], Cont}) ->
    #vertex{ task_id = Ref
           , worker_module = Mod
           , data = Data
           } = Vtx,
    case map_sets:is_element(Ref, ExcludedTasks) of
        true ->
            search_task( G
                       , Pattern
                       , ExcludedTasks
                       , ets:match_object(Cont)
                       );
        false ->
            {ok, #task{ task_id = Ref
                      , worker_module = Mod
                      , data = Data
                      }}
    end.

-spec print_graph(task_graph()) -> ok.
print_graph(G) ->
    List = ets:tab2list(G),
    Edges = [{From, To} || #vertex{task_id = To, dependencies = Deps} <- List
                         , From <- map_sets:to_list(Deps)],
    io:format("digraph G {~n"),
    lists:foreach( fun({A, B}) ->
                       io:format("  ~p -> ~p;~n", [A, B])
                   end
                 , Edges),
    io:format("}~n").


-spec complete_task(task_graph(), task_id()) -> {ok, task_graph()}.
complete_task(G, Ref) ->
    %% Remove the task itself
    ets:delete(G, Ref),
    %% Remove it from the dependencies (Performace is so-so)
    %% TODO: avoid full table sweep
    ets:tab
    .
%% complete_task(G, _, '$end_of_table') ->
%%     {ok, G};
%% complete_task(G, Ref, {[Vertex], Cont}) ->
%%     Deps = Vertex#vertex.dependencies,
%%     Vertex2 = Vertex#vertex{dependencies = map_sets:del_element(Ref, Deps)},
%%     ets:insert(G, Vertex2),
%%     io:format(user, "OHAYOOOOO ets: ~p~n", [ets:tab2list(G)]),
%%     complete_task(G, Ref, maps:match_object(Cont)).

-ifdef(TEST).
search_tasks_test() ->
    G0 = new_graph(foo),
    {ok, G1} = add_task(G0, #task{task_id = 0}),
    {ok, G2} = add_task(G1, #task{task_id = 1}),
    {ok, G3} = add_dependencies(G2, [{1, 0}]),
    ?assertEqual( {ok, [#task{task_id=0}]}
                , search_tasks(G3, #{}, #{})
                ),
    ?assertEqual( {ok, []}
                , search_tasks(G3, map_sets:from_list([undefined]), #{})
                ),
    ?assertEqual( {ok, []}
                , search_tasks(G3, #{}, map_sets:from_list([1]))
                ),
    {ok, G4} = complete_task(G3, 0),
    ?assertEqual( {ok, [#task{task_id=1}]}
                , search_tasks(G4, #{}, #{})
                ).
-endif.
