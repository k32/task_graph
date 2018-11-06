-module(task_graph_resource).

-export_type([ resources/0
             , state/0
             ]).

-export([ is_empty/1
        , init_state/1
        , to_resources/2
        , push_task/3
        , pop_alloc/1
        , free/2
        , show_state/1
        ]).

-record(s,
        { table :: ets:tid()
        , resources :: [reference()]
        , name_map :: #{task_graph:resource_id() => reference()}
        , all_tasks :: gb_trees:tree(task_graph:task_id(), resources())
        , exhausted_resources :: [reference()]
        }).

-record(r,
        { id :: reference()
        , quantity :: non_neg_integer()
        , excl_tasks :: task_set() %% Tasks that DO NOT depend on this resource
        }).

-opaque resources() :: map_sets:set(reference()).

-opaque state() :: #s{}.

-type task_set() :: gb_sets:set(task_graph:task_id()).

-spec is_empty(resources()) -> boolean().
is_empty(#{}) ->
  true;
is_empty(A) when is_list(A) ->
  false.

-spec init_state(#{task_graph:resource_id() => non_neg_integer()}) -> state().
init_state(RR) ->
  Tab = ets:new(resources_tab, [{keypos, #r.id}]),
  NameMap = maps:fold( fun(K, V, Acc) when is_integer(V), V > 0 ->
                           Ref = make_ref(),
                           ets:insert(Tab, #r{ id = Ref
                                             , quantity = V
                                             , excl_tasks = gb_sets:new()
                                             }),
                           Acc#{K => Ref};
                          (K, V, _) ->
                           error({badresource, K, V})
                       end
                     , #{}
                     , RR
                     ),
  %% TODO: Optimize order to filter out tasks faster
  Resources = maps:values(NameMap),
  #s{ table = Tab
    , name_map = NameMap
    , resources = Resources
    , all_tasks = gb_trees:empty()
    , exhausted_resources = []
    }.

-spec to_resources(state(), [task_graph:resource_id()]) -> resources().
to_resources(#s{name_map = M}, RR) ->
  map_sets:from_list(
    lists:filtermap( fun(K) ->
                             case M of
                                 #{K := V} -> {true, V};
                                 _ -> false
                             end
                     end
                   , lists:usort(RR)
                   )).

-spec push_task(state(), task_graph:task_id(), resources()) -> state().
push_task(State = #s{table = Tab, resources = RL, all_tasks = AllTasks0}, TaskId, RR) ->
  lists:foreach( fun(I) ->
                     case map_sets:is_element(I, RR) of
                       false ->
                         [Obj] = ets:lookup(Tab, I),
                         Set = gb_sets:add_element(TaskId, Obj#r.excl_tasks),
                         ets:insert(Tab, Obj#r{ excl_tasks = Set
                                              });
                       true ->
                         ok
                     end
                 end
               , RL
               ),
  AllTasks = gb_trees:insert(TaskId, RR, AllTasks0),
  State#s{all_tasks = AllTasks}.

-spec free(state(), resources()) -> state().
free(#s{table = Tab, exhausted_resources = E0} = State, Resources) ->
  E = map_sets:fold( fun(Key, Acc) ->
                       ets:update_counter(Tab, Key, {#r.quantity, 1}),
                       lists:delete(Key, Acc)
                     end
                   , E0
                   , Resources
                   ),
  State#s{exhausted_resources = E}.

show_state(#s{table = Tab}) ->
  io_lib:format("~p", [ets:tab2list(Tab)]).

-spec pop_alloc(state()) -> {[task_graph:task_id()], state()}.
pop_alloc(State) ->
  pop_alloc(State, []).

pop_alloc(State0, Acc) ->
  Exhausted = exhausted_resources(State0),
  NumExhausted = length(Exhausted),
  NumResources = maps:size(State0#s.name_map),
  AllTasks0 = State0#s.all_tasks,
  case {NumExhausted, gb_trees:is_empty(AllTasks0)} of
    {0, false} ->
      %% All resources are available; alloc immediately
      {TaskId, Resources, AllTasks} = gb_trees:take_smallest(AllTasks0),
      State = alloc(State0#s{all_tasks = AllTasks}, Resources),
      delete_task(TaskId, State),
      pop_alloc(State, [TaskId|Acc]);
    {NumResources, _} ->
      %% No need to run search: all resources have been used up
      {Acc, State0};
    {_, false} when NumExhausted < NumResources ->
      %% Some resources are exhausted, search for tasks that don't use
      %% them:
      pop_alloc_2(Exhausted, State0, Acc);
    {_, true} ->
      %% No tasks left
      {Acc, State0}
  end.

pop_alloc_2([R0|Tail] = Exhausted, State, Acc) ->
  S0 = get_excl_tasks(R0, State),
  Next = gb_sets:next(gb_sets:iterator(S0)),
  pop_alloc_2(Exhausted, Tail, Next, State, Acc).

pop_alloc_2(_, _, none, State, Acc) ->
  {Acc, State};
pop_alloc_2(_Exhausted0, [], {Tid, Iter}, State0, Acc0) ->
  {Resources, State1} = take_task(Tid, State0),
  State = alloc(State1, Resources),
  Exhausted = exhausted_resources(State),
  Acc = [Tid|Acc0],
  pop_alloc_2( Exhausted
             , Exhausted
             , gb_sets:next(Iter)
             , State
             , Acc
             );
pop_alloc_2(Exhausted, [R1|Tail], {Tid, _Iter0}, State, Acc) ->
  S1 = get_excl_tasks(R1, State),
  Iter1 = gb_sets:iterator_from(Tid, S1),
  case gb_sets:next(Iter1) of
    {Tid, Iter} ->
      pop_alloc_2( Exhausted
                 , Tail
                 , {Tid, Iter}
                 , State
                 , Acc
                 );
    {OtherTid, Iter} ->
      %% TODO: we check R1 twice, this perhaps could be avoided
      pop_alloc_2( Exhausted
                 , Exhausted
                 , {OtherTid, Iter}
                 , State
                 , Acc
                 );
    none ->
      {Acc, State}
  end.

get_excl_tasks(R, #s{table = Tab}) ->
  ets:lookup_element(Tab, R, #r.excl_tasks).

alloc(#s{table = Tab, exhausted_resources = E0} = State, Resources) ->
  E1 = lists:foldl( fun(Key, Acc) ->
                        Val = ets:update_counter(Tab, Key, {#r.quantity, -1}),
                        case Val of
                          0 ->
                            [Key|Acc];
                          _ ->
                            Acc
                        end
                    end
                  , []
                  , map_sets:to_list(Resources)
                  ),
  State#s{exhausted_resources = E0 ++ E1}.

take_task(Tid, State0 = #s{all_tasks = A0}) ->
  Resources = gb_trees:get(Tid, A0),
  delete_task(Tid, State0),
  State = State0#s{all_tasks = gb_trees:delete(Tid, A0)},
  {Resources, State}.

delete_task(Tid, State = #s{resources = RR, table = Tab}) ->
  lists:foreach( fun(RId) ->
                     [R] = ets:take(Tab, RId),
                     E = gb_sets:del_element(Tid, R#r.excl_tasks),
                     ets:insert(Tab, R#r{ excl_tasks = E
                                        })
                 end
               , RR
               ).

exhausted_resources(#s{exhausted_resources = E}) ->
  E.
