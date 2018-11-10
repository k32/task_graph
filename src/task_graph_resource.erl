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

-include_lib("stdlib/include/ms_transform.hrl").

-opaque resources() :: non_neg_integer().

-record(task,
        { id :: task_graph:task_id()
        , rmask :: resources()
        }).

-record(resource,
        { bitpos :: non_neg_integer()
        , quantity :: non_neg_integer()
        , resource_id :: task_graph:resource_id()
        }).

-record(s,
        { tasks :: [#task{}]
        , resources :: ets:tid()
        , exhausted_resources :: resources()
        , all_exhausted :: non_neg_integer()
        }).

-opaque state() :: #s{}.

-spec is_empty(resources()) -> boolean().
is_empty(0) ->
  true;
is_empty(A) when is_integer(A) ->
  false.

-spec init_state(#{task_graph:resource_id() => non_neg_integer()}) -> state().
init_state(RR) ->
    Tab = ets:new(resources, [{keypos, #resource.bitpos}, private]),
    PMax = maps:fold( fun(K, V, Acc) when is_integer(V), V > 0 ->
                              ets:insert(Tab, #resource{ bitpos = Acc
                                                       , quantity = V
                                                       , resource_id = K
                                                       }),
                              Acc + 1;
                         (K, V, _) ->
                              error({badresource, K, V})
                      end
                    , 0
                    , RR
                    ),
    #s{ tasks = []
      , resources = Tab
      , exhausted_resources = 0
      , all_exhausted = max(1 bsl PMax - 1, 1)
      }.

-spec to_resources(state(), [task_graph:resource_id()]) -> resources().
to_resources(#s{resources = Tab}, RR) ->
    RMap = map_sets:from_list(RR),
    ets:foldl( fun(#resource{resource_id = RId, bitpos = P}, Acc) ->
                       case map_sets:is_element(RId, RMap) of
                           true ->
                               Acc bor (1 bsl P);
                           false ->
                               Acc
                       end
               end
             , 0
             , Tab
             ).

-spec push_task(state(), task_graph:task_id(), resources()) -> state().
push_task(State = #s{tasks = Tasks0}, TaskId, RR) ->
    Tasks = [#task{ id = TaskId
                  , rmask = RR
                  }|Tasks0],
    State#s{tasks = Tasks}.

-spec free(state(), resources()) -> state().
free(#s{resources = Tab, exhausted_resources = E0} = State, Resources) ->
    update_counters(Tab, Resources, 1),
    E = E0 band (bnot Resources),
    State#s{exhausted_resources = E}.

-spec show_state(state()) -> iolist().
show_state(#s{resources = RR, tasks = TT, exhausted_resources = E}) ->
    io_lib:format("~p", [#{ resources => ets:tab2list(RR)
                          , tasks => TT
                          , exhausted_resources => E
                          }]).

-spec pop_alloc(state()) -> {[task_graph:task_id()], state()}.
pop_alloc(State0 = #s{tasks = T0}) ->
    pop_alloc(State0#s{tasks = []}, T0, []).

pop_alloc(State, [], Acc) ->
    {Acc, State};
pop_alloc( State0 = #s{ all_exhausted = E
                      , exhausted_resources = E
                      , tasks = T0
                      }
         , Tail
         , Acc
         ) ->
    {Acc, State0#s{tasks = Tail ++ T0}};
pop_alloc(State0, [Task | Tail], Acc) ->
    #s{ exhausted_resources = E0
      , tasks = T0
      } = State0,
    #task{ id = TId
         , rmask = Resources
         } = Task,
    case E0 band Resources of
        0 ->
            State = alloc(State0, Resources),
            pop_alloc(State, Tail, [TId|Acc]);
        _ ->
            State = State0#s{tasks = [Task|T0]},
            pop_alloc(State, Tail, Acc)
    end.

alloc(#s{resources = Tab, exhausted_resources = E0} = State, Resources) ->
    E1 = update_counters(Tab, Resources, -1),
    %% Assert:
    0 = E0 band E1,
    State#s{exhausted_resources = E0 bor E1}.

update_counters(Tab, Resources, Delta) ->
    update_counters(Tab, Resources, Delta, 0, 0).

update_counters(_, 0, _, _, Acc) ->
    Acc;
update_counters(Tab, Resources, Delta, BPos, Acc0) ->
    Acc =
        case Resources band 1 of
            1 ->
                case ets:update_counter(Tab, BPos, {#resource.quantity, Delta}) of
                    0 ->
                        Acc0 bor (1 bsl BPos);
                    _ ->
                        Acc0
                end;
            0 ->
                Acc0
        end,
    update_counters(Tab, Resources bsr 1, Delta, BPos + 1, Acc).
