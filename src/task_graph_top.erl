-module(task_graph_top).

-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-include("task_graph_int.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type timestamp() :: non_neg_integer().

-type dt() :: non_neg_integer().

-type top() :: { Min :: timestamp()
               , NItems :: non_neg_integer()
               , Top :: gb_trees:tree(dt(), task_graph:task_id())
               }.

-record(s,
        { sheduling          = 0    :: dt()
        , pending_sheduling  = 0    :: timestamp()
        , extension          = 0    :: dt()
        , pending_extension  = 0    :: timestamp()
        , pending_tasks      = #{}  :: #{task_graph:task_id() => timestamp()}
        , pending_guards     = #{}  :: #{task_graph:task_id() => timestamp()}
        , task_top                  :: top()
        , guard_top                 :: top()
        , callback                  :: fun()
        , n_top_items               :: non_neg_integer()
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

init(#{n := N, callback := Callback}) ->
    {ok, #s{ task_top = new_top()
           , guard_top = new_top()
           , callback = Callback
           , n_top_items = N
           }}.

handle_event(#tg_event{timestamp = TS, kind = Kind, data = Data}, State) ->
    #s{ sheduling          = Shed0
      , extension          = Extend0
      , pending_tasks      = PendingTasks0
      , pending_guards     = PendingGuards0
      , pending_sheduling  = PendingSched
      , pending_extension  = PendingExtension
      , task_top           = TT0
      , guard_top          = GT0
      , callback           = Callback
      , n_top_items        = N
      } = State,
    case Kind of
        spawn_task ->
            {ok, State#s{ pending_tasks = PendingTasks0#{Data => TS}
                        }};
        complete_task ->
            #{Data := Begin} = PendingTasks0,
            {ok, State#s{ pending_tasks = maps:without([Data], PendingTasks0)
                        , task_top = maybe_push_to_top(N, TS - Begin, Data, TT0)
                        }};

        run_guard ->
            {ok, State#s{ pending_guards = PendingGuards0#{Data => TS}
                        }};
        guard_complete ->
            #{Data := Begin} = PendingGuards0,
            {ok, State#s{ pending_guards = maps:without([Data], PendingGuards0)
                        , guard_top = maybe_push_to_top(N, TS - Begin, Data, GT0)
                        }};

        shed_begin ->
            {ok, State#s{pending_sheduling = TS}};
        shed_end ->
            {ok, State#s{sheduling = TS - PendingSched
                        }};

        extend_begin ->
            {ok, State#s{pending_extension = TS}};
        extend_end ->
            {ok, State#s{extension = TS - PendingExtension
                        }};

        complete_graph ->
            Callback(#{ sheduling => Shed0
                      , extension => Extend0
                      , task_top => from_top(TT0)
                      , guard_top => from_top(GT0)
                      }),
            {ok, State};

        _ ->
            {ok, State}
    end.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_top() ->
    {0, 0, gb_trees:empty()}.

maybe_push_to_top(N, Val, Key, {_, M, Top0}) when M < N ->
    Top = push_to_top(Key, Val, Top0),
    {Min, _} = gb_trees:smallest(Top),
    {Min, M + 1, Top};
maybe_push_to_top(_, Val, _, Old = {Min, _, _}) when Min >= Val ->
    Old;
maybe_push_to_top(_, Val, Key, {_, M, Top0}) ->
    Top1 = push_to_top(Key, Val, Top0),
    Top = case gb_trees:take_smallest(Top1) of
              {_, [_], Top2} ->
                  Top2;
              {SK, [_|Tail], Top2} ->
                  gb_trees:insert(SK, Tail, Top2)
          end,
    {Min, _} = gb_trees:smallest(Top),
    {Min, M, Top}.

push_to_top(K, V, Top) ->
    case gb_trees:lookup(V, Top) of
        none ->
            gb_trees:insert(V, [K], Top);
        {value, L} ->
            gb_trees:insert(V, [K|L], gb_trees:delete(V, Top))
    end.

from_top({_, _, Top}) ->
    lists:reverse([{Key, Val} || {Val, Keys} <- gb_trees:to_list(Top), Key <- Keys]).

-ifdef(TEST).

top_test() ->
    Vals = [ {a, 4}
           , {b, 1}
           , {c, 0}
           , {d, 0}
           , {e, 1}
           , {f, 8}
           ],
    Top = lists:foldl( fun({Key, Val}, Acc) ->
                               maybe_push_to_top(4, Val, Key, Acc)
                       end
                     , new_top()
                     , Vals
                     ),
    ?assertMatch([{f, 8}, {a, 4}, {b, 1}, {e, 1}], from_top(Top)).

-endif.
