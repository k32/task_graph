%%%-------------------------------------------------------------------
%%% Verify absense of race conditions using Concuerror
%%%-------------------------------------------------------------------
-module(concuerror_tests).

-export([gc_test/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("task_graph/include/task_graph.hrl").
-include("task_graph_test.hrl").

gc_test() ->
    ets:new(?TEST_TABLE, [ set
                         , public
                         , named_table
                         , {keypos, #test_table.task_id}
                         ]),
    Edges = [{1, 2}, {1, 3}, {2, 3}, {2, 4}, {1, dynamic}],
    Tasks = [#task{ task_id = I
                  , execute = test_worker
                  , data = #{deps => [From || {From, To} <- Edges, To =:= I]}
                  }
             || I <- lists:seq(1, 4)],
    %% Task that blocks "defer" task
    Task5 = #task{ task_id = 5
                 , execute = test_worker
                 , data    = #{deps => []}
                 },
    Defer = #task{ task_id = defer
                 , execute = test_worker
                 , data = #{ deps => [dynamic]
                           , defer => {[Task5], [{5, defer}]}
                           }
                 },
    %% Task that spawns "defer" task upon completion
    Dynamic = #task{ task_id = dynamic
                   , execute = test_worker
                   , data = #{ deps => [1]
                             , dynamic => {[Defer], [{dynamic, defer}]}
                             }
                   },
    Tasks1 = [Dynamic | Tasks],
    DAG = {Tasks1, Edges},
    Result = task_graph:run_graph(foo, DAG),
    lists:foreach( fun(I) ->
                           ?assertEqual( {I, true}
                                       , {I, ets:member(?TEST_TABLE, I)}
                                       )
                   end
                 , [defer, dynamic | lists:seq(1, 5)]
                 ),
    ets:delete(?TEST_TABLE),
    ?assertEqual({ok, #{}}, Result).
