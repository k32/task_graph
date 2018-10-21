-module(test_worker).
-behavior(task_graph_runner).

-export([run_task/3]).

-include("task_graph_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("task_graph/include/task_graph.hrl").

run_task(Ref, error, _GetDepsResult) ->
    {error, oh_no_task_failed, Ref};
run_task(Ref, exception, _GetDepsResult) ->
    error({oh_no_task_crashed, Ref});
run_task(Ref, #{deps := Deps}, GetDepsResult) ->
    {ok, {result, Ref}};
run_task(Ref, {deferred, {Vertices0, Edges0}}, GetDepsResult) ->
    case Vertices0 of
        [#tg_task{task_id = T0}|_] ->
            case GetDepsResult({Ref, T0}) of
                error ->
                    %% We haven't been executed yet...
                    %% Tag all dependencies to avoid collision with
                    %% existing tasks
                    Vertices = [T#tg_task{task_id = {Ref, T#tg_task.task_id}}
                                || T <- Vertices0
                               ],
                    Edges = [{{Ref, A}, {Ref, B}}
                             || {A, B} <- Edges0
                            ],
                    {defer, {Vertices, Edges}};
                _ ->
                    ok
            end;
        _ ->
            ok
    end;
run_task(Ref, _, _) ->
    ok.

check_dep_results(_Self, GetDepResult, Deps) ->
    lists:foreach( fun(I) ->
                           ?assertEqual( {ok, {result, I}}
                                       , GetDepResult(I)
                                       )
                   end
                 , Deps).
