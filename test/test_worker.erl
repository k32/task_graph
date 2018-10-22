-module(test_worker).
-behavior(task_graph_runner).

-export([run_task/3, guard/2]).

-include("task_graph_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("task_graph/include/task_graph.hrl").

run_task(Ref, error, _GetDepsResult) ->
    {error, oh_no_task_failed, Ref};
run_task(Ref, exception, _GetDepsResult) ->
    error({oh_no_task_crashed, Ref});
run_task(Ref, #{deps := Deps}, GetDepsResult) ->
    check_dep_results(Ref, GetDepsResult, Deps),
    {ok, {result, Ref}};
run_task(Ref, {deferred, {Vertices, Edges}}, GetDepsResult) ->
    case Vertices of
        [#tg_task{task_id = T0}|_] ->
            case GetDepsResult(T0) of
                error ->
                    %% We haven't been executed yet...
                    %% Tag all dependencies to avoid collision with
                    %% existing tasks
                    %% TODO: verify dependencies on the existsing tasks...
                    {defer, {Vertices, Edges}};
                _ ->
                    ok
            end;
        _ ->
            ok
    end;
run_task(Ref, _, _) ->
    ok.

guard(Ref, {guard, true}) ->
    {unchanged, {result, Ref}};
guard(_Ref, _Data) ->
    changed.

check_dep_results(_Self, GetDepsResult, Deps) ->
    lists:foreach( fun(I) ->
                           ?assertEqual( {ok, {result, I}}
                                       , GetDepsResult(I)
                                       )
                   end
                 , Deps).
