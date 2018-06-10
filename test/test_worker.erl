-module(test_worker).
-behavior(task_graph_runner).

-export([start/1, stop/1, run_task/3]).
-include("task_graph_test.hrl").

start(N) ->
    io:format("Started test_worker #~p~n", [N]),
    N.

stop(State) ->
    io:format("Stopped test_worker #~p~n", [State]),
    ok.

run_task(State, Ref, error) ->
    {error, oh_no_task_failed, Ref};
run_task(State, Ref, exception) ->
    error({oh_no_task_crashed, Ref});
run_task(State, Ref, #{deps := Deps}) ->
    case check_deps(Deps) of
        true ->
            ok;
        false ->
            error({unresolved_dependencies, Ref, Deps})
    end,
    ets:insert(?TEST_TABLE, #test_table{task_id = Ref}),
    {ok, {}}.

check_deps(Deps) ->
    lists:foldl( fun(Dep, Acc) ->
                         Acc andalso ets:member(?TEST_TABLE, Dep)
                 end
               , true
               , Deps).
