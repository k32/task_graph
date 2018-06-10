-module(test_worker).
-behavior(task_graph_runner).

-export([start/1, stop/1, run_task/2]).

start(N) ->
    io:format("Started test_worker #~p~n", [N]),
    N.

stop(State) ->
    io:format("Stopped test_worker #~p~n", [State]),
    ok.

run_task(State, Task) ->
    io:format("Running task ~p~n", [Task]),
    {ok, {}}.
