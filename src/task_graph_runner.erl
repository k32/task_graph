-module(task_graph_runner).

-callback start(Number :: integer()) ->
    {ok, State :: term()} | {error, string()}.

-callback stop(State :: term()) ->
    ok.

-type ok_result() :: ok | unchanged.

-callback run_task( State
                  , task_graph_lib:task_id()
                  , Data :: term()
                  , GetDepResult :: fun((task_graph_lib:task_id()) -> {ok, term()} | error)
                  ) ->
          ok_result()
        | {ok_result(), Result}
        | {ok_result(), Result, task_graph_lib:tasks()}
        | {defer, task_graph_lib:tasks()}
        | {error, Reason}
        when State    :: term(),
             Result   :: term(),
             Reason   :: term().
