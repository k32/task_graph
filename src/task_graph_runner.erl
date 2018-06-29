-module(task_graph_runner).

-callback start(Number :: integer()) ->
    {ok, State :: term()} | {error, string()}.

-callback stop(State :: term()) ->
    ok.

-callback run_task( State :: term()
                  , task_graph_lib:task_id()
                  , Data :: term()
                  , GetDepResult :: fun((task_graph_lib:task_id()) -> {ok, term()} | error)
                  ) ->
          {ok, Result}
        | {ok, Result, task_graph_lib:tasks()}
        | {defer, State, task_graph_lib:tasks()}
        | {error, State, Reason}
        when State    :: term(),
             Result   :: term(),
             Reason   :: term().
