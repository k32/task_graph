-module(task_graph_runner).

-type run_result() :: ok
                    | {ok, Result :: term()}
                    | {ok, Result :: term(), task_graph_lib:tasks()}
                    | {defer, task_graph_lib:tasks()}
                    | {error, Reason :: term()}
                    .

-callback run_task( task_graph_lib:task_id()
                  , Data :: term()
                  , GetDepResult :: fun((task_graph_lib:task_id()) -> {ok, term()} | error)
                  ) -> run_result().

-type guard_result() :: unchanged
                      | {unchanged, Result :: term()}
                      | changed
                      .

-callback guard( task_graph_lib:task_id()
               , Data :: term()
               ) -> guard_result().
