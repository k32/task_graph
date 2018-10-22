-module(task_graph_runner).

-export_type([ get_deps_result/0
             , run_result/0
             , guard_result/0
             ]).

-type get_deps_result() ::
        fun((task_graph_lib:task_id()) -> {ok, term()} | error).

-type run_result() :: ok
                    | {ok, Result :: term()}
                    | {ok, Result :: term(), task_graph_lib:tasks()}
                    | {defer, task_graph_lib:tasks()}
                    | {error, Reason :: term()}
                    .

-callback run_task( task_graph_lib:task_id()
                  , Data :: term()
                  , GetDepResult :: get_deps_result()
                  ) -> run_result().

-type guard_result() :: unchanged
                      | {unchanged, Result :: term()}
                      | changed
                      .

-callback guard( task_graph_lib:task_id()
               , Data :: term()
               , GetDepResult :: get_deps_result()
               ) -> guard_result().
