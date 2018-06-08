-ifndef(_TASK_GRAPH_HRL_).
-define(_TASK_GRAPH_HRL_, true).

-record(task, { task_id       :: task_graph_lib:task_id()
              , worker_module :: task_graph_lib:worker_module()
              , data          :: term()
              }).

-endif.
