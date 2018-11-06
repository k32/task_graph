-ifndef(_TASK_GRAPH_HRL_).
-define(_TASK_GRAPH_HRL_, true).

-record(tg_task,
        { id             :: task_graph:task_id()
        , execute        :: task_graph:task_execute()
        , data           :: term()
        , resources = [] :: [task_graph:resource_id()]
                          | task_graph_resource:resources()
        }).

-record(tg_event,
        { timestamp :: integer()
        , kind      :: atom()
        , data      :: term()
        }).

-endif.
