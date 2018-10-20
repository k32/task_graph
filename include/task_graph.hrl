-ifndef(_TASK_GRAPH_HRL_).
-define(_TASK_GRAPH_HRL_, true).

-record(tg_task,
        { task_id        :: task_graph_lib:task_id()
        , execute        :: task_graph_lib:task_execute()
        , data           :: term()
        , resources = [] :: [task_graph_lib:resource_id()]
        }).

-record(tg_event,
        { timestamp :: integer()
        , kind      :: atom()
        , data      :: term() | undefined
        }).

-endif.
