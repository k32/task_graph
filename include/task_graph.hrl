-ifndef(_TASK_GRAPH_HRL_).
-define(_TASK_GRAPH_HRL_, true).

-record(tg_task,
        { id
        , execute
        , data
        , resources = []
        }).

-record(tg_event,
        { timestamp :: integer()
        , kind      :: atom()
        , data      :: term()
        }).

-endif.
