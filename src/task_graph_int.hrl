-ifndef(TASK_GRAPH_INT_HRL).
-define(TASK_GRAPH_INT_HRL, 1).

-include_lib("task_graph/include/task_graph.hrl").

-ifndef(OLD_TIME_UNITS).
-define(tg_timeUnit, millisecond).
-else.
-define(tg_timeUnit, milli_seconds).
-endif.

-record(tg_event,
        { timestamp :: integer()
        , kind      :: atom()
        , data      :: term() | undefined
        }).

-endif.
