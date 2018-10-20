-ifndef(TASK_GRAPH_INT_HRL).
-define(TASK_GRAPH_INT_HRL, 1).

-include_lib("task_graph/include/task_graph.hrl").

-ifndef(OLD_TIME_UNITS).
-define(tg_timeUnit, millisecond).
-else.
-define(tg_timeUnit, milli_seconds).
-endif.

-ifdef(OTP_RELEASE).
-define(BIND_STACKTRACE(V), : V).
-define(GET_STACKTRACE(V), ok).
-else.
-define(BIND_STACKTRACE(V),).
-define(GET_STACKTRACE(V), V = erlang:get_stacktrace()).
-endif.

-endif.
