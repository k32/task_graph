%%% @doc Main interface for the Task Graph library
%%%
-module(task_graph).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        , run_graph/2
        , run_graph_async/4
        , run_graph_async/3
        ]).


-export_type([ task/0
             , task/1
             , edge/0
             , edges/0
             , digraph/0
             , task_id/0
             , resource_id/0
             , task_execute/0
             , maybe/1
             , settings_key/0
             , settings/0
             , result_type/0
             , complete_callback/0
             ]).

-type task_execute() :: atom() | task_runner:run().

-type task_id() :: term().

-type task(A) ::
        #tg_task{ id        :: task_graph:task_id()
                , execute   :: task_graph:task_execute()
                , data      :: A
                , resources :: [task_graph:resource_id()]
                             | task_graph_resource:resources()
                }.

-type task() :: task(term()).

-type resource_id() :: atom() | number() | reference() | list().

-type maybe(A) :: {just, A} | undefined.

-type result_type() :: ok | error | aborted.

-type edge() :: {task_id(), task_id()}.

-type edges() :: [edge()].

-type digraph() :: {[task()], edges()}.

-type settings_key() :: event_manager
                      | event_handlers
                      | resources
                      | disable_guards
                      | keep_going
                      | shutdown_timeout
                      .

-type settings() :: #{settings_key() => term()}.

-type complete_callback() :: fun(({result_type(), term()}) -> ok).

%%--------------------------------------------------------------------
%% @doc Execute task graph with default settings
%% @see run_graph/3
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , task_graph:digraph()
               ) -> {ok, term()} | {error, term()}.
run_graph(Name, Tasks) ->
    run_graph(Name, #{}, Tasks).

%%--------------------------------------------------------------------
%% @doc Execute a task graph. `Name' is an unique identifier of the
%% process executing task graph.
%%
%% `Settings' is a map that may contain the following elements:
%%
%%    `event_manager' is pid of a process receiving `tg_event's. By
%%    default there is no event manager. `tg_events' are useful for
%%    progress tracking and profiling
%%
%%    `event_handlers' is a list of 2-tuples containing gen_event
%%    handler module and its initial state. Task graph will start a
%%    new gen_event process with these handlers. Note that only one
%%    event handler is supported at time. `event_manager' parameter
%%    takes precedence over `event_handlers'.
%%
%%    `resources' is a map containing resource limits. By default all
%%    resources are unlimited.
%%
%%    `disable_guards' is a boolean flag that forces execution of all
%%    tasks. (Similar to ```make -B``` flag)
%%
%%    `keep_going' is a boolean flag that allows all tasks to run even
%%     in presense of errors. False by default
%%
%%    `shutdown_timeout' is a positive integer that indicates how long
%%     task graph can run after reporting the result
%%
%% `Tasks' is a 2-tuple containing vertices and edges of the task
%% graph, respectively. Vertices are represented by a list of
%% ```#tg_task{}``` records. Task ids should be unique. Edges is a
%% list of 2-tuples where first element blocks execution of the second
%% one.
%%
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , settings()
               , task_graph:digraph()
               ) -> {result_type(), term()}.
run_graph(Name, Settings, Tasks) ->
    task_graph_server:run_graph(Name, Settings, Tasks).


%%--------------------------------------------------------------------
%% @doc Asynchronous version of `run_graph' with default settings
%%--------------------------------------------------------------------
-spec run_graph_async( atom()
                     , task_graph:digraph()
                     , complete_callback()
                     ) -> {ok, pid()}
                        | {error, term()}.
run_graph_async(Name, Tasks, Callback) ->
    task_graph_server:run_graph_async(Name, #{}, Tasks, Callback).

%%--------------------------------------------------------------------
%% @doc Asynchronous version of `run_graph'
%%--------------------------------------------------------------------
-spec run_graph_async( atom()
                     , settings()
                     , task_graph:digraph()
                     , complete_callback()
                     ) -> {ok, pid()}
                        | {error, term()}.
run_graph_async(Name, Settings, Tasks, Callback) ->
    task_graph_server:run_graph_async(Name, Settings, Tasks, Callback).
