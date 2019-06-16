-module(task_graph_actor).

%% Rules: no synchronous calls to the parent!

-behaviour(gen_statem).

-include("task_graph_int.hrl").

%% API
-export([ start_link/4
        , add_consumer/3
        , add_requirement/3
        , launch/1
        , abort/2
        , resources_acquired/1
        , rip/1
        , get_states/1
        ]).

%% gen_statem callbacks
-export([callback_mode/0, handle_event/4, init/1, terminate/3, code_change/4]).

%%%===================================================================
%%% Types
%%%===================================================================

-export_type([state/0]).

%% States:
%%
%% 0. `startup' The task is waiting until `task_graph_server' provides
%% it with the pids of upstream and downstram dependencies. This is
%% the intial state.
%%
%% 1. `wait_deps' The task is waiting for the upstream dependencies to
%% complete
%%
%% 2. `wait_resources' The task asked `task_graph_server' to grant
%% resources and is waiting for the confirmation
%%
%% 3. `complete' The task is waiting for the `task_graph_server' to
%% record its return value
%%
-type state() :: startup
               | wait_deps
               | wait_resources
               | complete
               .

-record(d,
        { id                         :: task_graph:task_id()
        , data                       :: term()
        , requires = #{}             :: #{task_graph:task_id() => pid() | future}
        , provides = #{}             :: #{task_graph:task_id() => pid()}
        , exec_fun                   :: fun()
        , n_deps = 0                 :: non_neg_integer()
        , n_changed_deps = 0         :: non_neg_integer()
        , rank = 0                   :: non_neg_integer() %% Fixme: calculate rank
        , guard_fun                  :: fun()
        , resources                  :: task_graph_resource:resources()
        , parent                     :: pid()
        , event_mgr                  :: pid()
        , get_result_fun             :: task_graph_runner:get_deps_result()
        , no_guards                  :: boolean()
        , epitaph                    :: term()
        }).

-import(task_graph_server, [event/3, event/2]).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link( pid()
                , task_graph:task()
                , task_graph_runner:get_deps_result()
                , #{}
                ) -> {ok, Pid :: pid()} |
                     ignore |
                     {error, Error :: term()}.
start_link(EventMgr, Task, GetResultFun, Settings) ->
    Attrs = [self(), Task, EventMgr, GetResultFun, Settings],
    gen_statem:start_link(?MODULE, Attrs, []).

add_consumer(Pid, To, PTo) ->
    gen_statem:call(Pid, {add_consumer, To, PTo}).

add_requirement(Pid, From, PFrom) ->
    gen_statem:call(Pid, {add_requirement, From, PFrom}).

-spec launch(pid()) -> ok.
launch(Pid) ->
    gen_statem:cast(Pid, launch).

-spec abort(pid(), term()) -> ok.
abort(Pid, Reason) ->
    gen_statem:cast(Pid, {abort, Reason}).

-spec rip(pid()) -> ok.
rip(Pid) ->
    gen_statem:cast(Pid, rip).

-spec resources_acquired(pid()) -> ok.
resources_acquired(Pid) ->
    gen_statem:cast(Pid, resources_acquired).

-spec get_states([pid()]) -> [state()].
get_states(Pids) ->
    %% TODO
    [].

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init([ Parent
     , #tg_task{ id = Id
               , data = Data
               , execute = Exec
               , resources = Resources
               }
     , EventMgr
     , GetResultFun
     , Settings
     ]) ->
    case Exec of
        _ when is_atom(Exec) ->
            RunTaskFun = fun Exec:run_task/3,
            GuardFun   = fun Exec:guard/3;
        _ when is_function(Exec) ->
            RunTaskFun = Exec,
            GuardFun   = fun(_, _, _) -> changed end;
        {RunTaskFun, GuardFun} when is_function(RunTaskFun)
                                  , is_function(GuardFun) ->
            ok;
        _ ->
            RunTaskFun = undefined, %% D'oh!
            GuardFun = undefined,
            error({badtask, Exec})
    end,
    NoGuards = maps:get(disable_guards, Settings, false),
    {ok, startup, #d{ id             = Id
                    , data           = Data
                    , exec_fun       = RunTaskFun
                    , guard_fun      = GuardFun
                    , resources      = Resources
                    , parent         = Parent
                    , event_mgr      = EventMgr
                    , get_result_fun = GetResultFun
                    , no_guards      = NoGuards
                    }}.

%% Startup state:
handle_event(enter, _, startup, _Data) ->
    keep_state_and_data;
handle_event( {call, From}
            , {add_requirement, Id, Pid}
            , startup
            , Data0 = #d{ requires = R0
                        , n_deps = N0
                        }
            ) ->
    case R0 of
        #{Id := Pid0} ->
            N = N0,
            %% Assert:
            Pid = Pid0;
        _ ->
            N = N0 + 1
    end,
    Data = Data0#d{ requires       = R0#{Id => Pid}
                  , n_deps         = N
                  , n_changed_deps = N
                  },
    {keep_state, Data, [{reply, From, ok}]};
handle_event( {call, From}
            , {add_consumer, Id, Pid}
            , startup
            , Data0 = #d{provides = P0}
            ) ->
    case P0 of
        #{Id := Pid0} ->
            %% Assert:
            Pid = Pid0;
        _ ->
            ok
    end,
    Data = Data0#d{ provides = P0#{Id => Pid}
                  },
    {keep_state, Data, [{reply, From, ok}]};
handle_event(cast, launch, startup, D = #d{n_deps = N}) ->
    case N of
        0 ->
            do_run_guard(D);
        _ ->
            {next_state, wait_deps, D}
    end;

%% wait_deps state:
handle_event( cast
            , {dep_failed, DepId}
            , wait_deps
            , Data
            ) ->
    complete_task(Data, {aborted, {dependency_failed, DepId}}, true, undefined);
handle_event( cast
            , {dep_complete, Id, Unchanged}
            , wait_deps
            , Data0 = #d{ n_deps         = N
                        , n_changed_deps = U
                        , requires       = Req
                        }
            ) ->
    %% Assert:
    #{Id := _} = Req,
    %% Assert:
    true = N > 0,
    %% Assert:
    true = U >= N,
    Data = Data0#d{ n_deps         = N - 1
                  , n_changed_deps = if Unchanged ->
                                             U - 1;
                                        true ->
                                             U
                                     end
                  },
    check_deps(Data);

%% wait_resources state
handle_event(enter, _, wait_resources, Data) ->
    Action = {timeout, 0, check_resources},
    {next_state, wait_resources, Data, Action};
handle_event( timeout
            , check_resources
            , wait_resources
            , #d{ id = Id
                , resources = Resources
                , parent = Parent
                , rank = Rank
                }
            ) ->
    case task_graph_resource:is_empty(Resources) of
        true ->
            %% No resources to allocate, skip call to Parent
            resources_acquired(self());
        false ->
            task_graph_server:grab_resources(Parent, Id, Rank, Resources)
    end,
    keep_state_and_data;
handle_event(cast, resources_acquired, wait_resources, Data) ->
    do_run_task(Data);

%% Now when `task_graph_server' recorded our return value it's time to
%% notify consumers:
handle_event(cast, rip, complete, #d{epitaph = Epitaph, provides = Prov}) ->
    %% Send task completion status (epitaph) to the downstream
    %% dependencies:
    maps:map( fun(_Id, Pid) ->
                      gen_statem:cast(Pid, Epitaph)
              end
            , Prov
            ),
    {stop, normal};
handle_event(_, _, complete, _) ->
    keep_state_and_data;

%% Common actions:
%% Gracefully abort execution at any state:
handle_event(cast, {abort, Reason}, _, Data) ->
    complete_task(Data, {aborted, Reason}, true, undefined);
handle_event(enter, _, NewState, Data) ->
    {next_state, NewState, Data}.

terminate(_Reason, _State, _Data) ->
    void.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_deps(Data = #d{n_deps = N, n_changed_deps = C}) ->
    case {N, C} of
        {0, 0} ->
            do_run_guard(Data);
        {0, _} ->
            {next_state, wait_resources, Data};
        _ ->
            {keep_state, Data}
    end.

-spec do_run_guard(#d{}) -> false | {next_state, complete, #d{}}.
do_run_guard(Data = #d{ no_guards = true}) ->
    {next_state, wait_resources, Data};
do_run_guard(Data = #d{ event_mgr      = EventMgr
                      , guard_fun      = GuardFun
                      , data           = Payload
                      , id             = Ref
                      , get_result_fun = GetDepResult
                      }
            ) ->
    event(run_guard, Ref, EventMgr),
    try GuardFun(Ref, Payload, GetDepResult) of
        unchanged ->
            complete_task(Data, {ok, undefined}, false, undefined);
        {unchanged, Return} ->
            complete_task(Data, {ok, Return}, false, undefined);
        changed ->
            {next_state, wait_resources, Data};
        BadReturn ->
            complete_task(Data, {error, {badreturn, BadReturn}}, true, undefined)
    catch
        _:Err ?BIND_STACKTRACE(Stack) ->
            ?GET_STACKTRACE(Stack),
            complete_task( Data
                         , {error, {uncaught_exception, Err, Stack}}
                         , _changed = true
                         , undefined
                         )
    after
        event(guard_complete, Ref, EventMgr)
    end.

-spec do_run_task(#d{}) -> {next_state, complete, #d{}}.
do_run_task( Data = #d{ id             = Ref
                      , data           = Payload
                      , event_mgr      = EventMgr
                      , exec_fun       = RunTaskFun
                      , get_result_fun = GetDepResult
                      }
           ) ->
    event(spawn_task, Ref, EventMgr),
    try RunTaskFun(Ref, Payload, GetDepResult) of
        ok ->
            complete_task( Data
                         , {ok, undefined}
                         , _changed = true
                         , undefined
                         );
        {ok, Result} ->
            complete_task( Data
                         , {ok, Result}
                         , _changed = true
                         , undefined
                         );
        {ok, Result, NewTasks} ->
            complete_task( Data
                         , {ok, Result}
                         , _changed = true
                         , NewTasks
                         );

        {defer, NewTasks} ->
            defer_task(Data, NewTasks);

        {error, Reason} ->
            complete_task( Data
                         , {error, Reason}
                         , _changed = true
                         , undefined
                         );

        BadReturn ->
            complete_task( Data
                         , {error, {badreturn, BadReturn}}
                         , _changed = true
                         , undefined
                         )
    catch
        _:Err ?BIND_STACKTRACE(Stack) ->
            ?GET_STACKTRACE(Stack),
            complete_task( Data
                         , {error, {uncaught_exception, Err, Stack}}
                         , _changed = true
                         , undefined
                         )
    end.

-spec complete_task( #d{}
                   , {task_graph_server:result_type(), term()}
                   , boolean()
                   , task_graph:digraph() | undefined
                   ) -> {next_state, complete, #d{}}.
complete_task(Data = #d{ id        = Id
                       , parent    = Parent
                       , event_mgr = EventMgr
                       , resources = Resources
                       }
             , {ok, Result}
             , Changed
             , NewTasks
             ) ->
    Changed andalso event(complete_task, Id, EventMgr),
    task_graph_server:complete_task(Parent, Id, {ok, Result}, Resources, NewTasks),
    Epitaph = {dep_complete, Id, not Changed},
    {next_state, complete, Data#d{ epitaph = Epitaph }};
complete_task(Data = #d{ id        = Id
                       , parent    = Parent
                       , event_mgr = EventMgr
                       , resources = Resources
                       }
             , {ReturnType, Error}
             , _Changed
             , _NewTasks
             ) ->
    event(task_failed, [Id, Error], EventMgr),
    task_graph_server:complete_task(Parent, Id, {ReturnType, Error}, Resources, undefined),
    {next_state, complete, Data#d{ epitaph = {dep_failed, Id}
                                 }}.

-spec defer_task(#d{}, task_graph:digraph()) -> {next_state, startup, #d{}}.
defer_task(Data = #d{parent = Pid, id = Id, event_mgr = EventMgr}, NewTasks) ->
    event(defer_task, Id, EventMgr),
    task_graph_server:extend_graph(Pid, Id, NewTasks),
    {next_state, startup, Data}.
