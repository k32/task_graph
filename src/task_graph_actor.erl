-module(task_graph_actor).

-behaviour(gen_statem).

-include("task_graph_int.hrl").

%% API
-export([ start_link/3
        , add_consumer/3
        , add_requirement/3
        , launch/1
        ]).

%% gen_statem callbacks
-export([callback_mode/0, handle_event/4, init/1, terminate/3, code_change/4]).

-record(d,
        { id                         :: task_graph:task_id()
        , data                       :: term()
        , requires = #{}             :: #{task_graph:task_id() => pid()}
        , provides = #{}             :: #{task_graph:task_id() => pid()}
        , exec_fun                   :: fun()
        , n_deps = 0                 :: non_neg_integer()
        , n_changed_deps = 0         :: non_neg_integer()
        , guard_fun                  :: fun()
        , resources                  :: [task_graph:resource_id()]
        , parent                     :: pid()
        , event_mgr                  :: pid()
        , get_result_fun             :: task_graph_runner:get_deps_result()
        }).

-import(task_graph_server, [event/3, event/2]).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link( pid()
                , task_graph:task()
                , task_graph_runner:get_deps_result()
                ) ->
                        {ok, Pid :: pid()} |
                        ignore |
                        {error, Error :: term()}.
start_link(EventMgr, Task, GetResultFun) ->
    Attrs = [self(), Task, EventMgr, GetResultFun],
    gen_statem:start_link(?MODULE, Attrs, []).

add_consumer(Pid, To, PTo) ->
    gen_statem:call(Pid, {add_consumer, To, PTo}).

add_requirement(Pid, From, PFrom) ->
    gen_statem:call(Pid, {add_requirement, From, PFrom}).

launch(Pid) ->
    gen_statem:cast(Pid, launch).

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
    {ok, startup, #d{ id             = Id
                    , data           = Data
                    , exec_fun       = RunTaskFun
                    , guard_fun      = GuardFun
                    , resources      = Resources
                    , parent         = Parent
                    , event_mgr      = EventMgr
                    , get_result_fun = GetResultFun
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
handle_event(cast, launch, startup, D = #d{n_deps = N, resources = R}) ->
    case N of
        0 ->
            {next_state, wait_resources, D};
        _ ->
            {next_state, wait_deps, D}
    end;

%% wait_deps state:
handle_event(enter, _, wait_deps, Data) ->
    {next_state, wait_deps, Data};
handle_event( cast
            , {dep_failed, DepId}
            , wait_deps
            , Data
            ) ->
    complete_task(Data, {aborted, {dependency_failed, DepId}}, true, undefined),
    {stop, normal};
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
    self() ! check_resources,
    {next_state, wait_resources, Data};
handle_event(info, check_resources, wait_resources, Data) ->
    execute(Data).

terminate(_Reason, _State, _Data) ->
    void.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_deps(Data = #d{n_deps = N}) ->
    case N of
        0 ->
            {next_state, wait_resources, Data};
        _ ->
            {keep_state, Data}
    end.

-spec execute(#d{}) -> {stop, term()}.
execute(Data) ->
    try
        DepsUnchanged = Data#d.n_changed_deps =:= 0,
        case DepsUnchanged andalso do_run_guard(Data) of
            false ->
                do_run_task(Data);
            Result ->
                Result
        end
    catch
        _:Err ?BIND_STACKTRACE(Stack) ->
            ?GET_STACKTRACE(Stack),
            complete_task( Data
                         , {error, {uncaught_exception, Err, Stack}}
                         , _changed = true
                         , undefined
                         )
    end.

-spec do_run_guard(#d{}) -> false | {stop, term()}.
do_run_guard( Data = #d{ event_mgr      = EventMgr
                       , guard_fun      = GuardFun
                       , data           = Payload
                       , id             = Ref
                       , get_result_fun = GetDepResult
                       }
            ) ->
    event(run_guard, Ref, EventMgr),
    Result = GuardFun(Ref, Payload, GetDepResult),
    event(guard_complete, Ref, EventMgr),
    case Result of
        unchanged ->
            complete_task(Data, {ok, undefined}, false, undefined);
        {unchanged, Return} ->
            complete_task(Data, {ok, Return}, false, undefined);
        changed ->
            false
    end.

-spec do_run_task(#d{}) -> {stop, normal}.
do_run_task( Data = #d{ id             = Ref
                      , data           = Payload
                      , event_mgr      = EventMgr
                      , parent         = Parent
                      , exec_fun       = RunTaskFun
                      , get_result_fun = GetDepResult
                      }
           ) ->
    event(spawn_task, Ref, EventMgr),
    Return = RunTaskFun(Ref, Payload, GetDepResult),
    case Return of
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
                         )
    end.

-spec complete_task( #d{}
                   , {task_graph_server:result_type(), term()}
                   , boolean()
                   , task_graph:digraph() | undefined
                   ) -> {stop, normal}.
complete_task(#d{ id        = Id
                , parent    = Parent
                , event_mgr = EventMgr
                , provides  = Prov
                }
             , {ok, Result}
             , Changed
             , NewTasks
             ) ->
    event(complete_task, Id, EventMgr),
    task_graph_server:complete_task(Parent, Id, {ok, Result}, NewTasks),
    maps:map( fun(_Id, Pid) ->
                      gen_statem:cast(Pid, {dep_complete, Id, not Changed})
              end
            , Prov
            ),
    {stop, normal};
complete_task(#d{ id        = Id
                , parent    = Parent
                , event_mgr = EventMgr
                , provides  = Prov
                }
             , {ReturnType, Error}
             , _Changed
             , _NewTasks
             ) ->
    event(task_failed, [Id, Error], EventMgr),
    maps:map( fun(_Id, Pid) ->
                      gen_statem:cast(Pid, {dep_failed, Id})
              end
            , Prov
            ),
    task_graph_server:complete_task(Parent, Id, {ReturnType, Error}, undefined),
    {stop, normal}.

-spec defer_task(#d{}, task_graph:digraph()) -> {next_state, startup, #d{}}.
defer_task(Data = #d{parent = Pid, id = Id, event_mgr = EventMgr}, NewTasks) ->
    event(defer_task, Id, EventMgr),
    task_graph_server:extend_graph(Pid, Id, NewTasks),
    {next_state, startup, Data}.
