-module(task_graph_server).

-behaviour(gen_server).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state,
        { graph                            :: task_graph_digraph:digraph()
        , event_mgr                        :: event_mgr()
        , candidates = []                  :: [task_graph_digraph:candidate()]
        , current_tasks = #{}              :: #{task_graph:task_id() => pid()}
        , failed_tasks = #{}               :: #{task_graph:task_id() => term()}
        , parent                           :: pid()
        , guards                           :: boolean()
        }).

-type event_mgr() :: atom()
                   | {atom(), atom()}
                   | {global, term()}
                   | pid()
                   | undefined
                   .

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , #{task_graph:settings_key() => term()}
               , task_graph:digraph()
               ) -> {ok, term()} | {error, term()}.
run_graph(TaskName, Settings, Tasks) ->
    case maps:get(event_manager, Settings, undefined) of
        EventMgr when is_pid(EventMgr) ->
            ok;
        undefined ->
            {ok, EventMgr} = gen_event:start_link(),
            lists:foreach( fun({Handler, Args}) ->
                                   gen_event:add_handler(EventMgr, Handler, Args)
                           end
                         , maps:get(event_handlers, Settings, [])
                         )
    end,
    Ret = gen_server:start( {local, TaskName}
                          , ?MODULE
                          , {TaskName, EventMgr, Settings, Tasks, self()}
                          , []
                          ),
    case Ret of
        {ok, Pid} ->
            Ref = monitor(process, Pid),
            receive
                {result, Pid, Result} ->
                    gen_event:stop(EventMgr, normal, infinity),
                    %% Make sure the server terminated:
                    receive
                        {'DOWN', Ref, process, Pid, _} ->
                            ok
                    after 1000 ->
                            %% Should not happen
                            exit(Pid, kill),
                            error({timeout_waiting_for, Pid})
                    end,
                    Result;
                {'DOWN', Ref, process, Pid, Reason} ->
                    {error, {internal_error, Reason}}
            end;
        Err ->
            Err
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, EventMgr, Settings, Tasks, Parent}) ->
    ResourceLimits = maps:get(resources, Settings, #{}),
    Graph = task_graph_digraph:new_graph(TaskName, ResourceLimits),
    try
        ok = push_tasks(Tasks, Graph, EventMgr, undefined),
        maybe_pop_tasks(),
        {ok, #state{ graph = Graph
                   , event_mgr = EventMgr
                   , parent = Parent
                   , guards = not maps:get(disable_guards, Settings, false)
                   , candidates = task_graph_digraph:unlocked_tasks(Graph)
                   }}
    catch
        _:{badmatch, {error, circular_dependencies, Cycle}} ->
            {stop, {topology_error, Cycle}}
    end.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({complete_task, Ref, _Success = false, _Changed, Return, _}, State) ->
    maybe_pop_tasks(),
    {noreply, push_error(Ref, Return, State)};
handle_cast({complete_task, Ref, _Success = true, Changed, Return, NewTasks}, State) ->
    #state{ candidates = OldCandidates
          , graph = G
          } = State,
    event(complete_task, Ref, State),
    {ok, Unlocked} = task_graph_digraph:complete_task(G, Ref, Changed, Return),
    State1 =
        case push_tasks(NewTasks, State#state{graph = G}, undefined) of
            ok ->
                Candidates =
                    case NewTasks of
                        {[], []} ->
                            merge_candidates(Unlocked, OldCandidates);
                        _ ->
                            %% TODO: Perhaps it's not necessary to do a full search
                            task_graph_digraph:unlocked_tasks(G)
                    end,
                State#state{ current_tasks =
                                 maps:remove(Ref, State#state.current_tasks)
                           , candidates = Candidates
                           };
            Err ->
                %% Dynamically added tasks are malformed or break the topology
                push_error(Ref, Err, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast({defer_task, Ref, NewTasks}, State) ->
    #state{ current_tasks = Curr
          , graph = G
          } = State,
    event(defer_task, Ref, State),
    State1 =
        case push_tasks(NewTasks, State, {just, Ref}) of
            ok ->
                State#state{ current_tasks = map_sets:del_element(Ref, Curr)
                           , candidates = task_graph_digraph:unlocked_tasks(G)
                           };
            Err ->
                push_error(Ref, Err, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast(maybe_pop_tasks, State) ->
    #state{ graph = G
          , current_tasks = CurrentlyRunningTasks
          , candidates = Candidates
          } = State,
    event(shed_begin, State),
    case is_success(State) of
        true ->
            %% There are no failed tasks, proceed
            {ok, NewCandidates, Tasks} =
                task_graph_digraph:pre_schedule_tasks( G
                                                     , Candidates
                                                     );
        false ->
            %% Something's failed, don't schedule new tasks
            Tasks = [],
            NewCandidates = Candidates
    end,
    event(shed_end, State),
    case Tasks of
        [] ->
            case is_complete(State) of
                true ->
                    %% Task graph
                    complete_graph(State);
                false ->
                    %% All tasks have unresolved dependencies, wait
                    {noreply, State}
            end;
        _ ->
            %% Schedule new tasks
            State1 = State#state{ candidates = NewCandidates
                                },
            State2 = lists:foldl( fun run_task/2
                                , State1
                                , Tasks
                                ),
            {noreply, State2}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    task_graph_digraph:delete_graph(State#state.graph),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec push_tasks( task_graph:digraph()
                , #state{}
                , task_graph:maybe(task_graph:task_id())
                ) -> ok
                   | {error, term()}.
push_tasks(NewTasks, #state{graph = G, event_mgr = EventMgr}, Parent) ->
    push_tasks(NewTasks, G, EventMgr, Parent).

push_tasks({[], []}, _Graph, _, _) ->
    ok;
push_tasks(NewTasks = {Vertices, Edges}, G, EventMgr, Parent) ->
    event(extend_begin, EventMgr),
    event(add_tasks, Vertices, EventMgr),
    event(add_dependencies, Edges, EventMgr),
    Ret = task_graph_digraph:expand(G, NewTasks, Parent),
    event(extend_end, EventMgr),
    Ret.

event(A, B) ->
    event(A, undefined, B).
event(Kind, Data, #state{event_mgr = EventMgr}) ->
    event(Kind, Data, EventMgr);
event(Kind, Data, EventMgr) when is_pid(EventMgr) ->
    gen_event:notify( EventMgr
                    , #tg_event{ timestamp = erlang:system_time(?tg_timeUnit)
                               , kind = Kind
                               , data = Data
                               }
                    ).

complete_graph(State = #state{parent = Pid, failed_tasks = Failed}) ->
    %% Assert:
    #{} = State#state.current_tasks,
    Result = case is_success(State) of
                 true ->
                     {ok, Failed};
                 false ->
                     {error, Failed}
             end,
    event(graph_complete, [self(), Result], State),
    Pid ! {result, self(), Result},
    {stop, normal, State}.

is_success(State) ->
    maps:size(State#state.failed_tasks) == 0.

is_complete(State) ->
    maps:size(State#state.current_tasks) == 0.

push_error(Ref, Error, State) ->
    OldFailedTasks = State#state.failed_tasks,
    OldCurrentTasks = State#state.current_tasks,
    event(task_failed, [Ref, Error], State),
    State#state{ failed_tasks = OldFailedTasks#{Ref => Error}
               , current_tasks = maps:remove(Ref, OldCurrentTasks)
               }.

maybe_pop_tasks() ->
    gen_server:cast(self(), maybe_pop_tasks).

-spec complete_task( pid()
                   , task_graph:task_id()
                   , boolean()
                   , boolean()
                   , term()
                   , task_graph:digraph()
                   ) -> ok.
complete_task(Parent, Ref, Success, Changed, Return, NewTasks) ->
    gen_server:cast(Parent, {complete_task, Ref, Success, Changed, Return, NewTasks}).

-spec complete_task(pid(), task_graph:task_id(), boolean(), boolean(), term()) -> ok.
complete_task(Parent, Ref, Success, Changed, Return) ->
    complete_task(Parent, Ref, Success, Changed, Return, {[], []}).

-spec defer_task(pid(), task_graph:task_id(), task_graph:digraph()) -> ok.
defer_task(Parent, Ref, NewTasks) ->
    gen_server:cast(Parent, {defer_task, Ref, NewTasks}).

-spec run_task({task_graph:task(), boolean()}, #state{}) -> #state{}.
run_task( {Task = #tg_task{id = Ref}, DepsUnchanged}
        , State = #state{ current_tasks = TT
                        , graph = G
                        , guards = Guards
                        }) ->
    GetDepResult = fun(Ref1) ->
                           task_graph_digraph:get_task_result(G, Ref1)
                   end,
    Pid = spawn_task( Task
                    , State#state.event_mgr
                    , GetDepResult
                    , DepsUnchanged andalso Guards
                    ),
    State#state{current_tasks = TT#{Ref => Pid}}.

-spec spawn_task( task_graph:task()
                , event_mgr()
                , fun((task_graph:task_id()) -> {ok, term()} | error)
                , boolean()
                ) -> pid().
spawn_task( Task = #tg_task{ id = Ref
                           , execute = Exec
                           }
          , EventMgr
          , GetDepResult
          , DepsUnchanged
          ) ->
    Parent = self(),
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
    spawn_link(
      fun() ->
              try
                  Unchanged = DepsUnchanged andalso do_run_guard( Parent
                                                                , EventMgr
                                                                , GuardFun
                                                                , Task
                                                                , GetDepResult
                                                                ),
                  if Unchanged ->
                          ok;
                     true ->
                          do_run_task( Parent
                                     , EventMgr
                                     , RunTaskFun
                                     , Task
                                     , GetDepResult
                                     )
                  end
              catch
                  _:Err ?BIND_STACKTRACE(Stack) ->
                      ?GET_STACKTRACE(Stack),
                      complete_task( Parent
                                   , Ref
                                   , _success = false
                                   , _changed = true
                                   , {uncaught_exception, Err, Stack}
                                   )
              end
      end).

-spec merge_candidates( [task_graph_digraph:candidate()]
                      , [task_graph_digraph:candidate()]
                      ) -> [task_graph_digraph:candidate()].
merge_candidates(C1, C2) ->
    C1 ++ C2.

-spec do_run_guard( pid()
                  , pid()
                  , fun()
                  , task_graph:task()
                  , task_runner:get_deps_result()
                  ) -> boolean().
do_run_guard( Parent
            , EventMgr
            , GuardFun
            , #tg_task{ id = Ref
                      , data = Data
                      }
            , GetDepResult
            ) ->
    event(run_guard, Ref, EventMgr),
    Result = GuardFun(Ref, Data, GetDepResult),
    event(guard_complete, Ref, EventMgr),
    case Result of
        unchanged ->
            complete_task(Parent, Ref, true, false, undefined),
            true;
        {unchanged, Return} ->
            complete_task(Parent, Ref, true, false, Return),
            true;
        changed ->
            false
    end.

-spec do_run_task( pid()
                 , pid()
                 , fun()
                 , task_graph:task()
                 , task_runner:get_deps_result()
                 ) -> ok.
do_run_task( Parent
           , EventMgr
           , RunTaskFun
           , #tg_task{ id = Ref
                     , data = Data
                     }
           , GetDepResult
           ) ->
    event(spawn_task, Ref, EventMgr),
    Return = RunTaskFun(Ref, Data, GetDepResult),
    case Return of
        ok ->
            complete_task( Parent
                         , Ref
                         , _success = true
                         , _changed = true
                         , undefined
                         );
        {ok, Result} ->
            complete_task( Parent
                         , Ref
                         , _success = true
                         , _changed = true
                         , Result
                         );
        {ok, Result, NewTasks} ->
            complete_task( Parent
                         , Ref
                         , _success = true
                         , _changed = true
                         , Result
                         , NewTasks
                         );

        {defer, NewTasks} ->
            defer_task(Parent, Ref, NewTasks);

        {error, Reason} ->
            complete_task( Parent
                         , Ref
                         , _success = false
                         , _changed = true
                         , Reason
                         )
    end.
