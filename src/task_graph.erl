-module(task_graph).

-behaviour(gen_server).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        , run_graph/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type worker_state() :: term().

-type event_mgr() :: atom()
                   | {atom(), atom()}
                   | {global, term()}
                   | pid()
                   | undefined.

-type settings_key() :: jobs
                      | event_handlers.

-record(worker_pool,
        { workers :: #{pid() => worker_state()}
        , cap :: non_neg_integer() | unlimited
        }).

-record(state,
        { graph                            :: task_graph_lib:graph()
        , event_mgr                        :: event_mgr()
        , current_tasks = #{}              :: #{task_graph_lib:task_id() => pid()}
        , failed_tasks = #{}               :: #{task_graph_lib:task_id() => term()}
        , workers                          :: #{task_graph_lib:task_execute() =>
                                                    #worker_pool{}}
        %% , results    :: #{task_id() => term()}
        , parent                           :: pid()
        , global_job_limit                 :: non_neg_integer() | infinity
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , task_manager_lib:tasks()
               ) -> {ok, term()} | {error, term()}.
run_graph(TaskName, Tasks) ->
    run_graph(TaskName, #{}, Tasks).

-spec run_graph( atom()
               , #{settings_key() => term()}
               , task_manager_lib:tasks()
               ) -> {ok, term()} | {error, term()}.
run_graph(TaskName, Settings, Tasks) ->
    {ok, EventMgr} = gen_event:start_link(),
    lists:foreach( fun({Handler, Args}) ->
                           gen_event:add_handler(EventMgr, Handler, Args)
                   end
                 , maps:get(event_handlers, Settings, [])
                 ),
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
    Graph = task_graph_lib:new_graph(TaskName, ResourceLimits),
    try
        {ok, Graph1} = push_tasks(Tasks, Graph, EventMgr, undefined),
        JobLimit = maps:get(jobs, Settings, infinity),
        %% io:format(user, "~s~n", [task_graph_lib:print_graph(Graph2)]),
        maybe_pop_tasks(),
        {ok, #state{ graph = Graph1
                   , workers = #{}
                   , event_mgr = EventMgr
                   %% , result = #{}
                   , parent = Parent
                   , global_job_limit = JobLimit
                   }}
    catch
        _:{badmatch,{error, circular_dependencies, Cycle}} ->
            {stop, {topology_error, Cycle}}
    end.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({complete_task, Ref, _Success = false, Return, _}, State) ->
    maybe_pop_tasks(),
    {noreply, push_error(Ref, Return, State)};
handle_cast({complete_task, Ref, _Success = true, Return, NewTasks}, State) ->
    #state{ graph = G0
          } = State,
    event(complete_task, Ref, State),
    {ok, G1} = task_graph_lib:complete_task(G0, Ref, Return),
    State1 =
        case push_tasks(NewTasks, State#state{graph = G1}, undefined) of
            {ok, G2} ->
                State#state{ graph = G2
                           , current_tasks =
                                 maps:remove(Ref, State#state.current_tasks)
                           };
            Err ->
                %% Dynamically added tasks are malformed or break the topology
                push_error(Ref, Err, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast({defer_task, Ref, NewTasks}, State) ->
    #state{ current_tasks = Curr
          } = State,
    State1 =
        case push_tasks(NewTasks, State, {just, Ref}) of
            {ok, G1} ->
                State#state{ current_tasks = map_sets:del_element(Ref, Curr)
                           , graph = G1
                           };
            Err ->
                push_error(Ref, Err, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast(maybe_pop_tasks, State) ->
    #state{ graph = G
          , current_tasks = CurrentlyRunningTasks
          } = State,
    event(shed_begin, State),
    case is_success(State) of
        true ->
            %% There are no failed tasks, proceed
            {ok, Tasks0} = task_graph_lib:search_tasks( G
                                                      , CurrentlyRunningTasks
                                                      ),
            case State#state.global_job_limit of
                infinity ->
                    Tasks = Tasks0;
                N ->
                    Tasks = lists:sublist( Tasks0
                                         , N - maps:size(CurrentlyRunningTasks)
                                         )
            end;
        false ->
            %% Something's failed, don't schedule new tasks
            Tasks = []
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
            event(scheduled_tasks, Tasks, State),
            State2 = lists:foldl( fun run_task/2
                                , State
                                , Tasks
                                ),
            {noreply, State2}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    task_graph_lib:delete_graph(State#state.graph),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec push_tasks( task_graph_lib:tasks()
                , #state{}
                , task_graph_lib:maybe(task_graph_lib:task_id())
                ) -> {ok, task_graph_lib:task_graph()}
                   | {error, term()}.
push_tasks(NewTasks, #state{graph = G0, event_mgr = EventMgr}, Parent) ->
    push_tasks(NewTasks, G0, EventMgr, Parent).
push_tasks(NewTasks = {Vertices, Edges}, G0, EventMgr, Parent) ->
    event(extend_begin, EventMgr),
    event(add_tasks, Vertices, EventMgr),
    event(add_dependencies, Edges, EventMgr),
    Ret = task_graph_lib:expand(G0, NewTasks, Parent),
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
                   , task_graph_lib:task_id()
                   , boolean()
                   , term()
                   , task_graph_lib:tasks()
                   ) -> ok.
complete_task(Parent, Ref, Successp, Return, NewTasks) ->
    gen_server:cast(Parent, {complete_task, Ref, Successp, Return, NewTasks}).

-spec complete_task(pid(), task_graph_lib:task_id(), boolean(), term()) -> ok.
complete_task(Parent, Ref, Success, Return) ->
    complete_task(Parent, Ref, Success, Return, {[], []}).

-spec defer_task(pid(), task_graph_lib:task_id(), task_graph_lib:tasks()) -> ok.
defer_task(Parent, Ref, NewTasks) ->
    gen_server:cast(Parent, {defer_task, Ref, NewTasks}).

-spec run_task(task_graph_lib:task(), #state{}) -> #state{}.
run_task(Task = #task{task_id = Ref}, State = #state{current_tasks = TT, graph = G}) ->
    GetDepResult = fun(Ref1) ->
                           task_graph_lib:get_task_result(G, Ref1)
                   end,
    Pid = spawn_worker(Task, todo_undefined, State#state.event_mgr, GetDepResult),
    State#state{current_tasks = TT#{Ref => Pid}}.

-spec spawn_worker( task_graph_lib:task()
                  , term()
                  , event_mgr()
                  , fun((task_graph_lib:task_id()) -> {ok, term()} | error)
                  ) -> pid().
spawn_worker(#task{task_id = Ref, execute = Exec, data = Data}, WorkerState, EventMgr, GetDepResult) ->
    Parent = self(),
    spawn_link(
      fun() ->
          event(spawn_task, [Ref, self()], EventMgr),
          try

              Return =
                  if is_atom(Exec) ->
                          Exec:run_task(WorkerState, Ref, Data, GetDepResult);
                     is_function(Exec) ->
                          Exec(Ref, Data, GetDepResult)
                  end,
              case Return of
                  ok ->
                      complete_task(Parent, Ref, true, undefined);
                  {ok, Result} ->
                      complete_task(Parent, Ref, true, Result);
                  {ok, Result, NewTasks} ->
                      complete_task(Parent, Ref, true, Result, NewTasks);

                  unchanged ->
                      complete_task(Parent, Ref, true, undefined);
                  {unchanged, Result} ->
                      complete_task(Parent, Ref, true, Result);
                  {unchanged, Result, NewTasks} ->
                      complete_task(Parent, Ref, true, Result, NewTasks);

                  {defer, NewTasks} ->
                      defer_task(Parent, Ref, NewTasks);

                  {error, Reason} ->
                      complete_task(Parent, Ref, false, Reason)
              end
          catch
              _:Err ->
                  complete_task( Parent
                               , Ref
                               , _success = false
                               , {uncaught_exception, Err, erlang:get_stacktrace()}
                               )
          end
      end).
