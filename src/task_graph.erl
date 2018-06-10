-module(task_graph).

-behaviour(gen_server).

-include_lib("task_graph/include/task_graph.hrl").

%% API
-export([run_graph/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type worker_state() :: term().

-type event_mgr() :: atom()
                   | {atom(), atom()}
                   | {global, term()}
                   | pid()
                   | undefined.

-record(worker_pool,
        { workers :: #{pid() => worker_state()}
        , cap :: non_neg_integer() | unlimited
        }).

-record(state,
        { graph                      :: task_graph_lib:graph()
        , event_sink                 :: event_mgr()
        , current_tasks = #{}        :: #{task_graph_lib:task_id() => pid()}
        , failed_tasks = #{}         :: #{task_graph_lib:task_id() => term()}
        , workers                    :: #{task_graph_lib:worker_module() => #worker_pool{}}
        %% , results    :: #{task_id() => term()}
        , parent                     :: pid()
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec run_graph( atom()
%%               , event_mgr()
               , #{task_graph_lib:worker_module() => non_neg_integer()}
               , task_manager_lib:tasks()
               ) -> {ok, term()} | {error, term()}.
run_graph(TaskName, PoolSizes, Tasks) ->
    EventMgr = undefined,
    {ok, Pid} = gen_server:start( {local, TaskName}
                                , ?MODULE
                                , {TaskName, EventMgr, PoolSizes, Tasks, self()}
                                , []
                                ),
    Ref = monitor(process, Pid),
    receive
        {result, Pid, Result} ->
            Result;
        {'DOWN', Ref, process, Pid, Reason} ->
            {error, {internal_error, Reason}}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, EventMgr, PoolSizes, {Nodes, Edges}, Parent}) ->
    Graph = task_graph_lib:new_graph(TaskName),
    {ok, Graph1} = task_graph_lib:add_tasks(Graph, Nodes),
    {ok, Graph2} = task_graph_lib:add_dependencies(Graph1, Edges),
    %% io:format(user, "~s~n", [task_graph_lib:print_graph(Graph2)]),
    maybe_pop_tasks(),
    {ok, #state{ graph = Graph2
               , workers = #{}
               %% , result = #{}
               , parent = Parent
               }}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({complete_task, Ref, _Success = false, Return, _}, State) ->
    maybe_pop_tasks(),
    {noreply, push_error(Ref, Return, State)};
handle_cast({complete_task, Ref, _Success = true, Return, NewTasks}, State) ->
    #state{ graph = G0
          } = State,
    event({complete_task, Ref}, State),
    State1 =
        case check_new_tasks(false, NewTasks, Ref) of
            true ->
                {ok, G1} = task_graph_lib:complete_task(G0, Ref),
                State#state{ graph = G1
                           , current_tasks =
                                 maps:remove(Ref, State#state.current_tasks)
                           };
            false ->
                %% Dynamically added tasks break the topology
                push_error(Ref, {topology_error, NewTasks}, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast({defer_task, Ref, NewTasks}, State) ->
    #state{ graph = G0
          , current_tasks = Curr
          } = State,
    State1 =
        case {check_new_tasks(true, NewTasks, Ref), task_graph_lib:expand(G0, NewTasks)} of
            {true, {ok, G1}} ->
                State#state{ current_tasks = map_sets:del_element(Ref, Curr)
                           , graph = G1
                           };
            _ ->
                push_error(Ref, {topology_error, NewTasks}, State)
        end,
    maybe_pop_tasks(),
    {noreply, State1};
handle_cast(maybe_pop_tasks, State) ->
    #state{ graph = G
          , current_tasks = CurrentlyRunningTasks
          } = State,
    SaturatedSchedulers = map_sets:new(), %% TODO
    event(pop_tasks, State),
    case is_success(State) of
        true ->
            %% There are no failed tasks, proceed
            {ok, Tasks} = task_graph_lib:search_tasks( G
                                                     , SaturatedSchedulers
                                                     , CurrentlyRunningTasks
                                                     );
        false ->
            %% Something's failed, don't schedule new tasks
            Tasks = []
    end,
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
            event({scheduled_tasks, Tasks}, State),
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

%% Dynamically added tasks should not introduce new dependencies for
%% any existing tasks
-spec check_new_tasks( boolean()
                     , task_graph_lib:tasks()
                     , task_graph_lib:task_id()
                     ) -> boolean().
check_new_tasks(Defer, {Vertices, Edges}, ParentTask) ->
    L0 = [Ref || #task{task_id = Ref} <- Vertices],
    New = case Defer of
              true ->
                  map_sets:from_list([ParentTask|L0]);
              false ->
                  map_sets:from_list(L0)
          end,
    Deps = map_sets:from_list([To || {_, To} <- Edges]),
    map_sets:is_subset(Deps, New).

event(Term, State) ->
    %% io:format("Event: ~p~nState~p~n", [Term, State]),
    %% case State of
    %%     #state{graph = G} ->
    %%         io:format(user, "~s", [task_graph_lib:print_graph(G)]);
    %%     _ ->
    %%         ok
    %% end,
    todo.

complete_graph(State = #state{parent = Pid, failed_tasks = Failed}) ->
    %% Assert:
    #{} = State#state.current_tasks,
    Result = case is_success(State) of
                 true ->
                     {ok, Failed};
                 false ->
                     {error, Failed}
             end,
    event({graph_complete, self(), Result}, State),
    Pid ! {result, self(), Result},
    {stop, normal, State}.

is_success(State) ->
    maps:size(State#state.failed_tasks) == 0.

is_complete(State) ->
    maps:size(State#state.current_tasks) == 0.

push_error(Ref, Error, State) ->
    OldFailedTasks = State#state.failed_tasks,
    OldCurrentTasks = State#state.current_tasks,
    event({task_failed, Ref, Error}, State),
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
run_task(Task = #task{task_id = Ref}, State = #state{current_tasks = TT}) ->
    Pid = spawn_worker(Task, todo_undefined, State#state.event_sink),
    State#state{current_tasks = TT#{Ref => Pid}}.

-spec spawn_worker(task_graph_lib:task(), term(), event_mgr()) -> pid().
spawn_worker(#task{task_id = Ref, worker_module = Mod, data = Data}, WorkerState, EventMgr) ->
    Parent = self(),
    spawn_link(
      fun() ->
          event({spawn_task, Ref, self()}, EventMgr),
          try
              case Mod:run_task(WorkerState, Ref, Data) of
                  {ok, Result} ->
                      complete_task(Parent, Ref, true, Result);
                  {ok, Result, NewTasks} ->
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
