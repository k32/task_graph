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
               ) -> ok | {error, term()}.
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
            {ok, Result};
        {'DOWN', Ref, process, Pid, Reason} ->
            error_logger:error_msg( "Task graph ~p terminated with reason ~p~n"
                                  , [Reason]
                                  ),
            {error, internal_error}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, EventMgr, PoolSizes, {Nodes, Edges}, Parent}) ->
    Graph = task_graph_lib:new_graph(TaskName),
    Graph1 = lists:foldl( fun(Task, Acc) ->
                                  {ok, G2} = task_graph_lib:add_task(Acc, Task),
                                  G2
                          end
                        , Graph
                        , Nodes
                        ),
    {ok, Graph2} = task_graph_lib:add_dependencies(Graph1, Edges),
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
    event({error, Ref, Return}, State),
    complete_graph({error, Ref, Return}, State);
handle_cast({complete_task, Ref, _Success = true, Return, NewTasks}, State) ->
    #state{ graph = G0
          } = State,
    event({complete_task, Ref}, State),
    case check_new_tasks(false, NewTasks, Ref) of
        true ->
            {ok, G1} = task_graph_lib:complete_task(G0, Ref),
            io:format("OHAYO after ~p~n", [ets:tab2list(G1)]),
            State1 = State#state{ graph = G1
                                , current_tasks =
                                      maps:remove(Ref, State#state.current_tasks)
                                },
            case task_graph_lib:is_empty(G1) of
                true ->
                    complete_graph(ok, State1);
                false ->
                    maybe_pop_tasks(),
                    {noreply, State1}
            end;
        false ->
            event({error, Ref, {topology_error, NewTasks}}, State),
            complete_graph({error, Ref, {topology_error, NewTasks}}, State)
    end;
handle_cast(maybe_pop_tasks, State) ->
    #state{ graph = G
          , current_tasks = CurrentlyRunningTasks
          } = State,
    SaturatedSchedulers = map_sets:new(),
    {ok, Tasks} = task_graph_lib:search_tasks( G
                                             , SaturatedSchedulers
                                             , CurrentlyRunningTasks
                                             ),
    event({scheduled_tasks, Tasks}, State),
    State2 = lists:foldl( fun run_task/2
                        , State
                        , Tasks
                        ),
    {noreply, State2}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
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
    io:format("Event: ~p~nState: ~p~n", [Term, State]),
    case State of
        #state{graph = G} ->
            task_graph_lib:print_graph(G);
        _ ->
            ok
    end,
    todo.
%% event({Type, Task, Data}, State) ->
%%     todo.

complete_graph(_ok, State = #state{parent = Pid}) ->
    event({graph_complete, self()}, State),
    Pid ! {result, self(), ok},
    {stop, normal, State}.
%% complete_graph({error, Task, Data}, State = #state{parent = Pid}) ->
%%     Pid ! {result, self(), {error, Task, Data}},
%%     exit(stop).

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
spawn_worker(Task = #task{task_id = Ref, worker_module = Mod}, WorkerState, EventMgr) ->
    Parent = self(),
    spawn_link(
      fun() ->
          event({spawn_task, Ref, self()}, EventMgr),
          try
              case Mod:run_task(WorkerState, Task) of
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
                               , false
                               , {uncaught_exception, Err, erlang:get_stacktrace()}
                               )
          end
      end).
