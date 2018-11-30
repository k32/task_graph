-module(task_graph_server).

-behaviour(gen_server).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        , run_graph_async/4
        , complete_task/5
        , extend_graph/3
        , grab_resources/4
        , event/3
        , event/2
        , abort/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% server state
-record(state,
        { settings           :: task_graph:settings()
        , resources          :: task_graph_resource:state()
        , tasks_table        :: ets:tid()
        , event_mgr          :: pid()
        , complete_callback  :: task_graph:complete_callback()
        , n_left             :: non_neg_integer()
        , success = true     :: boolean()
        , aborted = false    :: boolean()
        }).

-record(vertex,
        { id                 :: task_graph:task_id()
        , pid                :: pid()
        , task               :: #tg_task{}
        , done = false       :: boolean()
        , result             :: undefined | {task_graph:result_type(), term()}
        }).

-define(SHUTDOWN_TIMEOUT, 5000).

-define(FAILED_VERTEX,
        { vertex
        ,  _id    = '$1'
        , _pid    = '_'
        , _task   = '_'
        , _done   = true
        , _result = {error, '$2'}
        }).

-define(ACTIVE_VERTEX,
        { vertex
        ,  _id    = '$1'
        , _pid    = '$2'
        , _task   = '_'
        , _done   = false
        , _result = '_'
        }).

-define(TIMEOUT, infinity).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , task_graph:settings()
               , task_graph:digraph()
               ) -> {ok, term()} | {error, term()}.
run_graph(_, _, {[], _}) ->
    {ok, #{}};
run_graph(TaskName, Settings, Tasks) ->
    Ref = make_ref(),
    Parent = self(),
    Fun = fun(Result) ->
                  Parent ! {Ref, Result}
          end,
    ShutdownTimeout = maps:get(shutdown_timeout, Settings, ?SHUTDOWN_TIMEOUT),
    case run_graph_async(TaskName, Settings, Tasks, Fun) of
        {ok, Pid} ->
            MRef = monitor(process, Pid),
            receive
                {Ref, Result} ->
                    %% Make sure the server terminated:
                    receive
                        {'DOWN', MRef, process, Pid, _} ->
                            ok
                    after ShutdownTimeout ->
                            %% Should not happen
                            exit(Pid, kill),
                            error({timeout_waiting_for, Pid})
                    end,
                    Result;
                {'DOWN', Ref, process, Pid, Reason} ->
                    {error, {internal_error, Reason}}
            end;
        Error ->
            Error
    end.

-spec run_graph_async( atom()
                     , task_graph:settings()
                     , task_graph:digraph()
                     , task_graph:complete_callback()
                     ) -> {ok, pid()} | {error, term()}.
run_graph_async(TaskName, Settings, Tasks, CompleteCallback) ->
    gen_server:start( {local, TaskName}
                    , ?MODULE
                    , {TaskName, Settings, Tasks, CompleteCallback}
                    , []
                    ).

-spec complete_task( pid()
                   , task_graph:task_id()
                   , {task_graph:result_type(), term()}
                   , task_graph_resource:resources()
                   , task_graph:digraph() | undefined
                   ) -> ok.
complete_task(Pid, Id, Result, Resources, NewTasks) ->
    From = self(),
    gen_server:cast(Pid, {complete_task, From, Id, Result, Resources, NewTasks}).

-spec extend_graph( pid()
                  , task_graph:task_id()
                  , task_graph:digraph()
                  ) -> ok.
extend_graph(Pid, ParentTask, G) ->
    gen_server:cast(Pid, {extend_graph, ParentTask, G}).

-spec event(atom(), pid()) -> ok.
event(Kind, EventMgr) ->
    event(Kind, undefined, EventMgr).

-spec event(atom(), term(), pid()) -> ok.
event(Kind, Data, EventMgr) when is_pid(EventMgr) ->
    gen_event:notify( EventMgr
                    , #tg_event{ timestamp = erlang:system_time(?tg_timeUnit)
                               , kind = Kind
                               , data = Data
                               }
                    ).

-spec abort(pid(), term()) -> ok.
abort(Pid, Reason) ->
    gen_server:call(Pid, {abort, Reason}, ?TIMEOUT).

-spec grab_resources( pid()
                    , task_graph:task_id()
                    , non_neg_integer()
                    , task_graph_resource:resources()
                    ) -> ok.
grab_resources(Parent, Id, Rank, Resources) ->
    gen_server:cast(Parent, {grab_resources, Id, Rank, Resources}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, Settings, Tasks, CompleteCallback}) ->
    %% process_flag(trap_exit, true),
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
    ResourceLimits = maps:get(resources, Settings, #{}),
    Resources = task_graph_resource:init_state(ResourceLimits),
    Tab = ets:new(TaskName, [{keypos, #vertex.id}, protected]),
    State = #state{ settings          = Settings
                  , tasks_table       = Tab
                  , resources         = Resources
                  , complete_callback = CompleteCallback
                  , event_mgr         = EventMgr
                  , n_left            = 0
                  },
    do_extend_graph(Tasks, undefined, State).

handle_call({abort, Reason}, _From, State0) ->
    State = do_abort_graph(Reason, State0),
    {reply, ok, State}.

%% Handle new errors and `aborted' state:
handle_cast( {complete_task, From, Id, {ErrorType, ErrorMsg}, Resources, _}
           , State0 = #state{ tasks_table = Tab
                            , n_left      = N
                            , settings    = Settings
                            , aborted     = Aborted
                            , resources   = Res0
                            }
           ) when ErrorType =:= error
                ; ErrorType =:= aborted
                ; Aborted   =:= true ->
    %% Assert:
    [Vertex0] = ets:lookup(Tab, Id),
    ets:insert(Tab, Vertex0#vertex{ done   = true
                                  , result = {ErrorType, ErrorMsg}
                                  }),
    Res1 = task_graph_resource:free(Res0, Resources),
    task_graph_actor:rip(From),
    KeepGoing = maps:get(keep_going, Settings, false),
    State = State0#state{ n_left = N - 1
                        , success = false
                        , resources = Res1
                        },
    case {State#state.n_left, KeepGoing} of
        {0, _} ->
            complete_graph(State);
        {_, true} ->
            {noreply, State};
        _ ->
            {noreply, do_abort_graph(task_failed, State)}
    end;
%% Handle successful task completion:
handle_cast( {complete_task, From, Id, {ok, Result}, Resources, NewTasks}
           , State0 = #state{ tasks_table = Tab
                            , n_left      = N
                            , resources   = Res0
                            }
           ) ->
    %% Assert:
    [Vertex0] = ets:lookup(Tab, Id),
    ets:insert(Tab, Vertex0#vertex{ done   = true
                                  , result = {ok, Result}
                                  }),
    Res1 = task_graph_resource:free(Res0, Resources),
    task_graph_actor:rip(From),
    State1 = State0#state{ n_left = N - 1
                         , resources = Res1
                         },
    State2 = try_pop_tasks(State1),
    case do_extend_graph(NewTasks, undefined, State2) of
        {ok, State} ->
            case State#state.n_left of
                0 -> %% It was the last task
                    complete_graph(State);
                _ ->
                    {noreply, State}
            end;
        {stop, Err} ->
            {stop, {topology_error, Err}, State0}
    end;
handle_cast({extend_graph, ParentTask, G}, State) ->
    case do_extend_graph(G, ParentTask, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, Err} ->
            {stop, Err, State}
    end;
handle_cast({grab_resources, Id, _Rank, Resources}, State0) ->
    RState0 = State0#state.resources,
    RState = task_graph_resource:push_task(RState0, Id, Resources),
    State1 = State0#state{resources = RState},
    State = try_pop_tasks(State1),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{ event_mgr = EventMgr
                         , settings = Settings
                         , tasks_table = Tab
                         }) ->
    ets:delete(Tab),
    case Settings of
        #{event_manager := _} ->
            %% Event manager was started by someone else, don't touch it
            ok;
        _ ->
            %% I spawned you, so I will kill you!
            ShutdownTimeout = maps:get(shutdown_timeout, Settings, ?SHUTDOWN_TIMEOUT),
            gen_event:stop(EventMgr, normal, ShutdownTimeout)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_extend_graph(undefined, _, State) ->
    {ok, State};
do_extend_graph( {Vertices0, Edges}
               , ParentTaskId
               , State = #state{ event_mgr   = EventMgr
                               , tasks_table = Tab
                               , n_left      = N0
                               }
               ) ->
    %% FIXME: `ParentTaskId == undefined' will create all kind of mess here...
    event(extend_begin, EventMgr),
    Vertices = lists:usort(Vertices0),
    event(add_tasks, Vertices, EventMgr),
    event(add_dependencies, Edges, EventMgr),
    case do_analyse_graph({Vertices, Edges}, ParentTaskId, Tab) of
        ok ->
            {NNew, Pids} = add_vertices(State, ParentTaskId, Vertices),
            add_edges(Tab, Edges),
            lists:foreach(fun(Pid) -> task_graph_actor:launch(Pid) end, Pids),
            case ParentTaskId of
                undefined ->
                    ok;
                _ ->
                    task_graph_actor:launch(ets:lookup_element(Tab, ParentTaskId, #vertex.pid))
            end,
            event(extend_end, EventMgr),
            {ok, State#state{ n_left = N0 + NNew
                            }};
        {error, Err} ->
            {stop, {topology_error, Err}}
    end.

do_analyse_graph({Vertices, Edges}, ParentTaskId, Tab) ->
    try
        NewIds = map_sets:from_list([Id || #tg_task{id = Id} <- Vertices]),
        IsValidId = fun(Id) ->
                            { map_sets:is_element(Id, NewIds)
                            , ets:member(Tab, Id)
                            }
                    end,
        %% Check for duplicate tasks:
        lists:foreach( fun(T = #tg_task{id = Id}) ->
                               case ets:lookup(Tab, Id) of
                                   [] ->
                                       ok;
                                   [#vertex{task = T}] ->
                                       %% New task definition is the
                                       %% same, I'll allow it
                                       ok;
                                   _ ->
                                       throw({duplicate_task, Id})
                               end
                       end
                     , Vertices
                     ),
        %% Check for invalid dependencies:
        lists:foreach( fun({From, To}) ->
                               {A, B} = IsValidId(From),
                               A orelse B orelse throw({missing_dependency, From}),
                               {C, D} = IsValidId(To),
                               D andalso To =/= ParentTaskId andalso throw({time_paradox, From, To}),
                               C orelse  To =:= ParentTaskId orelse throw({missing_consumer, To})
                       end
                     , Edges
                     ),
        %% Check topology of the new graph:
        is_acyclic({Vertices, Edges}, NewIds) orelse
            throw({circular_dependencies, []}),
        ok
    catch
        Err ->
            {error, Err}
    end.

-spec try_pop_tasks(#state{}) -> #state{}.
try_pop_tasks(State = #state{ resources = Res0
                            , tasks_table = Tab
                            , event_mgr = EventMgr
                            }) ->
    event(shed_begin, EventMgr),
    {Tasks, Res1} = task_graph_resource:pop_alloc(Res0),
    lists:foreach( fun(TId) ->
                           Pid = ets:lookup_element(Tab, TId, #vertex.pid),
                           task_graph_actor:resources_acquired(Pid)
                   end
                 , Tasks
                 ),
    event(shed_end, EventMgr),
    State#state{resources = Res1}.

-spec do_abort_graph(term(), #state{}) -> #state{}.
do_abort_graph(Reason, State = #state{tasks_table = Tab}) ->
    lists:foreach( fun([_Id, Pid]) ->
                           task_graph_actor:abort(Pid, Reason)
                   end
                 , ets:match(Tab, ?ACTIVE_VERTEX)
                 ),
    State#state{aborted = true}.

-spec add_vertices( #state{}
                  , task_graph:task_id()
                  , [#tg_task{}]
                  ) -> {non_neg_integer(), [pid()]}.
add_vertices(State, ParentTaskId, Vertices) ->
    #state{ tasks_table = Tab
          , event_mgr = EventMgr
          , settings = Settings
          , resources = RState
          } = State,
    GetDepResult =
        fun(Id) ->
                case ets:lookup(Tab, Id) of
                    [#vertex{done = true, result = R}] ->
                        {ok, R};
                    _ ->
                        error
                end
        end,
    lists:foldl( fun(#tg_task{id = Id}, {N, Acc}) when Id =:= ParentTaskId ->
                         {N, Acc};
                    (Task0, {N, Acc}) ->
                         Resources =
                             task_graph_resource:to_resources( RState
                                                             , Task0#tg_task.resources
                                                             ),
                         Task = Task0#tg_task{ resources = Resources
                                             },
                         {ok, Pid} = task_graph_actor:start_link( EventMgr
                                                                , Task
                                                                , GetDepResult
                                                                , Settings
                                                                ),
                         ets:insert_new(Tab, #vertex{ id = Task#tg_task.id
                                                    , task = Task
                                                    , pid = Pid
                                                    }),
                         {N + 1, [Pid|Acc]}
                 end
               , {0, []}
               , Vertices
               ).

-spec add_edges(ets:tid(), task_graph:edges()) -> ok.
add_edges(Tab, Edges) ->
    lists:foreach( fun({From, To}) ->
                           [Vtx = #vertex{pid = PFrom}] = ets:lookup(Tab, From),
                           case is_task_complete(Vtx) of
                               false ->
                                   [#vertex{pid = PTo}] = ets:lookup(Tab, To),
                                   task_graph_actor:add_consumer(PFrom, To, PTo),
                                   task_graph_actor:add_requirement(PTo, From, PFrom);
                               true ->
                                   ok
                           end
                   end
                 , Edges
                 ).

-spec is_acyclic(task_graph:digraph(), [task_graph:task_id()]) -> boolean().
is_acyclic({Vertices, Edges}, NewIds) ->
    DG = digraph:new(),
    try
        [digraph:add_vertex(DG, Id) || #tg_task{id = Id} <- Vertices],
        [digraph:add_edge(DG, From, To)
         || {From, To} <- Edges
                        , map_sets:is_element(From, NewIds)
        ],
        digraph_utils:is_acyclic(DG)
    after
        digraph:delete(DG)
    end.

-spec is_task_complete(#vertex{}) -> boolean().
is_task_complete(#vertex{result = R}) ->
    R =/= undefined.

complete_graph(State = #state{ complete_callback = CompleteCallback
                             , success = Success
                             , tasks_table = Tab
                             , event_mgr = EventMgr
                             }
              ) ->
    Errors0 = ets:match(Tab, ?FAILED_VERTEX),
    Errors = maps:from_list([{Id, Err} || [Id, Err] <- Errors0]),
    ReturnValue = case Success of
                      true ->
                          %% Assert:
                          Errors = #{},
                          {ok, #{}};
                      false ->
                          {error, Errors}
                  end,
    event(complete_graph, EventMgr),
    try
        CompleteCallback(ReturnValue)
    catch
        _:Err ->
            error_logger:error_msg("Complete callback failed"
                                   " ~p for task graph ~p~n", [Err, self()])
    end,
    {stop, normal, State}.
