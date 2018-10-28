-module(task_graph_server).

-behaviour(gen_server).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        , complete_task/5
        , extend_graph/3
        , event/3
        , event/2
        , abort_execution/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% server state
-record(state,
        { tasks_table    :: ets:tid()
        , event_mgr      :: pid()
        , parent         :: pid()
        , n_left         :: non_neg_integer()
        , success = true :: boolean()
        }).

-record(vertex,
        { id
        , pid
        , task
        , done = false
        , result
        }).

-define(FAILED_VERTEX,
        { vertex
        ,  _id    = '$1'
        , _pid    = '_'
        , _task   = '_'
        , _done   = true
        , _result = {error, '$2'}
        }).

-type event_mgr() :: atom()
                   | {atom(), atom()}
                   | {global, term()}
                   | pid()
                   | undefined
                   .

-define(TIMEOUT, infinity).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec run_graph( atom()
               , #{task_graph:settings_key() => term()}
               , task_graph:digraph()
               ) -> {ok, term()} | {error, term()}.
run_graph(_, _, {[], _}) ->
    {ok, #{}};
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

complete_task(Pid, Id, Success, Result, NewTasks) ->
    gen_server:call(Pid, {complete_task, Id, Success, Result, NewTasks}, ?TIMEOUT).

extend_graph(Pid, ParentTask, G) ->
    gen_server:cast(Pid, {extend_graph, ParentTask, G}).

event(Kind, EventMgr) ->
    event(Kind, undefined, EventMgr).

event(Kind, Data, EventMgr) when is_pid(EventMgr) ->
    gen_event:notify( EventMgr
                    , #tg_event{ timestamp = erlang:system_time(?tg_timeUnit)
                               , kind = Kind
                               , data = Data
                               }
                    ).

-spec abort_execution(pid(), term()) -> ok.
abort_execution(Pid, Reason) ->
    gen_server:call(Pid, Reason, ?TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, EventMgr, Settings, Tasks, Parent}) ->
    process_flag(trap_exit, true),
    ResourceLimits = maps:get(resources, Settings, #{}),
    Tab = ets:new(TaskName, [{keypos, #vertex.id}, protected]),
    State = #state{ tasks_table = Tab
                  , parent      = Parent
                  , event_mgr   = EventMgr
                  , n_left      = 0
                  },
    do_extend_graph(Tasks, undefined, State).

handle_call( {complete_task, Id, Success1, Result, NewTasks}
           , From
           , State0 = #state{ tasks_table = Tab
                            , success     = Success0
                            , n_left      = N
                            }
           ) ->
    %% Assert:
    [Vertex0] = ets:lookup(Tab, Id),
    ResultTuple =
        if Success1 ->
                {ok, Result};
           true ->
                {error, Result}
        end,
    ets:insert(Tab, Vertex0#vertex{ done   = true
                                  , result = ResultTuple
                                  }),
    Success = Success0 andalso Success1,
    State1 = State0#state{ n_left = N - 1
                         , success = Success
                         },
    case do_extend_graph(NewTasks, undefined, State1) of
        {ok, State} ->
            case State#state.n_left of
                0 -> %% It was the last task
                    gen_server:reply(From, ok),
                    complete_graph(State);
                _ ->
                    {reply, ok, State}
            end;
        {stop, Err} ->
            {stop, {topology_error, Err}, State0}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({extend_graph, ParentTask, G}, State) ->
    case do_extend_graph(G, ParentTask, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, Err} ->
            {stop, Err, State}
    end;
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ets:delete(State#state.tasks_table),
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
    GetDepResult = fun(Id) ->
                           case ets:lookup(Tab, Id) of
                               [#vertex{done = true, result = R}] ->
                                   {ok, R};
                               _ ->
                                   error
                           end
                   end,
    case do_analyse_graph({Vertices, Edges}, ParentTaskId, Tab) of
        ok ->
            {NNew, Pids} =
                lists:foldl( fun(#tg_task{id = Id}, {N, Acc}) when Id =:= ParentTaskId ->
                                     {N, Acc};
                                (Task, {N, Acc}) ->
                                     {ok, Pid} = task_graph_actor:start_link( EventMgr
                                                                            , Task
                                                                            , GetDepResult
                                                                            ),
                                     ets:insert_new(Tab, #vertex{ id = Task#tg_task.id
                                                                , task = Task
                                                                , pid = Pid
                                                                }),
                                     {N + 1, [Pid|Acc]}
                             end
                           , {0, []}
                           , Vertices
                           ),
            lists:foreach( fun({From, To}) ->
                                   [#vertex{pid = PFrom}] = ets:lookup(Tab, From),
                                   [#vertex{pid = PTo}] = ets:lookup(Tab, To),
                                   task_graph_actor:add_consumer(PFrom, To, PTo),
                                   task_graph_actor:add_requirement(PTo, From, PFrom)
                           end
                         , Edges
                         ),
            lists:foreach( fun(Pid) ->
                                   task_graph_actor:launch(Pid)
                           end
                         , Pids
                         ),
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
        DG = digraph:new(),
        [digraph:add_vertex(DG, Id) || #tg_task{id = Id} <- Vertices],
        [digraph:add_edge(DG, From, To)
         || {From, To} <- Edges
          , map_sets:is_element(From, NewIds)
        ],
        Acyclic = digraph_utils:is_acyclic(DG),
        digraph:delete(DG),
        if Acyclic ->
                ok;
           true ->
                throw({cyclic_dependencies, []})
        end
    catch
        Err ->
            {error, Err}
    end.

complete_graph(State = #state{ parent = Parent
                             , success = Success
                             , tasks_table = Tab
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
    Parent ! {result, self(), ReturnValue},
    {stop, normal, State}.
