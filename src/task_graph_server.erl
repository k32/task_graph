-module(task_graph_server).

-behaviour(gen_server).

-include("task_graph_int.hrl").

%% API
-export([ run_graph/3
        , complete_task/4
        , extend_graph/3
        , event/3
        , event/2
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

complete_task(Pid, Id, Success, Result) ->
    gen_server:call(Pid, {complete_task, Id, Success, Result}).

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({TaskName, EventMgr, Settings, Tasks, Parent}) ->
    ResourceLimits = maps:get(resources, Settings, #{}),
    Tab = ets:new(TaskName, [{keypos, #vertex.id}, protected]),
    State = #state{ tasks_table = Tab
                  , parent      = Parent
                  , event_mgr   = EventMgr
                  , n_left      = 0
                  },
    do_extend_graph(Tasks, undefined, State).

handle_call( {complete_task, Id, Success1, Result}
           , From
           , State = #state{ n_left      = N
                           , tasks_table = Tab
                           , success     = Success0
                           , parent      = Parent
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
    case N of
        1 -> %% It was a last task
            ReturnValue = case Success of
                              true ->
                                  {ok, #{}};
                              false ->
                                  {error, #{}} % FIXME
                          end,
            Parent ! {result, self(), ReturnValue},
            gen_server:reply(From, ok),
            {stop, normal, State};
        _ when N > 1 ->
            {reply, ok, State#state{ n_left = N - 1
                                   , success = Success
                                   }}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({extend_graph, ParentTask, G}, State) ->
    case do_extend_graph(G, ParentTask, State) of
        {ok, State1} ->
            {noreply, State1};
        Err ->
            Err
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
            Pids =
                lists:map( fun(#tg_task{id = Id}) when Id == ParentTaskId ->
                                   ets:lookup_element(Tab, ParentTaskId, #vertex.pid);
                              (Task) ->
                                   {ok, Pid} = task_graph_actor:start_link( EventMgr
                                                                          , Task
                                                                          , GetDepResult
                                                                          ),
                                   ets:insert_new(Tab, #vertex{ id = Task#tg_task.id
                                                              , task = Task
                                                              , pid = Pid
                                                              }),
                                   Pid
                           end
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
            event(extend_end, EventMgr),
            case ParentTaskId of
                undefined ->
                    N = N0 + length(Vertices);
                _ ->
                    N = N0 + length(Vertices) - 1
            end,
            {ok, State#state{ n_left = N
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
                               C orelse throw({missing_consumer, To})
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
