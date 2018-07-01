%% Listens to task_graph events and formats dependency
%% graph in dot format
-module(task_graph_flow).

-behaviour(gen_event).

-include_lib("task_graph/include/task_graph.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state,
        { fd       :: file:io_device()
        , start_ts :: integer()
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

init(Args) ->
    Filename = maps:get(filename, Args, "task_graph_flow.log"),
    {ok, FD} = file:open(Filename, [write]),
    {ok, #state{ fd = FD
               , start_ts = erlang:monotonic_time()
               }}.

handle_event({spawn_task, Ref, _}, State) ->
    log(spawn, Ref, State),
    {ok, State};
handle_event({Evt, Ref}, State)
  when Evt =:= complete_task; Evt =:= task_failed ->
    log(Evt, Ref, State),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

log(Event, Ref, #state{fd=FD, start_ts=T}) ->
    Dt = erlang:convert_time_unit( erlang:monotonic_time() - T
                                 , native
                                 , millisecond
                                 ),
    io:format(FD, "~10B: ~p ~p~n", [Dt, Event, Ref]).

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, #state{fd=FD}) ->
    ok = file:close(FD).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
