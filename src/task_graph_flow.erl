%% Listens to task_graph events and formats dependency
%% graph in dot format
-module(task_graph_flow).

-behaviour(gen_event).

-include("task_graph_int.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state,
        { fd       :: file:io_device()
        , start_ts :: erlang:timestamp()
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
               , start_ts = erlang:system_time(?tg_timeUnit)
               }}.

handle_event(#tg_event{timestamp = Ts, kind = spawn_task, data = [Ref, _]}, State) ->
    log(Ts, spawn, Ref, State),
    {ok, State};
handle_event(#tg_event{timestamp = Ts, kind = Evt, data = Ref}, State)
  when Evt =:= complete_task; Evt =:= task_failed ->
    log(Ts, Evt, Ref, State),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

log(Ts, Event, Ref, #state{fd=FD, start_ts=T}) ->
    Dt = erlang:convert_time_unit( Ts - T
                                 , native
                                 , ?tg_timeUnit
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
