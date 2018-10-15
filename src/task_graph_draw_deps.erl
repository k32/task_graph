%% Listens to task_graph events and formats dependency
%% graph in dot format
-module(task_graph_draw_deps).

-behaviour(gen_event).

-include("task_graph_int.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state,
        { fd        :: file:io_device()
        , preamble  :: string()
        , style_fun :: fun()
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

init(Args) ->
    Filename = maps:get(filename, Args, "task_graph.dot"),
    Preamble = maps:get(preamble, Args, ""),
    StyleFun = maps:get(style, Args, fun(_) -> "" end),
    {ok, FD} = file:open(Filename, [write]),
    io:format(FD, "digraph task_graph {~n~s~n", [Preamble]),
    {ok, #state{ fd = FD
               , style_fun = StyleFun
               , preamble  = Preamble
               }}.

handle_event(#tg_event{kind = add_tasks, data = Tasks}, #state{fd=FD} = State) ->
    #state{style_fun = StyleFun} = State,
    [io:format( FD
              , "  ~p[~s];~n"
              , [Ref, StyleFun(Task)]
              )
     || Task = #task{task_id = Ref} <- Tasks],
    {ok, State};
handle_event(#tg_event{kind = add_dependencies, data = Deps}, #state{fd=FD} = State) ->
    [io:format(FD, "  ~p -> ~p;~n"
              , [From, To]
              )
     || {From, To} <- Deps],
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, #state{fd=FD}) ->
    io:format(FD, "}~n", []),
    ok = file:close(FD).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
