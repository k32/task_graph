%% Listens to task_graph events and formats dependency
%% graph in dot format
-module(task_graph_draw_deps).

-behaviour(gen_event).

-include_lib("task_graph/include/task_graph.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state,
        { fd       :: file:io_device()
        , preamble :: string()
        , color_fun :: fun()
        , shape_fun :: fun()
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
    Color    = maps:get(color, Args, fun(_) -> black end),
    Shape    = maps:get(shape, Args, fun(_) -> oval end),
    {ok, FD} = file:open(Filename, [write]),
    io:format(FD, "digraph task_graph {~n~s~n", [Preamble]),
    {ok, #state{ fd = FD
               , color_fun = Color
               , shape_fun = Shape
               , preamble  = Preamble
               }}.

handle_event({add_tasks, Tasks}, #state{fd=FD} = State) ->
    #state{shape_fun = Shape, color_fun = Color} = State,
    [io:format(FD, "  ~p[color=~p shape=~p];~n"
              , [Ref, Color(Exec), Shape(Exec)]
              )
     || #task{task_id = Ref, execute = Exec} <- Tasks],
    {ok, State};
handle_event({add_dependencies, Deps}, #state{fd=FD} = State) ->
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
