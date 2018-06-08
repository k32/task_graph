-module(task_graph_SUITE).
-export([ suite/0
        , all/0
        %% , init_per_testcase/2
        %% , end_per_testcase/2
        ]).

-export([t_topology/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("task_graph/include/task_graph.hrl").

all() ->
    [t_topology].

-define(TIMEOUT, 10).
-define(NUMTESTS, 100).

-define(DAG, [{nat(), nat()}]).

-define(RUN_PROP(PROP),
        begin
            %% Workaround against ct's wonderful features:
            group_leader(whereis(user), self()),
            true = proper:quickcheck( PROP()
                                    , [{numtests, ?NUMTESTS}]
                                    )
        end).

topology() ->
    ?FORALL(L, ?DAG,
            begin
                DAG = make_dag(L),
                {ok, _} = task_graph:run_graph( foo
                                              , undefined
                                              , DAG
                                              )
            end).

t_topology(_Config) ->
    ?RUN_PROP(topology).

make_dag(L) ->
    Edges = [{N, N + M + 1} || {N, M} <- L],
    print_graph(Edges),
    Vertices = [#task{ task_id = I
                     , worker_module = test_worker
                     , data = {}
                     }
                || I <- lists:usort(lists:flatten(lists:map( fun tuple_to_list/1
                                                           , Edges
                                                           )))],
    {Vertices, Edges}.

print_graph(Edges) ->
    io:format("digraph G {~n"),
    lists:foreach( fun({A, B}) ->
                       io:format("  ~p -> ~p;~n", [A, B])
                   end
                 , Edges),
    io:format("}~n").

suite() ->
    [{timetrap,{seconds, ?TIMEOUT}}].
