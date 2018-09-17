-module(test_worker).
-behavior(task_graph_runner).

-export([run_task/4]).

-include("task_graph_test.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("task_graph/include/task_graph.hrl").

run_task(State, Ref, error, _GetDepsResult) ->
    {error, oh_no_task_failed, Ref};
run_task(State, Ref, exception, _GetDepsResult) ->
    error({oh_no_task_crashed, Ref});
run_task(State, Ref, Opts = #{deps := Deps}, GetDepsResult) ->
    ?assertEqual(true, check_deps(Deps)),
    check_dep_results(Ref, GetDepsResult, Deps),
    Result = check_dynamic_deps(Ref, Opts, GetDepsResult),
    increase_ran_counter(Ref),
    Result;
run_task(State, Ref, #{}, _) ->
    increase_ran_counter(Ref),
    {ok, {}}.

check_deps(Deps) ->
    lists:foldl( fun(Dep, Acc) ->
                         Acc andalso ets:member(?TEST_TABLE, Dep)
                 end
               , true
               , Deps
               ).

check_dynamic_deps(Self, Opts, GetDepResult) ->
    case {Opts, get_ran_counter(Self)} of
        {#{defer := Defer}, 0} ->
            {defer, Defer};
        {#{defer := {Deps, _}}, 1} ->
            DepIds = [I || #task{task_id = I} <- Deps],
            ?assertEqual(true, check_deps(DepIds)),
            check_dep_results(Self, GetDepResult, DepIds),
            {ok, {result, Self}};
        {#{}, 0} ->
            case Opts of
                #{dynamic := Dyn} ->
                    {ok, {result, Self}, Dyn};
                #{} ->
                    {ok, {result, Self}}
            end;
        Err ->
            error({dynamic_deps_mismatch, Self, Err})
    end.

check_dep_results(Self, GetDepResult, Deps) ->
    lists:foreach( fun(I) ->
                           ?assertEqual( {ok, {result, I}}
                                       , GetDepResult(I)
                                       )
                   end
                 , Deps).

increase_ran_counter(Self) ->
    case ets:member(?TEST_TABLE, Self) of
        false ->
            ets:insert( ?TEST_TABLE
                      , #test_table{task_id = Self, ran_times = 1}
                      );
        true ->
            ets:update_counter( ?TEST_TABLE
                              , Self
                              , {#test_table.ran_times, 1}
                              )
    end.

get_ran_counter(Self) ->
    MatchSpec = #test_table{ task_id = Self
                           , ran_times = '$1'
                           },
    case ets:match(?TEST_TABLE, MatchSpec) of
        [] ->
            0;
        [[N]] ->
            N
    end.
