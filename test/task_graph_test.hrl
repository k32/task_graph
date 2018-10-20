%% Counters table "schema"
-record(test_table,
        { id
        , value = 0
        }).

-define(TEST_TABLE, test_table).
