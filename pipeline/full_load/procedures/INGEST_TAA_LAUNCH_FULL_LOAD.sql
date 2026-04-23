USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_LAUNCH_FULL_LOAD(
    CLIENT_ID_FILTER  VARCHAR DEFAULT NULL,
    TABLE_NAME_FILTER VARCHAR DEFAULT NULL,
    STAGE_NAME        VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var stage_name_safe = (STAGE_NAME !== null && STAGE_NAME !== undefined &&
                               STAGE_NAME.trim() !== "")
            ? STAGE_NAME.trim() : null;

        if (!stage_name_safe) {
            throw new Error("STAGE_NAME parameter is required. " +
                            "Example: CALL INGEST_TAA_LAUNCH_FULL_LOAD(NULL, NULL, ''demo.FAB_CF_WS_N1_STG'');");
        }

        var client_val = (CLIENT_ID_FILTER !== null && CLIENT_ID_FILTER !== undefined &&
                          CLIENT_ID_FILTER.trim() !== "")
            ? CLIENT_ID_FILTER.trim() : null;
        var table_val  = (TABLE_NAME_FILTER !== null && TABLE_NAME_FILTER !== undefined &&
                          TABLE_NAME_FILTER.trim() !== "")
            ? TABLE_NAME_FILTER.trim() : null;

        snowflake.createStatement({sqlText:
            "MERGE INTO INGEST_TAA_RUN_CONFIG tgt " +
            "USING (SELECT * FROM VALUES " +
            "  (''STAGE_NAME'',        " + (stage_name_safe ? "''" + stage_name_safe + "''" : "NULL") + "), " +
            "  (''CLIENT_ID_FILTER'',  " + (client_val ? "''" + client_val + "''" : "NULL")  + "), " +
            "  (''TABLE_NAME_FILTER'', " + (table_val  ? "''" + table_val  + "''" : "NULL")  + ") " +
            "AS src(PARAM_NAME, PARAM_VALUE)) src ON tgt.PARAM_NAME = src.PARAM_NAME " +
            "WHEN MATCHED THEN UPDATE SET " +
            "  tgt.PARAM_VALUE = src.PARAM_VALUE, " +
            "  tgt.UPDATED_AT  = CURRENT_TIMESTAMP();"
        }).execute();

        // Resume the root (it suspends itself at the end of each run via INGEST_TAA_FULL_LOAD_FINALIZE).
        // RESUME is idempotent -- safe to call even if already resumed.
        snowflake.createStatement({sqlText: "ALTER TASK TAA_FULL_ROOT RESUME;"}).execute();
        snowflake.createStatement({sqlText: "EXECUTE TASK TAA_FULL_ROOT;"}).execute();

        var scope = client_val ? " (client: " + client_val + ")" : " (all clients)";
        return "Task DAG triggered" + scope + ".\\n" +
               "Stage: " + stage_name_safe + "\\n" +
               "\\nMonitor progress:\\n" +
               "  SELECT * FROM TABLE(TASK_DEPENDENTS(''TAA_FULL_ROOT'', TRUE)) ORDER BY SCHEDULED_TIME;\\n" +
               "\\nView history:\\n" +
               "  SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME\\n" +
               "  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => ''TAA_FULL_ROOT'', RESULT_LIMIT => 10))\\n" +
               "  ORDER BY SCHEDULED_TIME DESC;";
    } catch (err) {
        throw new Error("INGEST_TAA_LAUNCH_FULL_LOAD failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: FULL_LOAD_FROM_CONFIG  (internal task dispatcher)
-- =============================================================================
-- Called by each Wave 1 / Wave 2 task.
-- Reads STAGE_NAME from INGEST_TAA_RUN_CONFIG then dispatches to the
-- corresponding FULL_LOAD_<TABLE> procedure.
-- Using a single dispatcher keeps each task body a simple CALL statement,
-- which avoids Snowflake Task scripting-block limitations with virtual
-- warehouses.
-- =============================================================================
