USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_LAUNCH_DELTA_LOAD(
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
            // If no stage passed, check if one is already persisted
            var existing = snowflake.createStatement({
                sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_DELTA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
            }).execute();
            existing.next();
            stage_name_safe = existing.getColumnValue(1) || null;
        }

        if (!stage_name_safe) {
            throw new Error("STAGE_NAME is required on first call. " +
                            "Example: CALL INGEST_TAA_LAUNCH_DELTA_LOAD(NULL, NULL, ''demo.FAB_CF_WS_N1_STG'');");
        }

        var client_val = (CLIENT_ID_FILTER !== null && CLIENT_ID_FILTER !== undefined &&
                          CLIENT_ID_FILTER.trim() !== "")
            ? CLIENT_ID_FILTER.trim() : null;
        var table_val  = (TABLE_NAME_FILTER !== null && TABLE_NAME_FILTER !== undefined &&
                          TABLE_NAME_FILTER.trim() !== "")
            ? TABLE_NAME_FILTER.trim() : null;

        snowflake.createStatement({sqlText:
            "MERGE INTO INGEST_TAA_DELTA_RUN_CONFIG tgt " +
            "USING (SELECT * FROM VALUES " +
            "  (''STAGE_NAME'',        " + (stage_name_safe ? "''" + stage_name_safe + "''" : "NULL") + "), " +
            "  (''CLIENT_ID_FILTER'',  " + (client_val ? "''" + client_val + "''" : "NULL")  + "), " +
            "  (''TABLE_NAME_FILTER'', " + (table_val  ? "''" + table_val  + "''" : "NULL")  + ") " +
            "AS src(PARAM_NAME, PARAM_VALUE)) src ON tgt.PARAM_NAME = src.PARAM_NAME " +
            "WHEN MATCHED THEN UPDATE SET " +
            "  tgt.PARAM_VALUE = src.PARAM_VALUE, " +
            "  tgt.UPDATED_AT  = CURRENT_TIMESTAMP();"
        }).execute();

        snowflake.createStatement({sqlText: "EXECUTE TASK TAA_DELTA_ROOT;"}).execute();

        var scope = client_val ? " (client: " + client_val + ")" : " (all clients)";
        return "Delta Task DAG triggered" + scope + ".\\n" +
               "Stage: " + stage_name_safe + "\\n" +
               "STAGE_NAME persists in INGEST_TAA_DELTA_RUN_CONFIG for future scheduled runs.\\n" +
               "\\nMonitor progress:\\n" +
               "  SELECT * FROM TABLE(TASK_DEPENDENTS(''TAA_DELTA_ROOT'', TRUE)) ORDER BY SCHEDULED_TIME;\\n" +
               "\\nView history:\\n" +
               "  SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME\\n" +
               "  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => ''TAA_DELTA_ROOT'', RESULT_LIMIT => 10))\\n" +
               "  ORDER BY SCHEDULED_TIME DESC;";
    } catch (err) {
        throw new Error("INGEST_TAA_LAUNCH_DELTA_LOAD failed: " + err.message);
    }
';


-- =============================================================================
-- TASK DAG DEFINITIONS
-- =============================================================================
-- Deployment notes:
--   - All tasks are created SUSPENDED by default.
--   - Run the ENABLE section at the bottom of this file after deployment.
--   - To modify the DAG: ALTER TASK TAA_DELTA_ROOT SUSPEND first.
--   - All tasks execute on warehouse WH_DS_AUTOMATION_TST.
--   - TAA_DELTA_ROOT fires nightly at 02:00 UTC. Adjust CRON expression as needed.
--   - For on-demand runs: CALL INGEST_TAA_LAUNCH_DELTA_LOAD(...)
--   - First-time setup: call INGEST_TAA_LAUNCH_DELTA_LOAD with STAGE_NAME to
--     persist the stage; subsequent scheduled runs use the stored value.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- ROOT TASK: build delta manifest
-- Fires nightly at 02:00 UTC. Also triggered on-demand via INGEST_TAA_LAUNCH_DELTA_LOAD.
-- -----------------------------------------------------------------------------
