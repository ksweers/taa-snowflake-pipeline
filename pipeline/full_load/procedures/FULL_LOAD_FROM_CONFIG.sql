USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE FULL_LOAD_FROM_CONFIG(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var cfg = snowflake.createStatement({
            sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
        }).execute();
        cfg.next();
        var stage = cfg.getColumnValue(1);
        if (!stage) {
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_RUN_CONFIG.";
        }
        var call_sql = "CALL FULL_LOAD_" + TABLE_NAME + "(''" + stage + "'')";
        var result = snowflake.createStatement({sqlText: call_sql}).execute();
        result.next();
        return result.getColumnValue(1);
    } catch (err) {
        throw new Error("FULL_LOAD_FROM_CONFIG(" + TABLE_NAME + ") failed: " + err.message);
    }
';

-- =============================================================================
-- TASK DAG DEFINITIONS
-- =============================================================================
-- Deployment notes:
--   - All tasks are created SUSPENDED by default.
--   - Run the ENABLE section at the bottom of this file after deployment.
--   - To modify the DAG: ALTER TASK TAA_FULL_ROOT SUSPEND first.
--   - All tasks execute on warehouse WH_DS_AUTOMATION_TST.
--   - TAA_FULL_ROOT has SCHEDULE = '11520 MINUTE' (8 days). Snowflake requires
--     a SCHEDULE on root tasks before they can be RESUMED. The schedule is
--     intentionally long -- the task should only ever run via EXECUTE TASK
--     triggered by INGEST_TAA_LAUNCH_FULL_LOAD.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- ROOT TASK: clear target tables + build file manifest
-- -----------------------------------------------------------------------------
