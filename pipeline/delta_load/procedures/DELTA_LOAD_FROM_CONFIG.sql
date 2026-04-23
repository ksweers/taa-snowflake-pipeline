USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_FROM_CONFIG(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var cfg = snowflake.createStatement({
            sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_DELTA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
        }).execute();
        cfg.next();
        var stage = cfg.getColumnValue(1);
        if (!stage) {
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_DELTA_RUN_CONFIG.";
        }
        var call_sql = "CALL DELTA_LOAD_" + TABLE_NAME + "(''" + stage + "'')";
        var result = snowflake.createStatement({sqlText: call_sql}).execute();
        result.next();
        return result.getColumnValue(1);
    } catch (err) {
        throw new Error("DELTA_LOAD_FROM_CONFIG(" + TABLE_NAME + ") failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: INGEST_TAA_DELTA_PREPARE
-- =============================================================================
-- No-argument payload for TAA_DELTA_ROOT.
-- Reads run parameters from INGEST_TAA_DELTA_RUN_CONFIG then calls
-- BUILD_STAGE_TAA_DELTA_MANIFEST to populate STAGE_TAA_DELTA_MANIFEST.
-- Wave 1 tasks start immediately after this succeeds.
-- =============================================================================
