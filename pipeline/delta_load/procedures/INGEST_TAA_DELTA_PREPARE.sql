USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_DELTA_PREPARE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var result_message = "";

    try {
        var cfg_result = snowflake.createStatement({sqlText: `
            SELECT PARAM_NAME, PARAM_VALUE
            FROM INGEST_TAA_DELTA_RUN_CONFIG
            WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')
        `}).execute();

        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }

        var stage_name_safe   = config["STAGE_NAME"]        || null;
        var client_filter     = config["CLIENT_ID_FILTER"]  || null;

        if (!stage_name_safe) {
            // STAGE_NAME not yet configured -- silent skip so scheduled runs
            // before first setup do not show as FAILED.
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_DELTA_RUN_CONFIG. " +
                   "Call INGEST_TAA_LAUNCH_DELTA_LOAD() to configure before the next scheduled run.";
        }

        var is_client_scoped = (client_filter !== null && client_filter.trim() !== "");

        result_message += "=== INGEST_TAA_DELTA_PREPARE ===\\n";
        result_message += "Client scope : " + (is_client_scoped ? client_filter : "ALL CLIENTS") + "\\n";
        result_message += "Stage        : " + stage_name_safe + "\\n";

        result_message += "\\n=== BUILDING DELTA MANIFEST ===\\n";
        var file_list_param = is_client_scoped ? "''" + client_filter + "''" : "NULL";
        var manifest_sql = "CALL BUILD_STAGE_TAA_DELTA_MANIFEST(" +
                           file_list_param + ", ''" + stage_name_safe + "'');";
        var manifest_result = snowflake.createStatement({sqlText: manifest_sql}).execute();
        manifest_result.next();
        result_message += "  " + manifest_result.getColumnValue(1) + "\\n";
        result_message += "\\nPREPARE COMPLETE -- Wave 1 tasks will now start.\\n";

        return result_message;

    } catch (err) {
        throw new Error("INGEST_TAA_DELTA_PREPARE failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: INGEST_TAA_DELTA_FINALIZE
-- =============================================================================
-- No-argument payload for TAA_DELTA_FINALIZE.
-- Runs after all leaf tasks complete. Currently returns a summary; extend
-- here for any post-load state updates or notifications.
-- =============================================================================
