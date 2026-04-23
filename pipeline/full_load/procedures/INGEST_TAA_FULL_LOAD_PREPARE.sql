USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_FULL_LOAD_PREPARE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var result_message = "";

    try {
        var cfg_result = snowflake.createStatement({sqlText: `
            SELECT PARAM_NAME, PARAM_VALUE
            FROM INGEST_TAA_RUN_CONFIG
            WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')
        `}).execute();

        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }

        var stage_name_safe   = config["STAGE_NAME"]        || null;
        var client_filter     = config["CLIENT_ID_FILTER"]  || null;
        var table_name_filter = config["TABLE_NAME_FILTER"] || null;

        if (!stage_name_safe) {
            // No parameters set -- this is a scheduled auto-run with no pending job.
            // Return silently so the task shows SUCCEEDED rather than FAILED.
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_RUN_CONFIG. " +
                   "Trigger via INGEST_TAA_LAUNCH_FULL_LOAD to run a real load.";
        }

        var is_client_scoped  = (client_filter !== null && client_filter.trim() !== "");
        var client_id_in_list = null;
        if (is_client_scoped) {
            var raw_ids = client_filter.trim().split(",");
            var quoted  = [];
            for (var ci = 0; ci < raw_ids.length; ci++) {
                var cid = raw_ids[ci].trim();
                if (cid.length > 0) { quoted.push("''" + cid + "''"); }
            }
            client_id_in_list = quoted.join(", ");
        }

        var table_filter_map = null;
        if (table_name_filter && table_name_filter.trim() !== "") {
            table_filter_map = {};
            var parts = table_name_filter.toUpperCase().split(",");
            for (var i = 0; i < parts.length; i++) {
                var t = parts[i].trim();
                if (t.length > 0) { table_filter_map[t] = true; }
            }
        }

        result_message += "=== INGEST_TAA_FULL_LOAD_PREPARE ===\\n";
        result_message += "Client scope : " + (is_client_scoped ? client_filter : "ALL CLIENTS") + "\\n";
        result_message += "Stage        : " + stage_name_safe + "\\n";

        result_message += "\\n=== CLEARING TARGET TABLES ===\\n";

        var ctrl = snowflake.createStatement({sqlText: `
            SELECT TABLE_NAME, IS_MULTI_TENANT
            FROM INGEST_TAA_TABLE_CONFIG
            WHERE IS_ACTIVE_FULL_LOAD = TRUE
            ORDER BY LOAD_ORDER, TABLE_NAME
        `}).execute();

        while (ctrl.next()) {
            var tbl_name        = ctrl.getColumnValue(1);
            var is_multi_tenant = ctrl.getColumnValue(2);

            if (table_filter_map !== null &&
                !table_filter_map[tbl_name.toUpperCase()]) { continue; }

            if (is_client_scoped && !is_multi_tenant) {
                result_message += "  SKIPPED (not multi-tenant): " + tbl_name + "\\n";
                continue;
            }

            var clear_sql;
            if (is_client_scoped) {
                clear_sql = "DELETE FROM " + tbl_name +
                            " WHERE DATABASEPHYSICALNAME IN (" + client_id_in_list + ");";
            } else {
                clear_sql = "TRUNCATE TABLE IF EXISTS " + tbl_name + ";";
            }
            snowflake.createStatement({sqlText: clear_sql}).execute();
            result_message += "  Cleared: " + tbl_name + "\\n";
        }

        result_message += "\\n=== BUILDING FILE MANIFEST ===\\n";
        var file_list_param = is_client_scoped ? "''" + client_filter + "''" : "NULL";
        var manifest_sql = "CALL BUILD_STAGE_TAA_FULL_FILE_MANIFEST(" +
                           file_list_param + ", ''" + stage_name_safe + "'');";
        var manifest_result = snowflake.createStatement({sqlText: manifest_sql}).execute();
        manifest_result.next();
        result_message += "  " + manifest_result.getColumnValue(1) + "\\n";
        result_message += "\\nPREPARE COMPLETE -- Wave 1 tasks will now start.\\n";

        return result_message;

    } catch (err) {
        throw new Error("INGEST_TAA_FULL_LOAD_PREPARE failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: INGEST_TAA_FULL_LOAD_FINALIZE
-- =============================================================================
-- No-argument payload for TAA_FULL_FINALIZE.
-- Updates INGEST_TAA_FULL_LOAD_STATE from STAGE_TAA_FULL_FILE_MANIFEST so the
-- delta pipeline knows the correct TableData_* folder and cutoff timestamps.
-- =============================================================================
