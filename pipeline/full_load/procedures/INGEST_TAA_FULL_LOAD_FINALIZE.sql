USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_FULL_LOAD_FINALIZE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var msg = "";
        var run_start = new Date();

        // ------------------------------------------------------------------
        // Read run config so we can echo it back in the summary header
        // ------------------------------------------------------------------
        var cfg_result = snowflake.createStatement({sqlText:
            "SELECT PARAM_NAME, PARAM_VALUE FROM INGEST_TAA_RUN_CONFIG " +
            "WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();
        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }
        var stage_name    = config["STAGE_NAME"]        || "(unknown)";
        var client_filter = config["CLIENT_ID_FILTER"]  || null;
        var table_filter  = config["TABLE_NAME_FILTER"] || null;

        msg += "=== INGEST FULL LOAD (Task DAG) ===\\n";
        msg += "Client scope : " + (client_filter || "ALL CLIENTS") + "\\n";
        msg += "Table filter : " + (table_filter  || "ALL ACTIVE TABLES") + "\\n";
        msg += "Stage        : " + stage_name + "\\n";

        // ------------------------------------------------------------------
        // Manifest summary
        // ------------------------------------------------------------------
        var mfst = snowflake.createStatement({sqlText:
            "SELECT COUNT(*) AS files, COUNT(DISTINCT client_id) AS clients, " +
            "COUNT(DISTINCT table_id) AS tables " +
            "FROM STAGE_TAA_FULL_FILE_MANIFEST"
        }).execute();
        mfst.next();
        msg += "\\n=== FILE MANIFEST ===\\n";
        msg += "  Total files  : " + mfst.getColumnValue(1) + "\\n";
        msg += "  Clients      : " + mfst.getColumnValue(2) + "\\n";
        msg += "  Tables       : " + mfst.getColumnValue(3) + "\\n";

        // ------------------------------------------------------------------
        // Per-table load summary from INGEST_TAA_FILE_AUDIT
        // Use INGEST_TAA_TABLE_CONFIG for ordered table names; join to audit.
        // We want rows written during this DAG run -- use STAGE_TAA_FULL_FILE_MANIFEST
        // as the scope anchor since it was populated by this run''s PREPARE step.
        // ------------------------------------------------------------------
        var tbl_result = snowflake.createStatement({sqlText: `
            SELECT
                cfg.TABLE_NAME,
                cfg.LOAD_ORDER,
                SUM(COALESCE(aud.ROWS_LOADED, 0))     AS rows_loaded,
                COUNT(aud.LOAD_ID)                     AS files_loaded,
                SUM(CASE WHEN aud.LOAD_STATUS = ''FAILED'' THEN 1 ELSE 0 END) AS failed_files
            FROM INGEST_TAA_TABLE_CONFIG cfg
            LEFT JOIN INGEST_TAA_FILE_AUDIT aud
                ON  UPPER(aud.TABLE_ID) = UPPER(cfg.SOURCE_TABLE_ID)
                AND aud.LOAD_STATUS IN (''SUCCESS'', ''FAILED'')
                -- Anchor to files that were in THIS run''s manifest, not a time window.
                -- This prevents prior-run audit rows from appearing when the manifest
                -- is empty (e.g. all files already loaded and skipped by audit dedup).
                AND EXISTS (
                    SELECT 1
                    FROM STAGE_TAA_FULL_FILE_MANIFEST mfst
                    WHERE mfst.FULL_FILE_PATH = aud.FULL_STAGE_PATH
                )
            WHERE cfg.IS_ACTIVE_FULL_LOAD = TRUE
            GROUP BY cfg.TABLE_NAME, cfg.LOAD_ORDER
            ORDER BY cfg.LOAD_ORDER, cfg.TABLE_NAME
        `}).execute();

        msg += "\\n=== LOADING TABLES ===\\n";
        var tbl_num   = 0;
        var total_rows = 0;
        var total_files = 0;
        var total_failed = 0;
        var table_lines = [];

        // Collect first so we know the total count for the [n/N] prefix
        while (tbl_result.next()) {
            table_lines.push({
                name:         tbl_result.getColumnValue(1),
                rows_loaded:  tbl_result.getColumnValue(3),
                files_loaded: tbl_result.getColumnValue(4),
                failed_files: tbl_result.getColumnValue(5)
            });
        }

        for (var i = 0; i < table_lines.length; i++) {
            var t = table_lines[i];
            var status_suffix = t.failed_files > 0 ? " (" + t.failed_files + " file(s) FAILED)" : "";
            msg += "  [" + (i + 1) + "/" + table_lines.length + "] " + t.name + "\\n";
            msg += "      Files loaded: " + t.files_loaded +
                   "  Rows: " + t.rows_loaded + status_suffix + "\\n";
            total_rows  += t.rows_loaded;
            total_files += t.files_loaded;
            total_failed += t.failed_files;
        }

        // ------------------------------------------------------------------
        // Update INGEST_TAA_FULL_LOAD_STATE
        // ------------------------------------------------------------------
        var state_result = snowflake.createStatement({sqlText: `
            MERGE INTO INGEST_TAA_FULL_LOAD_STATE tgt
            USING (
                SELECT
                    client_id,
                    table_id,
                    MAX(tabledata_folder) AS tabledata_folder,
                    MAX(filename)         AS full_load_file,
                    MAX(last_modified)    AS full_load_last_modified
                FROM STAGE_TAA_FULL_FILE_MANIFEST
                GROUP BY client_id, table_id
            ) src
            ON  tgt.client_id = src.client_id
            AND tgt.table_id  = src.table_id
            WHEN MATCHED THEN UPDATE SET
                tgt.tabledata_folder        = src.tabledata_folder,
                tgt.full_load_file          = src.full_load_file,
                tgt.full_load_last_modified = src.full_load_last_modified,
                tgt.state_updated_at        = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                client_id, table_id, tabledata_folder,
                full_load_file, full_load_last_modified, state_updated_at
            ) VALUES (
                src.client_id, src.table_id, src.tabledata_folder,
                src.full_load_file, src.full_load_last_modified, CURRENT_TIMESTAMP()
            )
        `}).execute();
        state_result.next();
        var rows_upserted = state_result.getColumnValue(1) + state_result.getColumnValue(2);

        msg += "\\n=== UPDATING FULL LOAD STATE ===\\n";
        msg += "  State rows upserted: " + rows_upserted + "\\n";

        // ------------------------------------------------------------------
        // Suspend root task
        // ------------------------------------------------------------------
        snowflake.createStatement({sqlText: "ALTER TASK TAA_FULL_ROOT SUSPEND;"}).execute();

        var end_time     = new Date();
        var duration_sec = ((end_time - run_start) / 1000).toFixed(2);

        msg += "\\n=== SUMMARY ===\\n";
        msg += "  Tables        : " + table_lines.length + "\\n";
        msg += "  Total files   : " + total_files + "\\n";
        msg += "  Total rows    : " + total_rows + "\\n";
        if (total_failed > 0) {
            msg += "  Failed files  : " + total_failed + " -- check INGEST_TAA_FILE_AUDIT\\n";
        }
        msg += "  Finalize time : " + duration_sec + " seconds\\n";
        msg += "  TAA_FULL_ROOT   : SUSPENDED\\n";
        msg += "\\nFULL LOAD DAG RUN COMPLETE";

        // Reset filter parameters now that the run is fully complete so the
        // next nightly scheduled run always starts clean.
        snowflake.createStatement({sqlText:
            "UPDATE INGEST_TAA_RUN_CONFIG " +
            "SET PARAM_VALUE = NULL, UPDATED_AT = CURRENT_TIMESTAMP() " +
            "WHERE PARAM_NAME IN (''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();

        return msg;
    } catch (err) {
        throw new Error("INGEST_TAA_FULL_LOAD_FINALIZE failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: INGEST_TAA_LAUNCH_FULL_LOAD  (user-facing entry point for Task DAG)
-- =============================================================================
-- Replaces: CALL INGEST_TAA_FULL_LOAD(CLIENT_ID_FILTER, TABLE_NAME_FILTER, STAGE_NAME)
--
-- 1. Validates STAGE_NAME.
-- 2. Writes all three run parameters into INGEST_TAA_RUN_CONFIG.
-- 3. Triggers the Task DAG via EXECUTE TASK TAA_FULL_ROOT.
-- 4. Returns immediately (non-blocking). Use TASK_HISTORY to monitor.
-- =============================================================================
