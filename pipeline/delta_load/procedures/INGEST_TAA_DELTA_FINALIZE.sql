USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE INGEST_TAA_DELTA_FINALIZE()
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
            "SELECT PARAM_NAME, PARAM_VALUE FROM INGEST_TAA_DELTA_RUN_CONFIG " +
            "WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();
        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }
        var stage_name    = config["STAGE_NAME"]        || "(unknown)";
        var client_filter = config["CLIENT_ID_FILTER"]  || null;
        var table_filter  = config["TABLE_NAME_FILTER"] || null;

        msg += "=== INGEST DELTA LOAD (Task DAG) ===\\n";
        msg += "Client scope : " + (client_filter || "ALL CLIENTS") + "\\n";
        msg += "Table filter : " + (table_filter  || "ALL ACTIVE TABLES") + "\\n";
        msg += "Stage        : " + stage_name + "\\n";

        // ------------------------------------------------------------------
        // Manifest summary: files processed this run
        // ------------------------------------------------------------------
        var mfst = snowflake.createStatement({sqlText:
            "SELECT COUNT(*) AS files, COUNT(DISTINCT client_id) AS clients, " +
            "COUNT(DISTINCT table_id) AS tables " +
            "FROM STAGE_TAA_DELTA_MANIFEST"
        }).execute();
        mfst.next();
        var manifest_files = mfst.getColumnValue(1);
        msg += "\\n=== DELTA FILE MANIFEST ===\\n";
        msg += "  Total delta files : " + manifest_files + "\\n";
        msg += "  Clients           : " + mfst.getColumnValue(2) + "\\n";
        msg += "  Tables            : " + mfst.getColumnValue(3) + "\\n";

        if (manifest_files === 0) {
            msg += "\\n  No new delta files found -- all tables are up to date.\\n";
            msg += "\\nDELTA LOAD DAG RUN COMPLETE (no-op)";
            return msg;
        }

        // ------------------------------------------------------------------
        // Per-table delta summary from INGEST_TAA_FILE_AUDIT.
        // Anchored to STAGE_TAA_DELTA_MANIFEST (this run''s files) so that
        // prior-run audit rows never bleed into the report.
        // ------------------------------------------------------------------
        var tbl_result = snowflake.createStatement({sqlText: `
            SELECT
                cfg.TABLE_NAME,
                cfg.LOAD_ORDER,
                SUM(CASE WHEN aud.LOAD_STATUS = ''SUCCESS'' THEN aud.ROWS_LOADED ELSE 0 END) AS rows_affected,
                COUNT(CASE WHEN aud.LOAD_STATUS = ''SUCCESS'' THEN 1 END)                     AS files_ok,
                COUNT(CASE WHEN aud.LOAD_STATUS = ''FAILED''  THEN 1 END)                     AS files_failed
            FROM INGEST_TAA_TABLE_CONFIG cfg
            LEFT JOIN INGEST_TAA_FILE_AUDIT aud
                ON  UPPER(aud.TABLE_ID) = UPPER(cfg.SOURCE_TABLE_ID)
                -- Anchor to files that were in THIS run''s manifest, not a time window.
                AND EXISTS (
                    SELECT 1
                    FROM STAGE_TAA_DELTA_MANIFEST mfst
                    WHERE mfst.FULL_FILE_PATH = aud.FULL_STAGE_PATH
                )
            WHERE cfg.IS_ACTIVE_DELTA_LOAD = TRUE
            GROUP BY cfg.TABLE_NAME, cfg.LOAD_ORDER
            ORDER BY cfg.LOAD_ORDER, cfg.TABLE_NAME
        `}).execute();

        msg += "\\n=== APPLYING DELTA CHANGES ===\\n";
        var table_lines  = [];
        while (tbl_result.next()) {
            table_lines.push({
                name:          tbl_result.getColumnValue(1),
                rows_affected: tbl_result.getColumnValue(3),
                files_ok:      tbl_result.getColumnValue(4),
                files_failed:  tbl_result.getColumnValue(5)
            });
        }

        var total_rows   = 0;
        var total_files  = 0;
        var total_failed = 0;

        for (var i = 0; i < table_lines.length; i++) {
            var t = table_lines[i];
            var status_suffix = t.files_failed > 0 ? " (" + t.files_failed + " file(s) FAILED)" : "";
            msg += "  [" + (i + 1) + "/" + table_lines.length + "] " + t.name + "\\n";
            msg += "      Files: " + t.files_ok +
                   "  Rows affected: " + t.rows_affected + status_suffix + "\\n";
            total_rows   += t.rows_affected;
            total_files  += t.files_ok;
            total_failed += t.files_failed;
        }

        var end_time     = new Date();
        var duration_sec = ((end_time - run_start) / 1000).toFixed(2);

        msg += "\\n=== SUMMARY ===\\n";
        msg += "  Tables          : " + table_lines.length + "\\n";
        msg += "  Total files     : " + total_files + "\\n";
        msg += "  Total rows      : " + total_rows + "\\n";
        if (total_failed > 0) {
            msg += "  Failed files    : " + total_failed + " -- check INGEST_TAA_FILE_AUDIT\\n";
        }
        msg += "  Finalize time   : " + duration_sec + " seconds\\n";
        msg += "\\nDELTA LOAD DAG RUN COMPLETE";

        // Reset filter parameters now that the run is fully complete so the
        // next nightly scheduled run always starts clean.
        snowflake.createStatement({sqlText:
            "UPDATE INGEST_TAA_DELTA_RUN_CONFIG " +
            "SET PARAM_VALUE = NULL, UPDATED_AT = CURRENT_TIMESTAMP() " +
            "WHERE PARAM_NAME IN (''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();

        return msg;
    } catch (err) {
        throw new Error("INGEST_TAA_DELTA_FINALIZE failed: " + err.message);
    }
';


-- =============================================================================
-- PROCEDURE: INGEST_TAA_LAUNCH_DELTA_LOAD  (user-facing entry point)
-- =============================================================================
-- On-demand trigger for the delta Task DAG.
-- Also used on first deployment to set STAGE_NAME (which then persists for
-- all subsequent scheduled runs).
--
-- 1. Validates STAGE_NAME.
-- 2. Writes all three run parameters into INGEST_TAA_DELTA_RUN_CONFIG.
--    STAGE_NAME persists -- subsequent scheduled runs use the stored value.
-- 3. Triggers the Task DAG via EXECUTE TASK TAA_DELTA_ROOT.
-- 4. Returns immediately (non-blocking). Use TASK_HISTORY to monitor.
-- =============================================================================
