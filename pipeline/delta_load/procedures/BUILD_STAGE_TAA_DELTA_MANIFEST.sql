USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE BUILD_STAGE_TAA_DELTA_MANIFEST(
    CLIENT_ID_FILTER    VARCHAR,
    TABLE_NAME_FILTER   VARCHAR,
    STAGE_NAME          VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var is_client_scoped = (
            CLIENT_ID_FILTER !== null &&
            CLIENT_ID_FILTER !== undefined &&
            CLIENT_ID_FILTER.trim() !== ""
        );

        var client_id_in_list = null;
        var client_filter_display = null;
        if (is_client_scoped) {
            var raw_ids = CLIENT_ID_FILTER.trim().split(",");
            var quoted_ids = [];
            client_filter_display = "";
            for (var ci = 0; ci < raw_ids.length; ci++) {
                var cid = raw_ids[ci].trim();
                if (cid.length > 0) {
                    quoted_ids.push("''" + cid + "''");
                    client_filter_display += (client_filter_display.length > 0 ? ", " : "") + cid;
                }
            }
            client_id_in_list = quoted_ids.join(", ");
        }

        // Parse comma-separated table names into a subquery against INGEST_TAA_TABLE_CONFIG.
        // The manifest stores table_id (UUID); we resolve names → UUIDs via the config table.
        var is_table_scoped = (
            TABLE_NAME_FILTER !== null &&
            TABLE_NAME_FILTER !== undefined &&
            TABLE_NAME_FILTER.trim() !== ""
        );
        var table_where_clause = "";
        var table_filter_display = null;
        if (is_table_scoped) {
            var tbl_names = TABLE_NAME_FILTER.trim().split(",");
            var quoted_tables = [];
            table_filter_display = "";
            for (var ti = 0; ti < tbl_names.length; ti++) {
                var tn = tbl_names[ti].trim().toUpperCase();
                if (tn.length > 0) {
                    quoted_tables.push("''" + tn + "''");
                    table_filter_display += (table_filter_display.length > 0 ? ", " : "") + tn;
                }
            }
            table_where_clause = "AND parsed.table_id IN (" +
                "SELECT SOURCE_TABLE_ID FROM INGEST_TAA_TABLE_CONFIG " +
                "WHERE UPPER(TABLE_NAME) IN (" + quoted_tables.join(", ") + "))";
        }

        // Always truncate before rebuilding
        snowflake.createStatement({
            sqlText: "TRUNCATE TABLE STAGE_TAA_DELTA_MANIFEST;"
        }).execute();

        // Load the CSV inventory from the static OneLake inventory file.
        // The Fabric notebook overwrites this file in-place on every run.
        // Filter to ChangeData paths only (CSV delta feed files).
        var inv_file = "Inventory/file_inventory/Inventory_CSV.csv";

        snowflake.createStatement({
            sqlText: "CREATE OR REPLACE TEMPORARY TABLE STAGE_TAA_CSV_INV_RAW AS " +
                     "SELECT " +
                     "  REGEXP_SUBSTR($1, ''/LandingZone/.*'') AS full_file_path, " +
                     "  TO_CHAR(TO_TIMESTAMP_NTZ($2, ''YYYY-MM-DD HH24:MI:SS''), " +
                     "          ''DY, DD MON YYYY HH24:MI:SS'') || '' UTC'' AS last_modified " +
                     "FROM @" + STAGE_NAME + "/" + inv_file + " (FILE_FORMAT => ''FF_TAA_INVENTORY_CSV'') " +
                     "WHERE $1 LIKE ''%/ChangeData/%''"
        }).execute();

        var cnt = snowflake.createStatement({sqlText: "SELECT COUNT(*) FROM STAGE_TAA_CSV_INV_RAW"}).execute();
        cnt.next();
        var total_files = cnt.getColumnValue(1);

        // Optional WHERE clauses to restrict inserts to specified client(s) and/or table(s).
        var client_where_clause = is_client_scoped
            ? "AND parsed.client_id IN (" + client_id_in_list + ")"
            : "";

        // Parse inventory results:
        //   Extract client_id: path segment before /Tables/
        //   Extract table_id:  path segment between /Tables/ and /TableData_/
        //   Extract tabledata_folder: the TableData_* segment
        //   Join to INGEST_TAA_FULL_LOAD_STATE to gate on folder + timestamp
        //   Exclude files already in INGEST_TAA_FILE_AUDIT (already applied)
        var insert_sql = `
            INSERT INTO STAGE_TAA_DELTA_MANIFEST
                (full_file_path, client_id, table_id, tabledata_folder, filename, last_modified)
            WITH file_list AS (
                SELECT full_file_path, last_modified FROM STAGE_TAA_CSV_INV_RAW
            ),
            parsed AS (
                SELECT
                    full_file_path,
                    TO_TIMESTAMP_TZ(last_modified, ''DY, DD MON YYYY HH24:MI:SS TZD'') AS last_modified,
                    REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'',       1, 1, ''e'', 1) AS client_id,
                    REGEXP_SUBSTR(full_file_path, ''/Tables/([^/]+)/'',       1, 1, ''e'', 1) AS table_id,
                    REGEXP_SUBSTR(full_file_path, ''/(TableData_[^/]+)/'',    1, 1, ''e'', 1) AS tabledata_folder,
                    SPLIT_PART(full_file_path, ''/'', -1)                                     AS filename
                FROM file_list
            )
            SELECT distinct
                parsed.full_file_path,
                parsed.client_id,
                parsed.table_id,
                parsed.tabledata_folder,
                parsed.filename,
                parsed.last_modified
            FROM parsed
            -- Only eligible if a full load has been run for this client/table
            INNER JOIN INGEST_TAA_FULL_LOAD_STATE state
                ON  state.client_id        = parsed.client_id
                AND state.table_id         = parsed.table_id
            -- Only include tables that are configured and active for delta load
            INNER JOIN INGEST_TAA_TABLE_CONFIG cfg
                ON  UPPER(cfg.SOURCE_TABLE_ID) = UPPER(parsed.table_id)
                AND cfg.IS_ACTIVE_DELTA_LOAD   = TRUE
            -- Must be in the same TableData_* folder as the last full load
            WHERE parsed.tabledata_folder  = state.tabledata_folder
            -- Must be newer than the full load file (not before or equal to it)
              AND parsed.last_modified     > state.full_load_last_modified
            -- Must not already have been applied
              AND NOT EXISTS (
                    SELECT 1
                    FROM INGEST_TAA_FILE_AUDIT aud
                    WHERE aud.full_stage_path = parsed.full_file_path
                      AND aud.load_status     = ''SUCCESS''
              )
              AND parsed.client_id   IS NOT NULL
              AND parsed.table_id    IS NOT NULL
              AND parsed.tabledata_folder IS NOT NULL
              ` + client_where_clause + `
              ` + table_where_clause + `
            ORDER BY parsed.last_modified ASC;
        `;

        var insert_result = snowflake.createStatement({sqlText: insert_sql}).execute();
        insert_result.next();
        var files_inserted = insert_result.getColumnValue(1);

        // Files excluded because they already appear in INGEST_TAA_FILE_AUDIT as SUCCESS
        // (total_files was captured before the INSERT above)
        var already_loaded = total_files - files_inserted;

        var scope_msg = is_client_scoped
            ? " (filtered to client(s): " + client_filter_display + ")"
            : " (all clients)";
        if (is_table_scoped) {
            scope_msg += " (tables: " + table_filter_display + ")";
        }

        return "Processed " + total_files + " CSV inventory file(s)" + scope_msg +
               "; inserted " + files_inserted + " unprocessed delta file(s) into STAGE_TAA_DELTA_MANIFEST" +
               (already_loaded > 0 ? "; " + already_loaded + " file(s) skipped (already loaded per audit)" : "") + ".";

    } catch (err) {
        throw new Error("BUILD_STAGE_TAA_DELTA_MANIFEST failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_USERINFOISSALARY
-- PK: DATABASEPHYSICALNAME + USERINFOISSALARYID
-- CSV columns: [meta1, meta2, change_type, meta4, USERINFOISSALARYID, USERID,
--               ISSALARY, STARTDATETIME, ENDDATETIME, MODIFIEDBY, MODIFIEDON]
-- DATABASEPHYSICALNAME derived from path via REGEXP_SUBSTR on METADATA$FILENAME
-- =============================================================================
