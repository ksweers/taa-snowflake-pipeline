USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE BUILD_STAGE_TAA_FULL_FILE_MANIFEST(
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
        // Parse comma-separated client IDs into a SQL IN list: ''id1'', ''id2'', ...
        // client_filter_display is used in return messages; client_id_in_list is used in SQL.
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
            table_where_clause = "AND p.table_id IN (" +
                "SELECT SOURCE_TABLE_ID FROM INGEST_TAA_TABLE_CONFIG " +
                "WHERE UPPER(TABLE_NAME) IN (" + quoted_tables.join(", ") + "))";
        }

        // Always TRUNCATE the manifest before rebuilding it.
        // This guarantees the individual load procedures only see files for the
        // current runs clients. Scope is then controlled by the WHERE clause on
        // the INSERT below -- never by preserving stale rows from prior runs.
        snowflake.createStatement({
            sqlText: "TRUNCATE TABLE STAGE_TAA_FULL_FILE_MANIFEST;"
        }).execute();

        // LIST the full stage every run -- a client-scoped pattern cannot be used
        // here because non-multi-tenant tables (CUSTOMER, ENTERPRISECUSTOMER) have
        // files that are not nested under a client subfolder.
        var list_command = "LIST @" + STAGE_NAME + " PATTERN=''.*FullCopyData.*[.]parquet''";
        snowflake.createStatement({sqlText: list_command}).execute();

        // Optional WHERE clauses to restrict inserts to specified client(s) and/or table(s).
        var client_where_clause = is_client_scoped
            ? "AND p.client_id IN (" + client_id_in_list + ")"
            : "";

        // Parse the listing and insert ALL files from the most-recent TableData_* folder
        // per client/table combination.
        // Large tables produce multiple data-N.parquet files in the same folder with the
        // same timestamp -- the QUALIFY here picks the latest folder, then the outer join
        // keeps every file inside it.
        var insert_command = `
            INSERT INTO STAGE_TAA_FULL_FILE_MANIFEST
                (FULL_FILE_PATH, CLIENT_ID, TABLE_ID, TABLEDATA_FOLDER, FILENAME, LAST_MODIFIED)
            WITH file_list AS (
                SELECT "name" AS full_file_path,
                "last_modified" AS last_modified
                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            ),
            parsed_files AS (
                SELECT
                    full_file_path,
                    SUBSTRING(full_file_path, POSITION(''LandingZone/'' IN full_file_path)) AS relative_path,
                    REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'',    1, 1, ''e'', 1) AS client_id,
                    REGEXP_SUBSTR(full_file_path, ''/Tables/([^/]+)/'',    1, 1, ''e'', 1) AS table_id,
                    REGEXP_SUBSTR(full_file_path, ''/(TableData_[^/]+)/'', 1, 1, ''e'', 1) AS tabledata_folder,
                    SPLIT_PART(full_file_path, ''/'', -1)                                   AS filename,
                    TO_TIMESTAMP_TZ(last_modified, ''DY, DD MON YYYY HH24:MI:SS TZD'')     AS last_modified
                FROM file_list
            ),
            -- Identify the single most-recent TableData_* folder per client/table
            latest_folder AS (
                SELECT client_id, table_id,
                       MAX(tabledata_folder) AS latest_tabledata_folder
                FROM parsed_files
                WHERE client_id IS NOT NULL
                  AND table_id  IS NOT NULL
                  AND tabledata_folder IS NOT NULL
                GROUP BY client_id, table_id
            )
            -- Keep every file that lives inside that latest folder
            -- and has not already been successfully loaded (audit deduplication)
            SELECT p.full_file_path, p.client_id, p.table_id,
                   p.tabledata_folder, p.filename, p.last_modified
            FROM parsed_files p
            INNER JOIN latest_folder lf
                ON  lf.client_id               = p.client_id
                AND lf.table_id                = p.table_id
                AND lf.latest_tabledata_folder = p.tabledata_folder
            -- Only include tables that are configured and active for full load
            INNER JOIN INGEST_TAA_TABLE_CONFIG cfg
                ON  UPPER(cfg.SOURCE_TABLE_ID) = UPPER(p.table_id)
                AND cfg.IS_ACTIVE_FULL_LOAD    = TRUE
            WHERE p.client_id IS NOT NULL
              AND p.table_id  IS NOT NULL
              -- Skip files already successfully loaded in a prior run
              AND NOT EXISTS (
                    SELECT 1
                    FROM INGEST_TAA_FILE_AUDIT aud
                    WHERE aud.full_stage_path = p.full_file_path
                      AND aud.load_status     = ''SUCCESS''
              )
              ` + client_where_clause + `
              ` + table_where_clause + `
            ORDER BY p.client_id, p.table_id, p.filename;
        `;

        var insert_result = snowflake.createStatement({sqlText: insert_command}).execute();
        insert_result.next();
        var files_inserted = insert_result.getColumnValue(1);

        // Total raw files seen by the LIST (before deduplication / client filtering / audit check)
        var total_files_query = "SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))";
        var total_result = snowflake.createStatement({sqlText: total_files_query}).execute();
        total_result.next();
        var total_files = total_result.getColumnValue(1);

        // Files excluded because they already appear in INGEST_TAA_FILE_AUDIT as SUCCESS
        var already_loaded = total_files - files_inserted;

        var scope_msg = is_client_scoped
            ? " (filtered to client(s): " + client_filter_display + ")"
            : " (all clients)";
        if (is_table_scoped) {
            scope_msg += " (tables: " + table_filter_display + ")";
        }

        var skipped_msg = already_loaded > 0
            ? "; " + already_loaded + " file(s) skipped (already loaded per audit)"
            : "";

        return "Processed " + total_files + " stage file(s)" + scope_msg +
               "; inserted " + files_inserted + " file(s) into STAGE_TAA_FULL_FILE_MANIFEST" +
               skipped_msg + ".";

    } catch (err) {
        throw new Error("BUILD_STAGE_TAA_FULL_FILE_MANIFEST failed: " + err.message);
    }
';


--
