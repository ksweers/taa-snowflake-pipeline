USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_USERINFO(
    STAGE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                client_id, table_id, filename, full_file_path, last_modified
            FROM STAGE_TAA_DELTA_MANIFEST
            WHERE table_id = ''c930ce7d-904e-31a5-156d-559bc63e4246''
            ORDER BY last_modified ASC
        `;
        var file_results  = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list     = [];
        var file_metadata = {};

        while (file_results.next()) {
            var relative_path  = file_results.getColumnValue(1);
            var client_id      = file_results.getColumnValue(2);
            var table_id       = file_results.getColumnValue(3);
            var filename       = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);

            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id:      client_id,
                table_id:       table_id,
                filename:       filename,
                full_file_path: full_file_path
            };
        }
        if (file_list.length === 0) { return "No delta files found for USERINFO."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_USERINFO;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        //         Note: sparse column mapping � only $5, $6, $44, $52 are extracted.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_USERINFO (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    USERID, EMPIDENTIFIER, MODIFIEDON, STARTDATE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::TEXT,
                        TRY_TO_TIMESTAMP_NTZ($44),
                        TRY_TO_TIMESTAMP_NTZ($52)
                    FROM @` + STAGE_NAME + ` (FILE_FORMAT => ''FF_TAA_ONELAKE_CSV'')
                )
                FILES = (` + files_clause + `)
                ON_ERROR = ABORT_STATEMENT FORCE = TRUE
            `;
            var copy_result = snowflake.createStatement({sqlText: copy_sql}).execute();

            while (copy_result.next()) {
                var file_name   = copy_result.getColumnValue(1);
                var status      = copy_result.getColumnValue(2);
                var rows_loaded = copy_result.getColumnValue(4);
                var first_error = copy_result.getColumnValue(7);

                var rel_path    = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/")) : file_name;
                var meta        = file_metadata[rel_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";

                var safe_filename  = (meta.filename       || file_name).replace(/''/g, "''''");
                var safe_full_path = (meta.full_file_path || file_name).replace(/''/g, "''''");
                var safe_client_id = (meta.client_id      || "UNKNOWN");
                var safe_table_id  = (meta.table_id       || "UNKNOWN");
                var safe_error     = first_error ? first_error.replace(/''/g, "''''") : null;

                snowflake.createStatement({ sqlText: `
                    INSERT INTO INGEST_TAA_FILE_AUDIT
                        (file_name, client_id, table_id, rows_loaded, batch_number,
                         load_status, error_message, full_stage_path)
                    VALUES (
                        ''` + safe_filename  + `'', ''` + safe_client_id + `'',
                        ''` + safe_table_id  + `'', ` + rows_loaded + `,
                        `  + (batch_num + 1) + `, ''` + load_status + `'',
                        `  + (safe_error ? "''" + safe_error + "''" : "NULL") + `,
                        ''` + safe_full_path + `''
                    )
                `}).execute();

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO USERINFO tgt
            USING (
                SELECT * FROM STG_DELTA_USERINFO WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, USERID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.USERID               = src.USERID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.EMPIDENTIFIER = src.EMPIDENTIFIER,
                tgt.MODIFIEDON    = src.MODIFIEDON,
                tgt.STARTDATE     = src.STARTDATE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, USERID, EMPIDENTIFIER, MODIFIEDON, STARTDATE
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.USERID, src.EMPIDENTIFIER,
                src.MODIFIEDON, src.STARTDATE
            )
        `;
        var merge_result = snowflake.createStatement({sqlText: merge_sql}).execute();
        merge_result.next();
        total_inserts = merge_result.getColumnValue(1);
        total_updates = merge_result.getColumnValue(2);
        total_deletes = merge_result.getColumnValue(3);

        return "Delta applied: " + files_processed + " file(s) across " +
               batch_count + " batch(es) -- " +
               "Rows copied to staging: " + total_rows_copied + ", " +
               "Inserts: " + total_inserts + ", Updates: " + total_updates +
               ", Deletes: " + total_deletes + ".";
    } catch (err) {
        throw new Error("DELTA_LOAD_USERINFO failed: " + err.message);
    }
';


-- =============================================================================
-- V3 ADDITIONS: Task DAG for parallel delta table loading
-- =============================================================================
-- The objects below add Task DAG support on top of the pipeline above.
-- All DELTA_LOAD_* procedures defined above are called by DELTA_LOAD_FROM_CONFIG.
--
-- Task DAG structure (mirrors full load wave pattern):
--   TAA_DL_ROOT (nightly CRON 02:00 UTC; also triggered via INGEST_TAA_LAUNCH_DELTA_LOAD)
--     -> Wave 1 (8 parallel): CUSTOMER, ENTERPRISECUSTOMER, PAYTYPE, SCHEDULE,
--                             USERINFO, TIMEOFFDATA, TIMEOFFREQUEST, TIMESLICEPOST
--     -> Wave 2 (4 serial, each after its FK parent):
--          USERINFO -> USERINFOISSALARY
--          TIMEOFFREQUEST -> TIMEOFFREQUESTDETAIL
--          TIMESLICEPOST -> TIMESLICEPOSTEXCEPTIONDETAIL
--          TIMESLICEPOST -> TIMESLICEPOSTSHIFTDIFFDETAIL
--     -> TAA_DL_FINALIZE (after all 9 leaf tasks)
-- =============================================================================


-- =============================================================================
-- CONFIG TABLE
-- Stores the current run's parameters so Tasks can read them at runtime.
-- STAGE_NAME persists between runs -- set it once via INGEST_TAA_LAUNCH_DELTA_LOAD
-- and all subsequent scheduled runs will use the same value automatically.
-- =============================================================================
