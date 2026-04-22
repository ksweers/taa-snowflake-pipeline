USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_USERINFOISSALARY(
    STAGE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts    = 0;
    var total_updates    = 0;
    var total_deletes    = 0;
    var files_processed  = 0;
    var total_rows_copied = 0;

    try {
        // ----------------------------------------------------------------
        // Step 1: Collect ALL delta files for this table from the manifest.
        //         Build a file_list array (for the FILES clause) and a
        //         file_metadata map (keyed by relative path) for audit.
        //         Files are ordered oldest-first; LSN dedup in the MERGE
        //         guarantees correct last-value-wins regardless of order.
        // ----------------------------------------------------------------
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                client_id,
                table_id,
                filename,
                full_file_path,
                last_modified
            FROM STAGE_TAA_DELTA_MANIFEST
            WHERE table_id = ''ab18c18c-ccff-62b6-4975-156ffc566ef8''
            ORDER BY last_modified ASC
        `;

        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list    = [];
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

        if (file_list.length === 0) {
            return "No delta files found for USERINFOISSALARY.";
        }

        // ----------------------------------------------------------------
        // Step 2: TRUNCATE staging ONCE -- all batches will append into it.
        // ----------------------------------------------------------------
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_USERINFOISSALARY;"}).execute();

        // ----------------------------------------------------------------
        // Step 3: COPY all files into staging in batches of 1000.
        //         (Snowflake FILES clause hard limit is 1000 per COPY INTO.)
        //         Each batch appends to the staging table -- no truncate
        //         between batches.  Per-file audit rows are written here
        //         using the row counts returned by each COPY result set.
        // ----------------------------------------------------------------
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var batch_files  = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_USERINFOISSALARY (
                    CHANGE_TYPE,
                    LSN,
                    DATABASEPHYSICALNAME,
                    USERINFOISSALARYID,
                    USERID,
                    ISSALARY,
                    STARTDATETIME,
                    ENDDATETIME,
                    MODIFIEDBY,
                    MODIFIEDON
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::NUMBER(38,0),
                        $7::BOOLEAN,
                        TRY_TO_TIMESTAMP_NTZ($8),
                        TRY_TO_TIMESTAMP_NTZ($9),
                        $10::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($11)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_CSV'')
                )
                FILES = (` + files_clause + `)
                ON_ERROR = ABORT_STATEMENT
                FORCE = TRUE
            `;
            var copy_result = snowflake.createStatement({sqlText: copy_sql}).execute();

            // Process COPY result set: one row per file.
            // Write a per-file audit record using the exact rows_loaded count
            // reported by Snowflake for that file.
            while (copy_result.next()) {
                var file_name   = copy_result.getColumnValue(1);  // file name
                var status      = copy_result.getColumnValue(2);  // status
                var rows_loaded = copy_result.getColumnValue(4);  // rows_loaded
                var first_error = copy_result.getColumnValue(7);  // first_error

                // Normalise to the relative path used as the metadata map key
                var rel_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;

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
                        ''` + safe_filename  + `'',
                        ''` + safe_client_id + `'',
                        ''` + safe_table_id  + `'',
                        `  + rows_loaded     + `,
                        `  + (batch_num + 1) + `,
                        ''` + load_status    + `'',
                        `  + (safe_error ? "''" + safe_error + "''" : "NULL") + `,
                        ''` + safe_full_path + `''
                    )
                `}).execute();

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // ----------------------------------------------------------------
        // Step 4: ONE MERGE across all staged rows from every batch.
        //         The QUALIFY clause deduplicates by PK using LSN descending,
        //         so the highest LSN (latest change) wins -- identical to the
        //         per-file merge behaviour, but executed as a single operation.
        //
        //         change_type 1 = DELETE, 2 = INSERT,
        //         change_type 3 = old-values row (skip), 4 = UPDATE
        // ----------------------------------------------------------------
        var merge_sql = `
            MERGE INTO USERINFOISSALARY tgt
            USING (
                SELECT * FROM STG_DELTA_USERINFOISSALARY
                WHERE CHANGE_TYPE IN (1, 2, 4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, USERINFOISSALARYID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.USERINFOISSALARYID   = src.USERINFOISSALARYID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID        = src.USERID,
                tgt.ISSALARY      = src.ISSALARY,
                tgt.STARTDATETIME = src.STARTDATETIME,
                tgt.ENDDATETIME   = src.ENDDATETIME,
                tgt.MODIFIEDBY    = src.MODIFIEDBY,
                tgt.MODIFIEDON    = src.MODIFIEDON
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2, 4) THEN INSERT (
                DATABASEPHYSICALNAME, USERINFOISSALARYID, USERID, ISSALARY,
                STARTDATETIME, ENDDATETIME, MODIFIEDBY, MODIFIEDON
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.USERINFOISSALARYID, src.USERID, src.ISSALARY,
                src.STARTDATETIME, src.ENDDATETIME, src.MODIFIEDBY, src.MODIFIEDON
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
        throw new Error("DELTA_LOAD_USERINFOISSALARY failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_TIMEOFFREQUESTDETAIL
-- PK: DATABASEPHYSICALNAME + TIMEOFFREQUESTDETAILID
-- =============================================================================
