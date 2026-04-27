USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

-- =============================================================================
-- DELTA_LOAD_LLDETAIL
-- PK: DATABASEPHYSICALNAME + LLDETAILID
-- DATABASEPHYSICALNAME derived from path via REGEXP_SUBSTR on METADATA$FILENAME
-- CDC CSV positional mapping ($1=LSN hex, $2=skipped, $3=CHANGE_TYPE, $4=skipped):
--   $5  = LLDetailID       (NUMBER)
--   $6  = LLID             (NUMBER)
--   $7  = LLDetailCode     (VARCHAR(300))
--   $8  = LLDetailName     (VARCHAR(300))
--   $9  = StartDate        (TIMESTAMP_NTZ)
--   $10 = EndDate          (TIMESTAMP_NTZ)
--   $11 = ModifiedBy       (NUMBER)
--   $12 = ModifiedOn       (TIMESTAMP_NTZ)
--   $13 = IsDeleted        (BOOLEAN)
--   $14 = EmpNotesRequired (BOOLEAN)
--   $15 = CreatedOn        (TIMESTAMP_NTZ)
--   $16 = CreatedBy        (NUMBER)
--   $17 = PayrollUniqueID  (NUMBER)
--   $18 = OriginalCode     (VARCHAR(300))
--   $19 = CAStartDate      (TIMESTAMP_NTZ)
--   $20 = CAEndDate        (TIMESTAMP_NTZ)
--   $21 = PayrollClientID  (VARCHAR(36))
--   $22 = ColorCode        (VARCHAR(7))
-- =============================================================================

CREATE OR REPLACE PROCEDURE DELTA_LOAD_LLDETAIL(
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
            WHERE table_id = ''46c059a2-1b66-97a0-6dbc-4b1bf1ca4219''
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
        if (file_list.length === 0) { return "No delta files found for LLDETAIL."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_LLDETAIL;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_LLDETAIL (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    LLDETAILID, LLID, LLDETAILCODE, LLDETAILNAME,
                    STARTDATE, ENDDATE, MODIFIEDBY, MODIFIEDON,
                    ISDELETED, EMPNOTESREQUIRED, CREATEDON, CREATEDBY,
                    PAYROLLUNIQUEID, ORIGINALCODE,
                    CASTARTDATE, CAENDDATE,
                    PAYROLLCLIENTID, COLORCODE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::NUMBER(38,0),
                        $7::VARCHAR(300),
                        $8::VARCHAR(300),
                        TRY_TO_TIMESTAMP_NTZ($9),
                        TRY_TO_TIMESTAMP_NTZ($10),
                        $11::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($12),
                        $13::BOOLEAN,
                        $14::BOOLEAN,
                        TRY_TO_TIMESTAMP_NTZ($15),
                        $16::NUMBER(38,0),
                        $17::NUMBER(38,0),
                        $18::VARCHAR(300),
                        TRY_TO_TIMESTAMP_NTZ($19),
                        TRY_TO_TIMESTAMP_NTZ($20),
                        $21::VARCHAR(36),
                        $22::VARCHAR(7)
                    FROM @` + STAGE_NAME + ` (FILE_FORMAT => ''FF_TAA_ONELAKE_CSV'')
                )
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE FORCE = TRUE
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
            MERGE INTO LLDETAIL tgt
            USING (
                SELECT * FROM STG_DELTA_LLDETAIL WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, LLDETAILID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.LLDETAILID           = src.LLDETAILID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.LLID            = src.LLID,
                tgt.LLDETAILCODE    = src.LLDETAILCODE,
                tgt.LLDETAILNAME    = src.LLDETAILNAME,
                tgt.STARTDATE       = src.STARTDATE,
                tgt.ENDDATE         = src.ENDDATE,
                tgt.MODIFIEDBY      = src.MODIFIEDBY,
                tgt.MODIFIEDON      = src.MODIFIEDON,
                tgt.ISDELETED       = src.ISDELETED,
                tgt.EMPNOTESREQUIRED = src.EMPNOTESREQUIRED,
                tgt.CREATEDON       = src.CREATEDON,
                tgt.CREATEDBY       = src.CREATEDBY,
                tgt.PAYROLLUNIQUEID = src.PAYROLLUNIQUEID,
                tgt.ORIGINALCODE    = src.ORIGINALCODE,
                tgt.CASTARTDATE     = src.CASTARTDATE,
                tgt.CAENDDATE       = src.CAENDDATE,
                tgt.PAYROLLCLIENTID = src.PAYROLLCLIENTID,
                tgt.COLORCODE       = src.COLORCODE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, LLDETAILID, LLID, LLDETAILCODE, LLDETAILNAME,
                STARTDATE, ENDDATE, MODIFIEDBY, MODIFIEDON,
                ISDELETED, EMPNOTESREQUIRED, CREATEDON, CREATEDBY,
                PAYROLLUNIQUEID, ORIGINALCODE,
                CASTARTDATE, CAENDDATE,
                PAYROLLCLIENTID, COLORCODE
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.LLDETAILID, src.LLID, src.LLDETAILCODE, src.LLDETAILNAME,
                src.STARTDATE, src.ENDDATE, src.MODIFIEDBY, src.MODIFIEDON,
                src.ISDELETED, src.EMPNOTESREQUIRED, src.CREATEDON, src.CREATEDBY,
                src.PAYROLLUNIQUEID, src.ORIGINALCODE,
                src.CASTARTDATE, src.CAENDDATE,
                src.PAYROLLCLIENTID, src.COLORCODE
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
        throw new Error("DELTA_LOAD_LLDETAIL failed: " + err.message);
    }
';

--
