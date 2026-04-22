USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_TIMEOFFREQUEST(
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
            WHERE table_id = ''db78652a-b192-ed5c-b7fd-410e8e8eb47a''
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
        if (file_list.length === 0) { return "No delta files found for TIMEOFFREQUEST."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMEOFFREQUEST;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMEOFFREQUEST (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME, TIMEOFFREQUESTID, USERID, PAYTYPEID,
                    TIMEOFFPOLICYDETAILID, DATETIMESUBMITTED, STARTDATETIME, ENDDATETIME,
                    INCLUDEWEEKENDS, DURATIONPERDAYSECS, STATUSTYPE, STATUSCHANGEDON,
                    EMPNOTES, ISBUYOUTREQUEST, BUYOUTSECS, BUYOUTADJUSTMENTID, PAYADJUSTMENTDATAID
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::NUMBER(38,0),
                        $8::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($9), TRY_TO_TIMESTAMP_NTZ($10), TRY_TO_TIMESTAMP_NTZ($11),
                        $12::BOOLEAN, $13::NUMBER(38,0), $14::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($15),
                        $16::TEXT, $17::BOOLEAN, $18::NUMBER(38,0), $19::NUMBER(38,0), $20::NUMBER(38,0)
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
            MERGE INTO TIMEOFFREQUEST tgt
            USING (
                SELECT * FROM STG_DELTA_TIMEOFFREQUEST WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMEOFFREQUESTID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.TIMEOFFREQUESTID     = src.TIMEOFFREQUESTID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID                = src.USERID,
                tgt.PAYTYPEID             = src.PAYTYPEID,
                tgt.TIMEOFFPOLICYDETAILID = src.TIMEOFFPOLICYDETAILID,
                tgt.DATETIMESUBMITTED     = src.DATETIMESUBMITTED,
                tgt.STARTDATETIME         = src.STARTDATETIME,
                tgt.ENDDATETIME           = src.ENDDATETIME,
                tgt.INCLUDEWEEKENDS       = src.INCLUDEWEEKENDS,
                tgt.DURATIONPERDAYSECS    = src.DURATIONPERDAYSECS,
                tgt.STATUSTYPE            = src.STATUSTYPE,
                tgt.STATUSCHANGEDON       = src.STATUSCHANGEDON,
                tgt.EMPNOTES              = src.EMPNOTES,
                tgt.ISBUYOUTREQUEST       = src.ISBUYOUTREQUEST,
                tgt.BUYOUTSECS            = src.BUYOUTSECS,
                tgt.BUYOUTADJUSTMENTID    = src.BUYOUTADJUSTMENTID,
                tgt.PAYADJUSTMENTDATAID   = src.PAYADJUSTMENTDATAID
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMEOFFREQUESTID, USERID, PAYTYPEID,
                TIMEOFFPOLICYDETAILID, DATETIMESUBMITTED, STARTDATETIME, ENDDATETIME,
                INCLUDEWEEKENDS, DURATIONPERDAYSECS, STATUSTYPE, STATUSCHANGEDON,
                EMPNOTES, ISBUYOUTREQUEST, BUYOUTSECS, BUYOUTADJUSTMENTID, PAYADJUSTMENTDATAID
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMEOFFREQUESTID, src.USERID, src.PAYTYPEID,
                src.TIMEOFFPOLICYDETAILID, src.DATETIMESUBMITTED, src.STARTDATETIME, src.ENDDATETIME,
                src.INCLUDEWEEKENDS, src.DURATIONPERDAYSECS, src.STATUSTYPE, src.STATUSCHANGEDON,
                src.EMPNOTES, src.ISBUYOUTREQUEST, src.BUYOUTSECS, src.BUYOUTADJUSTMENTID,
                src.PAYADJUSTMENTDATAID
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
        throw new Error("DELTA_LOAD_TIMEOFFREQUEST failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_TIMESLICEPOST
-- PK: DATABASEPHYSICALNAME + TIMESLICEPOSTID
-- DATABASEPHYSICALNAME derived from path via REGEXP_SUBSTR on METADATA$FILENAME
-- =============================================================================
