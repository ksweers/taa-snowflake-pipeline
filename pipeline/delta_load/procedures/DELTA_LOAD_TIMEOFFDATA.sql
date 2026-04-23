USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_TIMEOFFDATA(
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
            WHERE table_id = ''99629826-fe8e-61a4-0371-e3b33791fd23''
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
        if (file_list.length === 0) { return "No delta files found for TIMEOFFDATA."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMEOFFDATA;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMEOFFDATA (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME, TIMEOFFDATAID, USERID, PAYTYPEID,
                    ACCRUEDSECS, GRANTEDSECS, MANSECS, USEDSECS, AVAILABLESECS,
                    APPLYTODATETIME, ADJUSTMENTUSERID, ISSYSTEMGENERATED, NOTES,
                    TIMESLICEPREID, CREATIONDATETIME, MODIFIEDBY, MODIFIEDON,
                    MAKEUPSECS, ANCHORPOINT, ROLLOVERSECS, FORFEITEDSECS,
                    TRANSFERINID, TRANSFERINSECS, TRANSFEROUTID, TRANSFEROUTSECS,
                    TOTALACCRUEDSECS, MANTYPE, ISROLLOVER, PROCESSINDEX,
                    DELAYEDGRANTSECS, SECONDSWORKEDSTORE, EXPIRESDATETIME,
                    ROLLOVERTRANSFERINID, ROLLOVERTRANSFERINSECS,
                    ROLLOVERTRANSFEROUTID, ROLLOVERTRANSFEROUTSECS,
                    LASTPROCESSEDEVENTID, LASTPROCESSEDEVENTDATETIME
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::NUMBER(38,0),
                        $8::NUMBER(38,0), $9::NUMBER(38,0), $10::NUMBER(38,0),
                        $11::NUMBER(38,0), $12::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($13), $14::NUMBER(38,0), $15::BOOLEAN, $16::TEXT,
                        $17::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($18), $19::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($20),
                        $21::NUMBER(38,0), $22::BOOLEAN, $23::NUMBER(38,0), $24::NUMBER(38,0),
                        $25::NUMBER(38,0), $26::NUMBER(38,0), $27::NUMBER(38,0), $28::NUMBER(38,0),
                        $29::NUMBER(38,0), $30::NUMBER(38,0), $31::BOOLEAN, $32::NUMBER(38,0),
                        $33::NUMBER(38,0), $34::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($35),
                        $36::NUMBER(38,0), $37::NUMBER(38,0),
                        $38::NUMBER(38,0), $39::NUMBER(38,0),
                        $40::TEXT, TRY_TO_TIMESTAMP_NTZ($41)
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
            MERGE INTO TIMEOFFDATA tgt
            USING (
                SELECT * FROM STG_DELTA_TIMEOFFDATA WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMEOFFDATAID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.TIMEOFFDATAID        = src.TIMEOFFDATAID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID                    = src.USERID,
                tgt.PAYTYPEID                 = src.PAYTYPEID,
                tgt.ACCRUEDSECS               = src.ACCRUEDSECS,
                tgt.GRANTEDSECS               = src.GRANTEDSECS,
                tgt.MANSECS                   = src.MANSECS,
                tgt.USEDSECS                  = src.USEDSECS,
                tgt.AVAILABLESECS             = src.AVAILABLESECS,
                tgt.APPLYTODATETIME           = src.APPLYTODATETIME,
                tgt.ADJUSTMENTUSERID          = src.ADJUSTMENTUSERID,
                tgt.ISSYSTEMGENERATED         = src.ISSYSTEMGENERATED,
                tgt.NOTES                     = src.NOTES,
                tgt.TIMESLICEPREID            = src.TIMESLICEPREID,
                tgt.CREATIONDATETIME          = src.CREATIONDATETIME,
                tgt.MODIFIEDBY                = src.MODIFIEDBY,
                tgt.MODIFIEDON                = src.MODIFIEDON,
                tgt.MAKEUPSECS                = src.MAKEUPSECS,
                tgt.ANCHORPOINT               = src.ANCHORPOINT,
                tgt.ROLLOVERSECS              = src.ROLLOVERSECS,
                tgt.FORFEITEDSECS             = src.FORFEITEDSECS,
                tgt.TRANSFERINID              = src.TRANSFERINID,
                tgt.TRANSFERINSECS            = src.TRANSFERINSECS,
                tgt.TRANSFEROUTID             = src.TRANSFEROUTID,
                tgt.TRANSFEROUTSECS           = src.TRANSFEROUTSECS,
                tgt.TOTALACCRUEDSECS          = src.TOTALACCRUEDSECS,
                tgt.MANTYPE                   = src.MANTYPE,
                tgt.ISROLLOVER                = src.ISROLLOVER,
                tgt.PROCESSINDEX              = src.PROCESSINDEX,
                tgt.DELAYEDGRANTSECS          = src.DELAYEDGRANTSECS,
                tgt.SECONDSWORKEDSTORE        = src.SECONDSWORKEDSTORE,
                tgt.EXPIRESDATETIME           = src.EXPIRESDATETIME,
                tgt.ROLLOVERTRANSFERINID      = src.ROLLOVERTRANSFERINID,
                tgt.ROLLOVERTRANSFERINSECS    = src.ROLLOVERTRANSFERINSECS,
                tgt.ROLLOVERTRANSFEROUTID     = src.ROLLOVERTRANSFEROUTID,
                tgt.ROLLOVERTRANSFEROUTSECS   = src.ROLLOVERTRANSFEROUTSECS,
                tgt.LASTPROCESSEDEVENTID      = src.LASTPROCESSEDEVENTID,
                tgt.LASTPROCESSEDEVENTDATETIME = src.LASTPROCESSEDEVENTDATETIME
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMEOFFDATAID, USERID, PAYTYPEID,
                ACCRUEDSECS, GRANTEDSECS, MANSECS, USEDSECS, AVAILABLESECS,
                APPLYTODATETIME, ADJUSTMENTUSERID, ISSYSTEMGENERATED, NOTES,
                TIMESLICEPREID, CREATIONDATETIME, MODIFIEDBY, MODIFIEDON,
                MAKEUPSECS, ANCHORPOINT, ROLLOVERSECS, FORFEITEDSECS,
                TRANSFERINID, TRANSFERINSECS, TRANSFEROUTID, TRANSFEROUTSECS,
                TOTALACCRUEDSECS, MANTYPE, ISROLLOVER, PROCESSINDEX,
                DELAYEDGRANTSECS, SECONDSWORKEDSTORE, EXPIRESDATETIME,
                ROLLOVERTRANSFERINID, ROLLOVERTRANSFERINSECS,
                ROLLOVERTRANSFEROUTID, ROLLOVERTRANSFEROUTSECS,
                LASTPROCESSEDEVENTID, LASTPROCESSEDEVENTDATETIME
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMEOFFDATAID, src.USERID, src.PAYTYPEID,
                src.ACCRUEDSECS, src.GRANTEDSECS, src.MANSECS, src.USEDSECS, src.AVAILABLESECS,
                src.APPLYTODATETIME, src.ADJUSTMENTUSERID, src.ISSYSTEMGENERATED, src.NOTES,
                src.TIMESLICEPREID, src.CREATIONDATETIME, src.MODIFIEDBY, src.MODIFIEDON,
                src.MAKEUPSECS, src.ANCHORPOINT, src.ROLLOVERSECS, src.FORFEITEDSECS,
                src.TRANSFERINID, src.TRANSFERINSECS, src.TRANSFEROUTID, src.TRANSFEROUTSECS,
                src.TOTALACCRUEDSECS, src.MANTYPE, src.ISROLLOVER, src.PROCESSINDEX,
                src.DELAYEDGRANTSECS, src.SECONDSWORKEDSTORE, src.EXPIRESDATETIME,
                src.ROLLOVERTRANSFERINID, src.ROLLOVERTRANSFERINSECS,
                src.ROLLOVERTRANSFEROUTID, src.ROLLOVERTRANSFEROUTSECS,
                src.LASTPROCESSEDEVENTID, src.LASTPROCESSEDEVENTDATETIME
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
        throw new Error("DELTA_LOAD_TIMEOFFDATA failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_TIMEOFFREQUEST
-- PK: DATABASEPHYSICALNAME + TIMEOFFREQUESTID
-- DATABASEPHYSICALNAME derived from path via REGEXP_SUBSTR on METADATA$FILENAME
-- =============================================================================
