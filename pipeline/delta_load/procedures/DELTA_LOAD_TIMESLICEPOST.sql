USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_TIMESLICEPOST(
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
            WHERE table_id = ''0b30f4a8-bf11-0296-664d-a6996e0dca32''
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
        if (file_list.length === 0) { return "No delta files found for TIMESLICEPOST."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMESLICEPOST;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMESLICEPOST (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    TIMESLICEPOSTID, USERID, PAYTYPEID, TIMESLICEPREIDIN, TIMESLICEPREIDOUT,
                    ACTUALDATETIMEIN, ACTUALDATETIMEOUT, ROUNDEDDATETIMEIN, ROUNDEDDATETIMEOUT,
                    UTCDATETIMEIN, UTCDATETIMEOUT,
                    TOTALPAIDDURATIONSECS, REGDURATIONSECS, OTDURATIONSECS, UNPAIDDURATIONSECS,
                    MGRAPPROVEDIN, MGRAPPROVEDOUT, MGRNOTEIN, MGRNOTEOUT,
                    EMPAPPROVEDIN, EMPAPPROVEDOUT, EMPNOTEIN, EMPNOTEOUT,
                    TIMESHEETSUBMISSIONIN, TIMESHEETSUBMISSIONOUT,
                    PAYRATE, CHARGERATE, TOTALEARNINGS,
                    MISSINGPUNCHTYPEIN, MISSINGPUNCHTYPEOUT, ISMODIFIEDIN, ISMODIFIEDOUT,
                    SCHEDULEID, SCHEDULEDETAILID, APPLYTODATE, CLOSEDTYPE, TIMESLICEGROUPID,
                    LLDETAILID1, LLDETAILID2, LLDETAILID3, LLDETAILID4, LLDETAILID5,
                    LLDETAILID6, LLDETAILID7, LLDETAILID8, LLDETAILID9, LLDETAILID10,
                    LLDETAILID11, LLDETAILID12, LLDETAILID13, LLDETAILID14, LLDETAILID15,
                    HASHVALUE, TRANSACTIONTYPEIN, TRANSACTIONTYPEOUT,
                    TRANSACTIONSOURCEIN, TRANSACTIONSOURCEOUT,
                    APPLYTOOVERTIME, PAYLEVELRATETYPE, HASMODIFIER, ISCANCELED,
                    COUNTTOWARDSHOLIDAYMIN, HASSHIFTDIFF, ISMEALPREMIUM,
                    MODIFIEDBY, MODIFIEDON,
                    ADMINAPPROVEDIN, ADMINAPPROVEDOUT, MGR2APPROVEDIN, MGR2APPROVEDOUT,
                    LONGITUDEIN, LONGITUDEOUT, LATITUDEIN, LATITUDEOUT,
                    ISCOMPTIME, COMPTIMEREQUESTID, COMPTIMEOTCONVERTEDSECS,
                    MGRAPPROVEDBYIN, MGRAPPROVEDBYOUT, MGR2APPROVEDBYIN, MGR2APPROVEDBYOUT,
                    ADMINAPPROVEDBYIN, ADMINAPPROVEDBYOUT,
                    ISFORECAST, ISRECONCILE, ISSWIPEANDGOIN, ISSWIPEANDGOOUT,
                    POPULATEDFROMSCHEDULEIN, POPULATEDFROMSCHEDULEOUT, ISCALLBACK,
                    ACCURACYIN, ACCURACYOUT, ISBREAKPREMIUM, ADDITIONALPREMIUMTYPE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::NUMBER(38,0),
                        $8::NUMBER(38,0), $9::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($10), TRY_TO_TIMESTAMP_NTZ($11),
                        TRY_TO_TIMESTAMP_NTZ($12), TRY_TO_TIMESTAMP_NTZ($13),
                        TRY_TO_TIMESTAMP_NTZ($14), TRY_TO_TIMESTAMP_NTZ($15),
                        $16::NUMBER(38,0), $17::NUMBER(38,0), $18::NUMBER(38,0), $19::NUMBER(38,0),
                        $20::BOOLEAN, $21::BOOLEAN, $22::TEXT, $23::TEXT,
                        $24::BOOLEAN, $25::BOOLEAN, $26::TEXT, $27::TEXT,
                        $28::BOOLEAN, $29::BOOLEAN,
                        $30::NUMBER(19,4), $31::NUMBER(19,4), $32::NUMBER(19,4),
                        $33::NUMBER(38,0), $34::NUMBER(38,0), $35::BOOLEAN, $36::BOOLEAN,
                        $37::NUMBER(38,0), $38::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($39),
                        $40::NUMBER(38,0), $41::TEXT,
                        $42::NUMBER(38,0), $43::NUMBER(38,0), $44::NUMBER(38,0),
                        $45::NUMBER(38,0), $46::NUMBER(38,0), $47::NUMBER(38,0),
                        $48::NUMBER(38,0), $49::NUMBER(38,0), $50::NUMBER(38,0),
                        $51::NUMBER(38,0), $52::NUMBER(38,0), $53::NUMBER(38,0),
                        $54::NUMBER(38,0), $55::NUMBER(38,0), $56::NUMBER(38,0),
                        $57::TEXT,
                        $58::NUMBER(38,0), $59::NUMBER(38,0),
                        $60::NUMBER(38,0), $61::NUMBER(38,0),
                        $62::BOOLEAN, $63::NUMBER(38,0), $64::BOOLEAN, $65::BOOLEAN,
                        $66::BOOLEAN, $67::BOOLEAN, $68::BOOLEAN,
                        $69::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($70),
                        $71::BOOLEAN, $72::BOOLEAN, $73::BOOLEAN, $74::BOOLEAN,
                        $75::NUMBER(18,4), $76::NUMBER(18,4), $77::NUMBER(18,4), $78::NUMBER(18,4),
                        $79::BOOLEAN, $80::NUMBER(38,0), $81::NUMBER(38,0),
                        $82::NUMBER(38,0), $83::NUMBER(38,0),
                        $84::NUMBER(38,0), $85::NUMBER(38,0),
                        $86::NUMBER(38,0), $87::NUMBER(38,0),
                        $88::BOOLEAN, $89::BOOLEAN, $90::BOOLEAN, $91::BOOLEAN,
                        $92::BOOLEAN, $93::BOOLEAN, $94::BOOLEAN,
                        $95::NUMBER(18,4), $96::NUMBER(18,4), $97::BOOLEAN, $98::NUMBER(38,0)
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
            MERGE INTO TIMESLICEPOST tgt
            USING (
                SELECT * FROM STG_DELTA_TIMESLICEPOST WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMESLICEPOSTID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.TIMESLICEPOSTID      = src.TIMESLICEPOSTID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID = src.USERID, tgt.PAYTYPEID = src.PAYTYPEID,
                tgt.TIMESLICEPREIDIN = src.TIMESLICEPREIDIN,
                tgt.TIMESLICEPREIDOUT = src.TIMESLICEPREIDOUT,
                tgt.ACTUALDATETIMEIN = src.ACTUALDATETIMEIN,
                tgt.ACTUALDATETIMEOUT = src.ACTUALDATETIMEOUT,
                tgt.ROUNDEDDATETIMEIN = src.ROUNDEDDATETIMEIN,
                tgt.ROUNDEDDATETIMEOUT = src.ROUNDEDDATETIMEOUT,
                tgt.UTCDATETIMEIN = src.UTCDATETIMEIN,
                tgt.UTCDATETIMEOUT = src.UTCDATETIMEOUT,
                tgt.TOTALPAIDDURATIONSECS = src.TOTALPAIDDURATIONSECS,
                tgt.REGDURATIONSECS = src.REGDURATIONSECS,
                tgt.OTDURATIONSECS = src.OTDURATIONSECS,
                tgt.UNPAIDDURATIONSECS = src.UNPAIDDURATIONSECS,
                tgt.MGRAPPROVEDIN = src.MGRAPPROVEDIN,
                tgt.MGRAPPROVEDOUT = src.MGRAPPROVEDOUT,
                tgt.MGRNOTEIN = src.MGRNOTEIN, tgt.MGRNOTEOUT = src.MGRNOTEOUT,
                tgt.EMPAPPROVEDIN = src.EMPAPPROVEDIN,
                tgt.EMPAPPROVEDOUT = src.EMPAPPROVEDOUT,
                tgt.EMPNOTEIN = src.EMPNOTEIN, tgt.EMPNOTEOUT = src.EMPNOTEOUT,
                tgt.TIMESHEETSUBMISSIONIN = src.TIMESHEETSUBMISSIONIN,
                tgt.TIMESHEETSUBMISSIONOUT = src.TIMESHEETSUBMISSIONOUT,
                tgt.PAYRATE = src.PAYRATE, tgt.CHARGERATE = src.CHARGERATE,
                tgt.TOTALEARNINGS = src.TOTALEARNINGS,
                tgt.MISSINGPUNCHTYPEIN = src.MISSINGPUNCHTYPEIN,
                tgt.MISSINGPUNCHTYPEOUT = src.MISSINGPUNCHTYPEOUT,
                tgt.ISMODIFIEDIN = src.ISMODIFIEDIN,
                tgt.ISMODIFIEDOUT = src.ISMODIFIEDOUT,
                tgt.SCHEDULEID = src.SCHEDULEID,
                tgt.SCHEDULEDETAILID = src.SCHEDULEDETAILID,
                tgt.APPLYTODATE = src.APPLYTODATE, tgt.CLOSEDTYPE = src.CLOSEDTYPE,
                tgt.TIMESLICEGROUPID = src.TIMESLICEGROUPID,
                tgt.LLDETAILID1 = src.LLDETAILID1, tgt.LLDETAILID2 = src.LLDETAILID2,
                tgt.LLDETAILID3 = src.LLDETAILID3, tgt.LLDETAILID4 = src.LLDETAILID4,
                tgt.LLDETAILID5 = src.LLDETAILID5, tgt.LLDETAILID6 = src.LLDETAILID6,
                tgt.LLDETAILID7 = src.LLDETAILID7, tgt.LLDETAILID8 = src.LLDETAILID8,
                tgt.LLDETAILID9 = src.LLDETAILID9, tgt.LLDETAILID10 = src.LLDETAILID10,
                tgt.LLDETAILID11 = src.LLDETAILID11, tgt.LLDETAILID12 = src.LLDETAILID12,
                tgt.LLDETAILID13 = src.LLDETAILID13, tgt.LLDETAILID14 = src.LLDETAILID14,
                tgt.LLDETAILID15 = src.LLDETAILID15,
                tgt.HASHVALUE = src.HASHVALUE,
                tgt.TRANSACTIONTYPEIN = src.TRANSACTIONTYPEIN,
                tgt.TRANSACTIONTYPEOUT = src.TRANSACTIONTYPEOUT,
                tgt.TRANSACTIONSOURCEIN = src.TRANSACTIONSOURCEIN,
                tgt.TRANSACTIONSOURCEOUT = src.TRANSACTIONSOURCEOUT,
                tgt.APPLYTOOVERTIME = src.APPLYTOOVERTIME,
                tgt.PAYLEVELRATETYPE = src.PAYLEVELRATETYPE,
                tgt.HASMODIFIER = src.HASMODIFIER, tgt.ISCANCELED = src.ISCANCELED,
                tgt.COUNTTOWARDSHOLIDAYMIN = src.COUNTTOWARDSHOLIDAYMIN,
                tgt.HASSHIFTDIFF = src.HASSHIFTDIFF,
                tgt.ISMEALPREMIUM = src.ISMEALPREMIUM,
                tgt.MODIFIEDBY = src.MODIFIEDBY, tgt.MODIFIEDON = src.MODIFIEDON,
                tgt.ADMINAPPROVEDIN = src.ADMINAPPROVEDIN,
                tgt.ADMINAPPROVEDOUT = src.ADMINAPPROVEDOUT,
                tgt.MGR2APPROVEDIN = src.MGR2APPROVEDIN,
                tgt.MGR2APPROVEDOUT = src.MGR2APPROVEDOUT,
                tgt.LONGITUDEIN = src.LONGITUDEIN, tgt.LONGITUDEOUT = src.LONGITUDEOUT,
                tgt.LATITUDEIN = src.LATITUDEIN, tgt.LATITUDEOUT = src.LATITUDEOUT,
                tgt.ISCOMPTIME = src.ISCOMPTIME,
                tgt.COMPTIMEREQUESTID = src.COMPTIMEREQUESTID,
                tgt.COMPTIMEOTCONVERTEDSECS = src.COMPTIMEOTCONVERTEDSECS,
                tgt.MGRAPPROVEDBYIN = src.MGRAPPROVEDBYIN,
                tgt.MGRAPPROVEDBYOUT = src.MGRAPPROVEDBYOUT,
                tgt.MGR2APPROVEDBYIN = src.MGR2APPROVEDBYIN,
                tgt.MGR2APPROVEDBYOUT = src.MGR2APPROVEDBYOUT,
                tgt.ADMINAPPROVEDBYIN = src.ADMINAPPROVEDBYIN,
                tgt.ADMINAPPROVEDBYOUT = src.ADMINAPPROVEDBYOUT,
                tgt.ISFORECAST = src.ISFORECAST, tgt.ISRECONCILE = src.ISRECONCILE,
                tgt.ISSWIPEANDGOIN = src.ISSWIPEANDGOIN,
                tgt.ISSWIPEANDGOOUT = src.ISSWIPEANDGOOUT,
                tgt.POPULATEDFROMSCHEDULEIN = src.POPULATEDFROMSCHEDULEIN,
                tgt.POPULATEDFROMSCHEDULEOUT = src.POPULATEDFROMSCHEDULEOUT,
                tgt.ISCALLBACK = src.ISCALLBACK,
                tgt.ACCURACYIN = src.ACCURACYIN, tgt.ACCURACYOUT = src.ACCURACYOUT,
                tgt.ISBREAKPREMIUM = src.ISBREAKPREMIUM,
                tgt.ADDITIONALPREMIUMTYPE = src.ADDITIONALPREMIUMTYPE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMESLICEPOSTID, USERID, PAYTYPEID,
                TIMESLICEPREIDIN, TIMESLICEPREIDOUT,
                ACTUALDATETIMEIN, ACTUALDATETIMEOUT, ROUNDEDDATETIMEIN, ROUNDEDDATETIMEOUT,
                UTCDATETIMEIN, UTCDATETIMEOUT,
                TOTALPAIDDURATIONSECS, REGDURATIONSECS, OTDURATIONSECS, UNPAIDDURATIONSECS,
                MGRAPPROVEDIN, MGRAPPROVEDOUT, MGRNOTEIN, MGRNOTEOUT,
                EMPAPPROVEDIN, EMPAPPROVEDOUT, EMPNOTEIN, EMPNOTEOUT,
                TIMESHEETSUBMISSIONIN, TIMESHEETSUBMISSIONOUT,
                PAYRATE, CHARGERATE, TOTALEARNINGS,
                MISSINGPUNCHTYPEIN, MISSINGPUNCHTYPEOUT, ISMODIFIEDIN, ISMODIFIEDOUT,
                SCHEDULEID, SCHEDULEDETAILID, APPLYTODATE, CLOSEDTYPE, TIMESLICEGROUPID,
                LLDETAILID1, LLDETAILID2, LLDETAILID3, LLDETAILID4, LLDETAILID5,
                LLDETAILID6, LLDETAILID7, LLDETAILID8, LLDETAILID9, LLDETAILID10,
                LLDETAILID11, LLDETAILID12, LLDETAILID13, LLDETAILID14, LLDETAILID15,
                HASHVALUE, TRANSACTIONTYPEIN, TRANSACTIONTYPEOUT,
                TRANSACTIONSOURCEIN, TRANSACTIONSOURCEOUT,
                APPLYTOOVERTIME, PAYLEVELRATETYPE, HASMODIFIER, ISCANCELED,
                COUNTTOWARDSHOLIDAYMIN, HASSHIFTDIFF, ISMEALPREMIUM,
                MODIFIEDBY, MODIFIEDON,
                ADMINAPPROVEDIN, ADMINAPPROVEDOUT, MGR2APPROVEDIN, MGR2APPROVEDOUT,
                LONGITUDEIN, LONGITUDEOUT, LATITUDEIN, LATITUDEOUT,
                ISCOMPTIME, COMPTIMEREQUESTID, COMPTIMEOTCONVERTEDSECS,
                MGRAPPROVEDBYIN, MGRAPPROVEDBYOUT, MGR2APPROVEDBYIN, MGR2APPROVEDBYOUT,
                ADMINAPPROVEDBYIN, ADMINAPPROVEDBYOUT,
                ISFORECAST, ISRECONCILE, ISSWIPEANDGOIN, ISSWIPEANDGOOUT,
                POPULATEDFROMSCHEDULEIN, POPULATEDFROMSCHEDULEOUT, ISCALLBACK,
                ACCURACYIN, ACCURACYOUT, ISBREAKPREMIUM, ADDITIONALPREMIUMTYPE
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMESLICEPOSTID, src.USERID, src.PAYTYPEID,
                src.TIMESLICEPREIDIN, src.TIMESLICEPREIDOUT,
                src.ACTUALDATETIMEIN, src.ACTUALDATETIMEOUT,
                src.ROUNDEDDATETIMEIN, src.ROUNDEDDATETIMEOUT,
                src.UTCDATETIMEIN, src.UTCDATETIMEOUT,
                src.TOTALPAIDDURATIONSECS, src.REGDURATIONSECS,
                src.OTDURATIONSECS, src.UNPAIDDURATIONSECS,
                src.MGRAPPROVEDIN, src.MGRAPPROVEDOUT, src.MGRNOTEIN, src.MGRNOTEOUT,
                src.EMPAPPROVEDIN, src.EMPAPPROVEDOUT, src.EMPNOTEIN, src.EMPNOTEOUT,
                src.TIMESHEETSUBMISSIONIN, src.TIMESHEETSUBMISSIONOUT,
                src.PAYRATE, src.CHARGERATE, src.TOTALEARNINGS,
                src.MISSINGPUNCHTYPEIN, src.MISSINGPUNCHTYPEOUT,
                src.ISMODIFIEDIN, src.ISMODIFIEDOUT,
                src.SCHEDULEID, src.SCHEDULEDETAILID, src.APPLYTODATE,
                src.CLOSEDTYPE, src.TIMESLICEGROUPID,
                src.LLDETAILID1, src.LLDETAILID2, src.LLDETAILID3,
                src.LLDETAILID4, src.LLDETAILID5, src.LLDETAILID6,
                src.LLDETAILID7, src.LLDETAILID8, src.LLDETAILID9,
                src.LLDETAILID10, src.LLDETAILID11, src.LLDETAILID12,
                src.LLDETAILID13, src.LLDETAILID14, src.LLDETAILID15,
                src.HASHVALUE, src.TRANSACTIONTYPEIN, src.TRANSACTIONTYPEOUT,
                src.TRANSACTIONSOURCEIN, src.TRANSACTIONSOURCEOUT,
                src.APPLYTOOVERTIME, src.PAYLEVELRATETYPE,
                src.HASMODIFIER, src.ISCANCELED,
                src.COUNTTOWARDSHOLIDAYMIN, src.HASSHIFTDIFF, src.ISMEALPREMIUM,
                src.MODIFIEDBY, src.MODIFIEDON,
                src.ADMINAPPROVEDIN, src.ADMINAPPROVEDOUT,
                src.MGR2APPROVEDIN, src.MGR2APPROVEDOUT,
                src.LONGITUDEIN, src.LONGITUDEOUT, src.LATITUDEIN, src.LATITUDEOUT,
                src.ISCOMPTIME, src.COMPTIMEREQUESTID, src.COMPTIMEOTCONVERTEDSECS,
                src.MGRAPPROVEDBYIN, src.MGRAPPROVEDBYOUT,
                src.MGR2APPROVEDBYIN, src.MGR2APPROVEDBYOUT,
                src.ADMINAPPROVEDBYIN, src.ADMINAPPROVEDBYOUT,
                src.ISFORECAST, src.ISRECONCILE,
                src.ISSWIPEANDGOIN, src.ISSWIPEANDGOOUT,
                src.POPULATEDFROMSCHEDULEIN, src.POPULATEDFROMSCHEDULEOUT,
                src.ISCALLBACK, src.ACCURACYIN, src.ACCURACYOUT,
                src.ISBREAKPREMIUM, src.ADDITIONALPREMIUMTYPE
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
        throw new Error("DELTA_LOAD_TIMESLICEPOST failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL
-- PK: DATABASEPHYSICALNAME + TIMESLICEPOSTEXCEPTIONDETAILID
-- DATABASEPHYSICALNAME derived from path via REGEXP_SUBSTR on METADATA$FILENAME
-- =============================================================================
