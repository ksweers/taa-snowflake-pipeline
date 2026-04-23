USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE DELTA_LOAD_CUSTOMER(
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
        // CUSTOMER is non-multi-tenant: DATABASEPHYSICALNAME is a data column, not path-derived.
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                client_id, table_id, filename, full_file_path, last_modified
            FROM STAGE_TAA_DELTA_MANIFEST
            WHERE table_id = ''bf376338-3aaf-4306-9885-db20b386631c''
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
        if (file_list.length === 0) { return "No delta files found for CUSTOMER."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_CUSTOMER;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_CUSTOMER (
                    CHANGE_TYPE, LSN, CUSTOMERID, CUSTOMERNAME, DATABASECREATIONDATE, CUSTOMERALIAS,
                    CUSTOMERSTATUS, BRANDID, DATABASEPHYSICALNAME, DATABASESERVER,
                    SUPPORTEMAILADDRESS, IVRALIAS, ACTIVEEMPLOYEES, EMAILSERVER, EMAILPORT,
                    EMAILSSLENABLED, EMAILACCOUNT, EMAILUSERNAME, EMAILPASSWORD, EMAILDOMAIN,
                    EMAILSETTINGSOVERRIDE, QUEUEDELAYUNTIL, MODIFIEDBY, MODIFIEDON,
                    WIRELESSENABLED, FINGERPRINTENABLED, CUSTOMERIDEXTERNAL, WSTRACEENABLED,
                    WSTIMESTARTED, WSSTARTEDBY, TELEPUNCHALIAS, BISCLIENTID,
                    CUSTOMERLASTACTIVATEDBY, CUSTOMERLASTACTIVATEDON,
                    CUSTOMERLASTDEACTIVATEDBY, CUSTOMERLASTDEACTIVATEDON,
                    ISPROXY, MIGWORKFLOW, ISESSENTIALS, ISC2C, DONOTDELETE,
                    ROLLUPMULTIFEINSHAREDEMPLOYEE, CUSTOMERCREATIONSTATUSTYPE,
                    ENABLEAUTOCLOSINGTIMECARD, ENTERPRISECAIDBILL, CEIDBILL,
                    PAYROLLCLIENTIDBILL, CLIENTTYPE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        $5::NUMBER(38,0), $6::TEXT, TRY_TO_TIMESTAMP_NTZ($7), $8::TEXT,
                        $9::NUMBER(38,0), $10::NUMBER(38,0), $11::TEXT, $12::TEXT,
                        $13::TEXT, $14::NUMBER(38,0), $15::NUMBER(38,0), $16::TEXT, $17::NUMBER(38,0),
                        $18::BOOLEAN, $19::TEXT, $20::TEXT, $21::TEXT, $22::TEXT,
                        $23::BOOLEAN, TRY_TO_TIMESTAMP_NTZ($24), $25::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($26),
                        $27::BOOLEAN, $28::BOOLEAN, $29::TEXT, $30::BOOLEAN,
                        TRY_TO_TIMESTAMP_NTZ($31), $32::NUMBER(38,0), $33::NUMBER(38,0), $34::TEXT,
                        $35::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($36),
                        $37::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($38),
                        $39::BOOLEAN, $40::NUMBER(38,0), $41::BOOLEAN, $42::BOOLEAN, $43::BOOLEAN,
                        $44::NUMBER(5,0), $45::NUMBER(38,0),
                        $46::BOOLEAN, $47::TEXT, $48::TEXT, $49::TEXT, $50::NUMBER(38,0)
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
            MERGE INTO CUSTOMER tgt
            USING (
                SELECT * FROM STG_DELTA_CUSTOMER WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY CUSTOMERID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON tgt.CUSTOMERID = src.CUSTOMERID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.CUSTOMERNAME              = src.CUSTOMERNAME,
                tgt.DATABASECREATIONDATE      = src.DATABASECREATIONDATE,
                tgt.CUSTOMERALIAS             = src.CUSTOMERALIAS,
                tgt.CUSTOMERSTATUS            = src.CUSTOMERSTATUS,
                tgt.BRANDID                   = src.BRANDID,
                tgt.DATABASEPHYSICALNAME      = src.DATABASEPHYSICALNAME,
                tgt.DATABASESERVER            = src.DATABASESERVER,
                tgt.SUPPORTEMAILADDRESS       = src.SUPPORTEMAILADDRESS,
                tgt.IVRALIAS                  = src.IVRALIAS,
                tgt.ACTIVEEMPLOYEES           = src.ACTIVEEMPLOYEES,
                tgt.EMAILSERVER               = src.EMAILSERVER,
                tgt.EMAILPORT                 = src.EMAILPORT,
                tgt.EMAILSSLENABLED           = src.EMAILSSLENABLED,
                tgt.EMAILACCOUNT              = src.EMAILACCOUNT,
                tgt.EMAILUSERNAME             = src.EMAILUSERNAME,
                tgt.EMAILPASSWORD             = src.EMAILPASSWORD,
                tgt.EMAILDOMAIN               = src.EMAILDOMAIN,
                tgt.EMAILSETTINGSOVERRIDE     = src.EMAILSETTINGSOVERRIDE,
                tgt.QUEUEDELAYUNTIL           = src.QUEUEDELAYUNTIL,
                tgt.MODIFIEDBY                = src.MODIFIEDBY,
                tgt.MODIFIEDON                = src.MODIFIEDON,
                tgt.WIRELESSENABLED           = src.WIRELESSENABLED,
                tgt.FINGERPRINTENABLED        = src.FINGERPRINTENABLED,
                tgt.CUSTOMERIDEXTERNAL        = src.CUSTOMERIDEXTERNAL,
                tgt.WSTRACEENABLED            = src.WSTRACEENABLED,
                tgt.WSTIMESTARTED             = src.WSTIMESTARTED,
                tgt.WSSTARTEDBY               = src.WSSTARTEDBY,
                tgt.TELEPUNCHALIAS            = src.TELEPUNCHALIAS,
                tgt.BISCLIENTID               = src.BISCLIENTID,
                tgt.CUSTOMERLASTACTIVATEDBY   = src.CUSTOMERLASTACTIVATEDBY,
                tgt.CUSTOMERLASTACTIVATEDON   = src.CUSTOMERLASTACTIVATEDON,
                tgt.CUSTOMERLASTDEACTIVATEDBY = src.CUSTOMERLASTDEACTIVATEDBY,
                tgt.CUSTOMERLASTDEACTIVATEDON = src.CUSTOMERLASTDEACTIVATEDON,
                tgt.ISPROXY                   = src.ISPROXY,
                tgt.MIGWORKFLOW               = src.MIGWORKFLOW,
                tgt.ISESSENTIALS              = src.ISESSENTIALS,
                tgt.ISC2C                     = src.ISC2C,
                tgt.DONOTDELETE               = src.DONOTDELETE,
                tgt.ROLLUPMULTIFEINSHAREDEMPLOYEE  = src.ROLLUPMULTIFEINSHAREDEMPLOYEE,
                tgt.CUSTOMERCREATIONSTATUSTYPE     = src.CUSTOMERCREATIONSTATUSTYPE,
                tgt.ENABLEAUTOCLOSINGTIMECARD      = src.ENABLEAUTOCLOSINGTIMECARD,
                tgt.ENTERPRISECAIDBILL        = src.ENTERPRISECAIDBILL,
                tgt.CEIDBILL                  = src.CEIDBILL,
                tgt.PAYROLLCLIENTIDBILL       = src.PAYROLLCLIENTIDBILL,
                tgt.CLIENTTYPE                = src.CLIENTTYPE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                CUSTOMERID, CUSTOMERNAME, DATABASECREATIONDATE, CUSTOMERALIAS, CUSTOMERSTATUS,
                BRANDID, DATABASEPHYSICALNAME, DATABASESERVER, SUPPORTEMAILADDRESS, IVRALIAS,
                ACTIVEEMPLOYEES, EMAILSERVER, EMAILPORT, EMAILSSLENABLED, EMAILACCOUNT,
                EMAILUSERNAME, EMAILPASSWORD, EMAILDOMAIN, EMAILSETTINGSOVERRIDE, QUEUEDELAYUNTIL,
                MODIFIEDBY, MODIFIEDON, WIRELESSENABLED, FINGERPRINTENABLED, CUSTOMERIDEXTERNAL,
                WSTRACEENABLED, WSTIMESTARTED, WSSTARTEDBY, TELEPUNCHALIAS, BISCLIENTID,
                CUSTOMERLASTACTIVATEDBY, CUSTOMERLASTACTIVATEDON, CUSTOMERLASTDEACTIVATEDBY,
                CUSTOMERLASTDEACTIVATEDON, ISPROXY, MIGWORKFLOW, ISESSENTIALS, ISC2C, DONOTDELETE,
                ROLLUPMULTIFEINSHAREDEMPLOYEE, CUSTOMERCREATIONSTATUSTYPE, ENABLEAUTOCLOSINGTIMECARD,
                ENTERPRISECAIDBILL, CEIDBILL, PAYROLLCLIENTIDBILL, CLIENTTYPE
            ) VALUES (
                src.CUSTOMERID, src.CUSTOMERNAME, src.DATABASECREATIONDATE, src.CUSTOMERALIAS,
                src.CUSTOMERSTATUS, src.BRANDID, src.DATABASEPHYSICALNAME, src.DATABASESERVER,
                src.SUPPORTEMAILADDRESS, src.IVRALIAS, src.ACTIVEEMPLOYEES, src.EMAILSERVER,
                src.EMAILPORT, src.EMAILSSLENABLED, src.EMAILACCOUNT, src.EMAILUSERNAME,
                src.EMAILPASSWORD, src.EMAILDOMAIN, src.EMAILSETTINGSOVERRIDE, src.QUEUEDELAYUNTIL,
                src.MODIFIEDBY, src.MODIFIEDON, src.WIRELESSENABLED, src.FINGERPRINTENABLED,
                src.CUSTOMERIDEXTERNAL, src.WSTRACEENABLED, src.WSTIMESTARTED, src.WSSTARTEDBY,
                src.TELEPUNCHALIAS, src.BISCLIENTID, src.CUSTOMERLASTACTIVATEDBY,
                src.CUSTOMERLASTACTIVATEDON, src.CUSTOMERLASTDEACTIVATEDBY,
                src.CUSTOMERLASTDEACTIVATEDON, src.ISPROXY, src.MIGWORKFLOW, src.ISESSENTIALS,
                src.ISC2C, src.DONOTDELETE, src.ROLLUPMULTIFEINSHAREDEMPLOYEE,
                src.CUSTOMERCREATIONSTATUSTYPE, src.ENABLEAUTOCLOSINGTIMECARD,
                src.ENTERPRISECAIDBILL, src.CEIDBILL, src.PAYROLLCLIENTIDBILL, src.CLIENTTYPE
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
        throw new Error("DELTA_LOAD_CUSTOMER failed: " + err.message);
    }
';


-- =============================================================================
-- DELTA_LOAD_ENTERPRISECUSTOMER
-- PK: ENTERPRISECUSTOMERID  (non-multi-tenant; no DATABASEPHYSICALNAME column)
-- CSV columns: [meta1, meta2, change_type, meta4,
--               ENTERPRISECUSTOMERID, CUSTOMERID, PNGSSOCAID, CEID,
--               PAYROLLCLIENTID, MODIFIEDBY, MODIFIEDON, STRATUSTIMECAID,
--               LEGALCLIENTNAME, CEIDSTATUS, CEIDSTATUSDATE, MODIFIEDCHANGEREASON,
--               CEIDSUPERSEDEDBY, CACAID, HRISCAID, BISCLIENTID, USEDCLIENTMAINT]
-- =============================================================================
