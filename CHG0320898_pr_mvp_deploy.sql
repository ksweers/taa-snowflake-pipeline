USE ROLE ROLE_SNFLK_DSDP_APP_OWR;
USE WAREHOUSE WH_DSDP_ETL_PR;
USE DATABASE DL_P_STRATUSTIME_PR;
USE SCHEMA TAA;

GRANT USAGE ON STAGE EXST_STRATUSTIME_ONELAKE_PR TO ROLE ROLE_SNFLK_DSDP_APP_OWR;

UPDATE ingest_taa_file_audit
SET full_stage_path = SUBSTRING(full_stage_path, POSITION('/LandingZone/' IN full_stage_path));

-- FULL_LOAD procedures (15 tables) - old single-parameter versions
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_CUSTOMER(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_ENTERPRISECUSTOMER(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_LLDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_PAYTYPE(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_SCHEDULE(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFDATA(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFREQUEST(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFREQUESTDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOST(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFO(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOEMPSTATUS(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOISSALARY(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOPAYROLLMAPPING(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_FROM_CONFIG(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.BUILD_STAGE_TAA_FULL_FILE_MANIFEST(VARCHAR,VARCHAR);

-- DELTA_LOAD procedures (15 tables) - old single-parameter versions
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_CUSTOMER(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_ENTERPRISECUSTOMER(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_LLDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_PAYTYPE(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_SCHEDULE(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFDATA(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFREQUEST(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFREQUESTDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOST(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFO(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOEMPSTATUS(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOISSALARY(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOPAYROLLMAPPING(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_FROM_CONFIG(VARCHAR);
DROP PROCEDURE IF EXISTS DL_P_STRATUSTIME_PR.TAA.BUILD_STAGE_TAA_DELTA_MANIFEST(VARCHAR,VARCHAR);

CREATE or replace TABLE CLIENT_CONFIG (
    CLIENT_ID VARCHAR(200) NOT NULL,
    TABLE_ID VARCHAR(36) NOT NULL,
    ACTIVE_FOLDERNAME VARCHAR(500),
    CSV_SHARD_NO NUMBER(2), 
    PARQUET_SHARD_NO NUMBER(2), 
    CSV_DELTA_STATUS VARCHAR(5) DEFAULT 'N', 
    PARQUET_LOAD_STATUS VARCHAR(1) DEFAULT 'N',
    MASTER_STATUS VARCHAR(1) DEFAULT 'Y', 
    PRIMARY KEY (CLIENT_ID, TABLE_ID)
);

CREATE OR REPLACE FILE FORMAT FF_TAA_INVENTORY_CSV
    TYPE                         = CSV
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = (''); 

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_DELTA_PREPARE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var result_message = "";

    try {
        var cfg_result = snowflake.createStatement({sqlText: `
            SELECT PARAM_NAME, PARAM_VALUE
            FROM INGEST_TAA_DELTA_RUN_CONFIG
            WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')
        `}).execute();

        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }

        var stage_name_safe   = config["STAGE_NAME"]        || null;
        var client_filter     = config["CLIENT_ID_FILTER"]  || null;
        var table_name_filter = config["TABLE_NAME_FILTER"] || null;

        if (!stage_name_safe) {
            // STAGE_NAME not yet configured -- silent skip so scheduled runs
            // before first setup do not show as FAILED.
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_DELTA_RUN_CONFIG. " +
                   "Call INGEST_TAA_LAUNCH_DELTA_LOAD() to configure before the next scheduled run.";
        }

        var is_client_scoped = (client_filter !== null && client_filter.trim() !== "");

        result_message += "=== INGEST_TAA_DELTA_PREPARE ===\\n";
        result_message += "Client scope : " + (is_client_scoped ? client_filter : "ALL CLIENTS") + "\\n";
        result_message += "Stage        : " + stage_name_safe + "\\n";

        result_message += "\\n=== BUILDING DELTA MANIFEST ===\\n";
        var file_list_param  = is_client_scoped  ? "''" + client_filter     + "''" : "NULL";
        var table_list_param = table_name_filter ? "''" + table_name_filter + "''" : "NULL";
        var manifest_sql = "CALL BUILD_STAGE_TAA_DELTA_MANIFEST(" +
                           file_list_param + ", " + table_list_param + ", ''" + stage_name_safe + "'');";
        var manifest_result = snowflake.createStatement({sqlText: manifest_sql}).execute();
        manifest_result.next();
        result_message += "  " + manifest_result.getColumnValue(1) + "\\n";
        result_message += "\\nPREPARE COMPLETE -- Wave 1 tasks will now start.\\n";

        return result_message;

    } catch (err) {
        throw new Error("INGEST_TAA_DELTA_PREPARE failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_FROM_CONFIG("TABLE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var cfg = snowflake.createStatement({
            sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_DELTA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
        }).execute();
        cfg.next();
        var stage = cfg.getColumnValue(1);
        if (!stage) {
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_DELTA_RUN_CONFIG.";
        }
        var call_sql = "CALL DELTA_LOAD_" + TABLE_NAME + "(''" + stage + "'', ''" + CSV_SHARD_NO + "'')";
        var result = snowflake.createStatement({sqlText: call_sql}).execute();
        result.next();
        return result.getColumnValue(1);
    } catch (err) {
        throw new Error("DELTA_LOAD_FROM_CONFIG(" + TABLE_NAME + ", " + CSV_SHARD_NO + ") failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_CUSTOMER("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        // CUSTOMER is non-multi-tenant: DATABASEPHYSICALNAME is a data column, not path-derived.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''bf376338-3aaf-4306-9885-db20b386631c''
            ORDER BY m.last_modified ASC
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_ENTERPRISECUSTOMER("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        // ENTERPRISECUSTOMER is non-multi-tenant: all columns positional, no path-derived column.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''49f4e893-dbbd-280a-93b3-9edccba30424''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for ENTERPRISECUSTOMER."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_ENTERPRISECUSTOMER;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_ENTERPRISECUSTOMER (
                    CHANGE_TYPE, LSN, ENTERPRISECUSTOMERID, CUSTOMERID, PNGSSOCAID, CEID,
                    PAYROLLCLIENTID, MODIFIEDBY, MODIFIEDON, STRATUSTIMECAID, LEGALCLIENTNAME,
                    CEIDSTATUS, CEIDSTATUSDATE, MODIFIEDCHANGEREASON, CEIDSUPERSEDEDBY,
                    CACAID, HRISCAID, BISCLIENTID, USEDCLIENTMAINT
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::TEXT, $8::TEXT,
                        $9::TEXT, $10::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($11), $12::TEXT, $13::TEXT,
                        $14::TEXT, TRY_TO_TIMESTAMP_NTZ($15), $16::TEXT, $17::TEXT,
                        $18::TEXT, $19::TEXT, $20::TEXT, $21::BOOLEAN
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO ENTERPRISECUSTOMER tgt
            USING (
                SELECT * FROM STG_DELTA_ENTERPRISECUSTOMER WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY ENTERPRISECUSTOMERID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON tgt.ENTERPRISECUSTOMERID = src.ENTERPRISECUSTOMERID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.CUSTOMERID          = src.CUSTOMERID,
                tgt.PNGSSOCAID          = src.PNGSSOCAID,
                tgt.CEID                = src.CEID,
                tgt.PAYROLLCLIENTID     = src.PAYROLLCLIENTID,
                tgt.MODIFIEDBY          = src.MODIFIEDBY,
                tgt.MODIFIEDON          = src.MODIFIEDON,
                tgt.STRATUSTIMECAID     = src.STRATUSTIMECAID,
                tgt.LEGALCLIENTNAME     = src.LEGALCLIENTNAME,
                tgt.CEIDSTATUS          = src.CEIDSTATUS,
                tgt.CEIDSTATUSDATE      = src.CEIDSTATUSDATE,
                tgt.MODIFIEDCHANGEREASON = src.MODIFIEDCHANGEREASON,
                tgt.CEIDSUPERSEDEDBY    = src.CEIDSUPERSEDEDBY,
                tgt.CACAID              = src.CACAID,
                tgt.HRISCAID            = src.HRISCAID,
                tgt.BISCLIENTID         = src.BISCLIENTID,
                tgt.USEDCLIENTMAINT     = src.USEDCLIENTMAINT
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                ENTERPRISECUSTOMERID, CUSTOMERID, PNGSSOCAID, CEID, PAYROLLCLIENTID,
                MODIFIEDBY, MODIFIEDON, STRATUSTIMECAID, LEGALCLIENTNAME, CEIDSTATUS,
                CEIDSTATUSDATE, MODIFIEDCHANGEREASON, CEIDSUPERSEDEDBY, CACAID,
                HRISCAID, BISCLIENTID, USEDCLIENTMAINT
            ) VALUES (
                src.ENTERPRISECUSTOMERID, src.CUSTOMERID, src.PNGSSOCAID, src.CEID,
                src.PAYROLLCLIENTID, src.MODIFIEDBY, src.MODIFIEDON, src.STRATUSTIMECAID,
                src.LEGALCLIENTNAME, src.CEIDSTATUS, src.CEIDSTATUSDATE,
                src.MODIFIEDCHANGEREASON, src.CEIDSUPERSEDEDBY, src.CACAID,
                src.HRISCAID, src.BISCLIENTID, src.USEDCLIENTMAINT
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
        throw new Error("DELTA_LOAD_ENTERPRISECUSTOMER failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_LLDETAIL("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''46c059a2-1b66-97a0-6dbc-4b1bf1ca4219''
            ORDER BY m.last_modified ASC
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_PAYTYPE("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''f774054a-9744-5cbf-731e-1bdd7df870f7''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for PAYTYPE."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_PAYTYPE;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_PAYTYPE (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME, ID, PAYTYPEID,
                    STARTDATETIME, ENDDATETIME, ISDELETED, PAYTYPENAME, PAYTYPECODE,
                    COUNTTOWARDSHOLIDAYMIN, OVERRIDESABSENCE, ISWORKTYPE, APPLYTOOVERTIME,
                    INCLUDEINBLENDEDRATE, APPLYTOTIMEOFF, CANBEOVERTIME, DEFAULTPAYLEVELRATETYPE,
                    ISOVERTIMETYPE, OVERTIMEFACTOR, COLORCODE, MODIFIEDBY, MODIFIEDON,
                    ALLOWSHIFTDIFF, DEDUCTFROMPAYTYPE, DEDUCTFROMPAYTYPEID,
                    ISDURATIONONLY, ISLLREQUIRED, ISFMLATYPE, PAYATWEIGHTEDRATE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($7), TRY_TO_TIMESTAMP_NTZ($8), $9::BOOLEAN, $10::TEXT, $11::TEXT,
                        $12::BOOLEAN, $13::BOOLEAN, $14::BOOLEAN, $15::BOOLEAN,
                        $16::BOOLEAN, $17::BOOLEAN, $18::BOOLEAN, $19::NUMBER(38,0),
                        $20::BOOLEAN, $21::NUMBER(18,2), $22::TEXT, $23::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($24),
                        $25::BOOLEAN, $26::BOOLEAN, $27::NUMBER(38,0),
                        $28::BOOLEAN, $29::BOOLEAN, $30::BOOLEAN, $31::BOOLEAN
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO PAYTYPE tgt
            USING (
                SELECT * FROM STG_DELTA_PAYTYPE WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, ID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.ID                   = src.ID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.PAYTYPEID               = src.PAYTYPEID,
                tgt.STARTDATETIME           = src.STARTDATETIME,
                tgt.ENDDATETIME             = src.ENDDATETIME,
                tgt.ISDELETED               = src.ISDELETED,
                tgt.PAYTYPENAME             = src.PAYTYPENAME,
                tgt.PAYTYPECODE             = src.PAYTYPECODE,
                tgt.COUNTTOWARDSHOLIDAYMIN  = src.COUNTTOWARDSHOLIDAYMIN,
                tgt.OVERRIDESABSENCE        = src.OVERRIDESABSENCE,
                tgt.ISWORKTYPE              = src.ISWORKTYPE,
                tgt.APPLYTOOVERTIME         = src.APPLYTOOVERTIME,
                tgt.INCLUDEINBLENDEDRATE    = src.INCLUDEINBLENDEDRATE,
                tgt.APPLYTOTIMEOFF          = src.APPLYTOTIMEOFF,
                tgt.CANBEOVERTIME           = src.CANBEOVERTIME,
                tgt.DEFAULTPAYLEVELRATETYPE = src.DEFAULTPAYLEVELRATETYPE,
                tgt.ISOVERTIMETYPE          = src.ISOVERTIMETYPE,
                tgt.OVERTIMEFACTOR          = src.OVERTIMEFACTOR,
                tgt.COLORCODE               = src.COLORCODE,
                tgt.MODIFIEDBY              = src.MODIFIEDBY,
                tgt.MODIFIEDON              = src.MODIFIEDON,
                tgt.ALLOWSHIFTDIFF          = src.ALLOWSHIFTDIFF,
                tgt.DEDUCTFROMPAYTYPE       = src.DEDUCTFROMPAYTYPE,
                tgt.DEDUCTFROMPAYTYPEID     = src.DEDUCTFROMPAYTYPEID,
                tgt.ISDURATIONONLY          = src.ISDURATIONONLY,
                tgt.ISLLREQUIRED            = src.ISLLREQUIRED,
                tgt.ISFMLATYPE              = src.ISFMLATYPE,
                tgt.PAYATWEIGHTEDRATE       = src.PAYATWEIGHTEDRATE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, ID, PAYTYPEID, STARTDATETIME, ENDDATETIME,
                ISDELETED, PAYTYPENAME, PAYTYPECODE, COUNTTOWARDSHOLIDAYMIN, OVERRIDESABSENCE,
                ISWORKTYPE, APPLYTOOVERTIME, INCLUDEINBLENDEDRATE, APPLYTOTIMEOFF, CANBEOVERTIME,
                DEFAULTPAYLEVELRATETYPE, ISOVERTIMETYPE, OVERTIMEFACTOR, COLORCODE,
                MODIFIEDBY, MODIFIEDON, ALLOWSHIFTDIFF, DEDUCTFROMPAYTYPE, DEDUCTFROMPAYTYPEID,
                ISDURATIONONLY, ISLLREQUIRED, ISFMLATYPE, PAYATWEIGHTEDRATE
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.ID, src.PAYTYPEID, src.STARTDATETIME, src.ENDDATETIME,
                src.ISDELETED, src.PAYTYPENAME, src.PAYTYPECODE, src.COUNTTOWARDSHOLIDAYMIN,
                src.OVERRIDESABSENCE, src.ISWORKTYPE, src.APPLYTOOVERTIME, src.INCLUDEINBLENDEDRATE,
                src.APPLYTOTIMEOFF, src.CANBEOVERTIME, src.DEFAULTPAYLEVELRATETYPE,
                src.ISOVERTIMETYPE, src.OVERTIMEFACTOR, src.COLORCODE,
                src.MODIFIEDBY, src.MODIFIEDON, src.ALLOWSHIFTDIFF, src.DEDUCTFROMPAYTYPE,
                src.DEDUCTFROMPAYTYPEID, src.ISDURATIONONLY, src.ISLLREQUIRED,
                src.ISFMLATYPE, src.PAYATWEIGHTEDRATE
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
        throw new Error("DELTA_LOAD_PAYTYPE failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_SCHEDULE("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''f4830a1d-ae29-8044-7c71-6bd4b5779b70''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for SCHEDULE."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_SCHEDULE;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_SCHEDULE (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME, SCHEDULEID, USERID, PAYTYPEID,
                    STARTDATETIME, ENDDATETIME,
                    LLDETAILID1, LLDETAILID2, LLDETAILID3, LLDETAILID4, LLDETAILID5,
                    LLDETAILID6, LLDETAILID7, LLDETAILID8, LLDETAILID9, LLDETAILID10,
                    LLDETAILID11, LLDETAILID12, LLDETAILID13, LLDETAILID14, LLDETAILID15,
                    ISAUTOGENERATED, MODIFIEDBY, MODIFIEDON, ADVSCHEDULECAPACITYDETAILID,
                    NOTE, STARTDATETIMEUTC, ENDDATETIMEUTC, CALENDAREVENTID,
                    ISCALENDARSYNC, SCHEDULEGENERATEDSOURCE, USERHASBEENNOTIFIED
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($8), TRY_TO_TIMESTAMP_NTZ($9),
                        $10::NUMBER(38,0), $11::NUMBER(38,0), $12::NUMBER(38,0),
                        $13::NUMBER(38,0), $14::NUMBER(38,0), $15::NUMBER(38,0),
                        $16::NUMBER(38,0), $17::NUMBER(38,0), $18::NUMBER(38,0),
                        $19::NUMBER(38,0), $20::NUMBER(38,0), $21::NUMBER(38,0),
                        $22::NUMBER(38,0), $23::NUMBER(38,0), $24::NUMBER(38,0),
                        $25::BOOLEAN, $26::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($27), $28::NUMBER(38,0),
                        $29::TEXT, TRY_TO_TIMESTAMP_NTZ($30), TRY_TO_TIMESTAMP_NTZ($31), $32::TEXT,
                        $33::BOOLEAN, $34::NUMBER(38,0), $35::BOOLEAN
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO SCHEDULE tgt
            USING (
                SELECT * FROM STG_DELTA_SCHEDULE WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, SCHEDULEID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME = src.DATABASEPHYSICALNAME
            AND tgt.SCHEDULEID           = src.SCHEDULEID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID                      = src.USERID,
                tgt.PAYTYPEID                   = src.PAYTYPEID,
                tgt.STARTDATETIME               = src.STARTDATETIME,
                tgt.ENDDATETIME                 = src.ENDDATETIME,
                tgt.LLDETAILID1                 = src.LLDETAILID1,
                tgt.LLDETAILID2                 = src.LLDETAILID2,
                tgt.LLDETAILID3                 = src.LLDETAILID3,
                tgt.LLDETAILID4                 = src.LLDETAILID4,
                tgt.LLDETAILID5                 = src.LLDETAILID5,
                tgt.LLDETAILID6                 = src.LLDETAILID6,
                tgt.LLDETAILID7                 = src.LLDETAILID7,
                tgt.LLDETAILID8                 = src.LLDETAILID8,
                tgt.LLDETAILID9                 = src.LLDETAILID9,
                tgt.LLDETAILID10                = src.LLDETAILID10,
                tgt.LLDETAILID11                = src.LLDETAILID11,
                tgt.LLDETAILID12                = src.LLDETAILID12,
                tgt.LLDETAILID13                = src.LLDETAILID13,
                tgt.LLDETAILID14                = src.LLDETAILID14,
                tgt.LLDETAILID15                = src.LLDETAILID15,
                tgt.ISAUTOGENERATED             = src.ISAUTOGENERATED,
                tgt.MODIFIEDBY                  = src.MODIFIEDBY,
                tgt.MODIFIEDON                  = src.MODIFIEDON,
                tgt.ADVSCHEDULECAPACITYDETAILID = src.ADVSCHEDULECAPACITYDETAILID,
                tgt.NOTE                        = src.NOTE,
                tgt.STARTDATETIMEUTC            = src.STARTDATETIMEUTC,
                tgt.ENDDATETIMEUTC              = src.ENDDATETIMEUTC,
                tgt.CALENDAREVENTID             = src.CALENDAREVENTID,
                tgt.ISCALENDARSYNC              = src.ISCALENDARSYNC,
                tgt.SCHEDULEGENERATEDSOURCE     = src.SCHEDULEGENERATEDSOURCE,
                tgt.USERHASBEENNOTIFIED         = src.USERHASBEENNOTIFIED
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, SCHEDULEID, USERID, PAYTYPEID,
                STARTDATETIME, ENDDATETIME,
                LLDETAILID1, LLDETAILID2, LLDETAILID3, LLDETAILID4, LLDETAILID5,
                LLDETAILID6, LLDETAILID7, LLDETAILID8, LLDETAILID9, LLDETAILID10,
                LLDETAILID11, LLDETAILID12, LLDETAILID13, LLDETAILID14, LLDETAILID15,
                ISAUTOGENERATED, MODIFIEDBY, MODIFIEDON, ADVSCHEDULECAPACITYDETAILID,
                NOTE, STARTDATETIMEUTC, ENDDATETIMEUTC, CALENDAREVENTID,
                ISCALENDARSYNC, SCHEDULEGENERATEDSOURCE, USERHASBEENNOTIFIED
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.SCHEDULEID, src.USERID, src.PAYTYPEID,
                src.STARTDATETIME, src.ENDDATETIME,
                src.LLDETAILID1, src.LLDETAILID2, src.LLDETAILID3, src.LLDETAILID4, src.LLDETAILID5,
                src.LLDETAILID6, src.LLDETAILID7, src.LLDETAILID8, src.LLDETAILID9, src.LLDETAILID10,
                src.LLDETAILID11, src.LLDETAILID12, src.LLDETAILID13, src.LLDETAILID14, src.LLDETAILID15,
                src.ISAUTOGENERATED, src.MODIFIEDBY, src.MODIFIEDON, src.ADVSCHEDULECAPACITYDETAILID,
                src.NOTE, src.STARTDATETIMEUTC, src.ENDDATETIMEUTC, src.CALENDAREVENTID,
                src.ISCALENDARSYNC, src.SCHEDULEGENERATEDSOURCE, src.USERHASBEENNOTIFIED
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
        throw new Error("DELTA_LOAD_SCHEDULE failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFDATA("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''99629826-fe8e-61a4-0371-e3b33791fd23''
            ORDER BY m.last_modified ASC
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFREQUEST("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''db78652a-b192-ed5c-b7fd-410e8e8eb47a''
            ORDER BY m.last_modified ASC
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMEOFFREQUESTDETAIL("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''f9e8cf07-8d4f-1c51-df47-da7de058a176''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for TIMEOFFREQUESTDETAIL."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMEOFFREQUESTDETAIL;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMEOFFREQUESTDETAIL (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME, TIMEOFFREQUESTDETAILID,
                    TIMEOFFREQUESTID, STARTDATETIME, ENDDATETIME, STATUSTYPE, ISDELETED,
                    STATUSCHANGEDBY, STATUSCHANGEDON, MGRNOTES, TIMESLICEPREID,
                    AUTORESETQUALIFYBYHOURSWORKED, ISCALENDARSYNC, CALENDAREVENTID
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($7), TRY_TO_TIMESTAMP_NTZ($8),
                        $9::NUMBER(38,0), $10::BOOLEAN, $11::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($12),
                        $13::TEXT, $14::NUMBER(38,0), $15::BOOLEAN, $16::BOOLEAN, $17::TEXT
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO TIMEOFFREQUESTDETAIL tgt
            USING (
                SELECT * FROM STG_DELTA_TIMEOFFREQUESTDETAIL WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMEOFFREQUESTDETAILID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME   = src.DATABASEPHYSICALNAME
            AND tgt.TIMEOFFREQUESTDETAILID = src.TIMEOFFREQUESTDETAILID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.TIMEOFFREQUESTID              = src.TIMEOFFREQUESTID,
                tgt.STARTDATETIME                 = src.STARTDATETIME,
                tgt.ENDDATETIME                   = src.ENDDATETIME,
                tgt.STATUSTYPE                    = src.STATUSTYPE,
                tgt.ISDELETED                     = src.ISDELETED,
                tgt.STATUSCHANGEDBY               = src.STATUSCHANGEDBY,
                tgt.STATUSCHANGEDON               = src.STATUSCHANGEDON,
                tgt.MGRNOTES                      = src.MGRNOTES,
                tgt.TIMESLICEPREID                = src.TIMESLICEPREID,
                tgt.AUTORESETQUALIFYBYHOURSWORKED = src.AUTORESETQUALIFYBYHOURSWORKED,
                tgt.ISCALENDARSYNC                = src.ISCALENDARSYNC,
                tgt.CALENDAREVENTID               = src.CALENDAREVENTID
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMEOFFREQUESTDETAILID, TIMEOFFREQUESTID,
                STARTDATETIME, ENDDATETIME, STATUSTYPE, ISDELETED, STATUSCHANGEDBY,
                STATUSCHANGEDON, MGRNOTES, TIMESLICEPREID, AUTORESETQUALIFYBYHOURSWORKED,
                ISCALENDARSYNC, CALENDAREVENTID
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMEOFFREQUESTDETAILID, src.TIMEOFFREQUESTID,
                src.STARTDATETIME, src.ENDDATETIME, src.STATUSTYPE, src.ISDELETED,
                src.STATUSCHANGEDBY, src.STATUSCHANGEDON, src.MGRNOTES, src.TIMESLICEPREID,
                src.AUTORESETQUALIFYBYHOURSWORKED, src.ISCALENDARSYNC, src.CALENDAREVENTID
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
        throw new Error("DELTA_LOAD_TIMEOFFREQUESTDETAIL failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOST("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''0b30f4a8-bf11-0296-664d-a6996e0dca32''
            ORDER BY m.last_modified ASC
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''be6c4966-d75e-ef52-7460-75c736afbf26''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for TIMESLICEPOSTEXCEPTIONDETAIL."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    TIMESLICEPOSTEXCEPTIONDETAILID, USERID, TIMESLICEPOSTID, TIMESLICEPREID,
                    SCHEDULEID, DATETIME, EXCEPTIONPOLICYRULEID, EXCEPTIONTYPE,
                    TRANSACTIONTYPE, EXCEPTIONPARAMETERSECS, HASHVALUE, ISACKNOWLEDGED,
                    MODIFIEDBY, MODIFIEDON, MGRNOTE, ACKNOWLEDGEDBY
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0), $7::NUMBER(38,0), $8::NUMBER(38,0),
                        $9::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($10), $11::NUMBER(38,0), $12::NUMBER(38,0),
                        $13::NUMBER(38,0), $14::NUMBER(38,0), $15::TEXT, $16::BOOLEAN,
                        $17::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($18), $19::TEXT, $20::NUMBER(38,0)
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO TIMESLICEPOSTEXCEPTIONDETAIL tgt
            USING (
                SELECT * FROM STG_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMESLICEPOSTEXCEPTIONDETAILID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME           = src.DATABASEPHYSICALNAME
            AND tgt.TIMESLICEPOSTEXCEPTIONDETAILID = src.TIMESLICEPOSTEXCEPTIONDETAILID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID                = src.USERID,
                tgt.TIMESLICEPOSTID       = src.TIMESLICEPOSTID,
                tgt.TIMESLICEPREID        = src.TIMESLICEPREID,
                tgt.SCHEDULEID            = src.SCHEDULEID,
                tgt.DATETIME              = src.DATETIME,
                tgt.EXCEPTIONPOLICYRULEID = src.EXCEPTIONPOLICYRULEID,
                tgt.EXCEPTIONTYPE         = src.EXCEPTIONTYPE,
                tgt.TRANSACTIONTYPE       = src.TRANSACTIONTYPE,
                tgt.EXCEPTIONPARAMETERSECS = src.EXCEPTIONPARAMETERSECS,
                tgt.HASHVALUE             = src.HASHVALUE,
                tgt.ISACKNOWLEDGED        = src.ISACKNOWLEDGED,
                tgt.MODIFIEDBY            = src.MODIFIEDBY,
                tgt.MODIFIEDON            = src.MODIFIEDON,
                tgt.MGRNOTE               = src.MGRNOTE,
                tgt.ACKNOWLEDGEDBY        = src.ACKNOWLEDGEDBY
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMESLICEPOSTEXCEPTIONDETAILID,
                USERID, TIMESLICEPOSTID, TIMESLICEPREID, SCHEDULEID, DATETIME,
                EXCEPTIONPOLICYRULEID, EXCEPTIONTYPE, TRANSACTIONTYPE,
                EXCEPTIONPARAMETERSECS, HASHVALUE, ISACKNOWLEDGED,
                MODIFIEDBY, MODIFIEDON, MGRNOTE, ACKNOWLEDGEDBY
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMESLICEPOSTEXCEPTIONDETAILID,
                src.USERID, src.TIMESLICEPOSTID, src.TIMESLICEPREID, src.SCHEDULEID,
                src.DATETIME, src.EXCEPTIONPOLICYRULEID, src.EXCEPTIONTYPE,
                src.TRANSACTIONTYPE, src.EXCEPTIONPARAMETERSECS, src.HASHVALUE,
                src.ISACKNOWLEDGED, src.MODIFIEDBY, src.MODIFIEDON,
                src.MGRNOTE, src.ACKNOWLEDGEDBY
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
        throw new Error("DELTA_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''bb67bf1f-a87a-1912-57fd-686aee5c7361''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for TIMESLICEPOSTSHIFTDIFFDETAIL."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        //         Write per-file audit rows from each COPY result set.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    TIMESLICEPOSTSHIFTDIFFDETAILID, TIMESLICEPOSTID,
                    STARTDATETIME, ENDDATETIME, STARTDATETIMEUTC, ENDDATETIMEUTC,
                    DURATION, SHIFTDIFFDETAILID, SHIFTDIFFCODE,
                    SHIFTDIFFFACTOR, SHIFTDIFFADDITIONAL, FINALPAYRATE,
                    HASHVALUE, MODIFIEDBY, MODIFIEDON, SHIFTDIFFAFTEROVERTIME
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0), $6::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($7), TRY_TO_TIMESTAMP_NTZ($8),
                        TRY_TO_TIMESTAMP_NTZ($9), TRY_TO_TIMESTAMP_NTZ($10),
                        $11::NUMBER(38,0), $12::NUMBER(38,0),
                        $13::TEXT,
                        $14::NUMBER(18,2), $15::NUMBER(19,4), $16::NUMBER(19,4),
                        $17::TEXT,
                        $18::NUMBER(38,0), TRY_TO_TIMESTAMP_NTZ($19), $20::BOOLEAN
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO TIMESLICEPOSTSHIFTDIFFDETAIL tgt
            USING (
                SELECT * FROM STG_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, TIMESLICEPOSTSHIFTDIFFDETAILID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME           = src.DATABASEPHYSICALNAME
            AND tgt.TIMESLICEPOSTSHIFTDIFFDETAILID = src.TIMESLICEPOSTSHIFTDIFFDETAILID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.TIMESLICEPOSTID        = src.TIMESLICEPOSTID,
                tgt.STARTDATETIME          = src.STARTDATETIME,
                tgt.ENDDATETIME            = src.ENDDATETIME,
                tgt.STARTDATETIMEUTC       = src.STARTDATETIMEUTC,
                tgt.ENDDATETIMEUTC         = src.ENDDATETIMEUTC,
                tgt.DURATION               = src.DURATION,
                tgt.SHIFTDIFFDETAILID      = src.SHIFTDIFFDETAILID,
                tgt.SHIFTDIFFCODE          = src.SHIFTDIFFCODE,
                tgt.SHIFTDIFFFACTOR        = src.SHIFTDIFFFACTOR,
                tgt.SHIFTDIFFADDITIONAL    = src.SHIFTDIFFADDITIONAL,
                tgt.FINALPAYRATE           = src.FINALPAYRATE,
                tgt.HASHVALUE              = src.HASHVALUE,
                tgt.MODIFIEDBY             = src.MODIFIEDBY,
                tgt.MODIFIEDON             = src.MODIFIEDON,
                tgt.SHIFTDIFFAFTEROVERTIME = src.SHIFTDIFFAFTEROVERTIME
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, TIMESLICEPOSTSHIFTDIFFDETAILID,
                TIMESLICEPOSTID, STARTDATETIME, ENDDATETIME,
                STARTDATETIMEUTC, ENDDATETIMEUTC, DURATION,
                SHIFTDIFFDETAILID, SHIFTDIFFCODE,
                SHIFTDIFFFACTOR, SHIFTDIFFADDITIONAL, FINALPAYRATE,
                HASHVALUE, MODIFIEDBY, MODIFIEDON, SHIFTDIFFAFTEROVERTIME
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.TIMESLICEPOSTSHIFTDIFFDETAILID,
                src.TIMESLICEPOSTID, src.STARTDATETIME, src.ENDDATETIME,
                src.STARTDATETIMEUTC, src.ENDDATETIMEUTC, src.DURATION,
                src.SHIFTDIFFDETAILID, src.SHIFTDIFFCODE,
                src.SHIFTDIFFFACTOR, src.SHIFTDIFFADDITIONAL, src.FINALPAYRATE,
                src.HASHVALUE, src.MODIFIEDBY, src.MODIFIEDON, src.SHIFTDIFFAFTEROVERTIME
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
        throw new Error("DELTA_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFO("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''c930ce7d-904e-31a5-156d-559bc63e4246''
            ORDER BY m.last_modified ASC
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
        //         Sparse CSV positional mapping (CDC cols $1-$4 are metadata):
        //           $5=USERID, $6=EMPIDENTIFIER, $44=MODIFIEDON, $48=STARTDATE,
        //           $68=CLIENTID, $70=PAYROLLEMPLOYEEID, $81=WEID, $82=PEID,
        //           $83=PNGSSOUSERGUID, $86=ISSHAREDWORKER, $88=PNGUSERNAME
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_USERINFO (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    USERID, EMPIDENTIFIER, MODIFIEDON, STARTDATE,
                    CLIENTID, PAYROLLEMPLOYEEID, WEID, PEID,
                    PNGSSOUSERGUID, ISSHAREDWORKER, PNGUSERNAME
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::VARCHAR(50),
                        TRY_TO_TIMESTAMP_NTZ($44),
                        TRY_TO_TIMESTAMP_NTZ($52),
                        $68::VARCHAR(50),
                        $70::NUMBER(38,0),
                        $81::VARCHAR(20),
                        $82::VARCHAR(20),
                        $83::VARCHAR(20),
                        $86::BOOLEAN,
                        $88::VARCHAR(25)
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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
                tgt.EMPIDENTIFIER     = src.EMPIDENTIFIER,
                tgt.MODIFIEDON        = src.MODIFIEDON,
                tgt.STARTDATE         = src.STARTDATE,
                tgt.CLIENTID          = src.CLIENTID,
                tgt.PAYROLLEMPLOYEEID = src.PAYROLLEMPLOYEEID,
                tgt.WEID              = src.WEID,
                tgt.PEID              = src.PEID,
                tgt.PNGSSOUSERGUID    = src.PNGSSOUSERGUID,
                tgt.ISSHAREDWORKER    = src.ISSHAREDWORKER,
                tgt.PNGUSERNAME       = src.PNGUSERNAME
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, USERID, EMPIDENTIFIER, MODIFIEDON, STARTDATE,
                CLIENTID, PAYROLLEMPLOYEEID, WEID, PEID, PNGSSOUSERGUID, ISSHAREDWORKER, PNGUSERNAME
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.USERID, src.EMPIDENTIFIER,
                src.MODIFIEDON, src.STARTDATE,
                src.CLIENTID, src.PAYROLLEMPLOYEEID, src.WEID, src.PEID,
                src.PNGSSOUSERGUID, src.ISSHAREDWORKER, src.PNGUSERNAME
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOEMPSTATUS("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''e1b6510c-9ad1-ba04-1c43-1c8345dc44b1''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for USERINFOEMPSTATUS."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_USERINFOEMPSTATUS;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_USERINFOEMPSTATUS (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    USERINFOEMPSTATUSID, USERID, EMPSTATUS,
                    STARTDATETIME, ENDDATETIME, MODIFIEDBY, MODIFIEDON,
                    DESCRIPTION, RETURNTOWORKDATE, INACTIVEEMPDATAPROCESSDATE
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::NUMBER(38,0),
                        $7::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($8),
                        TRY_TO_TIMESTAMP_NTZ($9),
                        $10::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($11),
                        $12::TEXT,
                        TRY_TO_TIMESTAMP_NTZ($13),
                        TRY_TO_TIMESTAMP_NTZ($14)
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO USERINFOEMPSTATUS tgt
            USING (
                SELECT * FROM STG_DELTA_USERINFOEMPSTATUS WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, USERINFOEMPSTATUSID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME  = src.DATABASEPHYSICALNAME
            AND tgt.USERINFOEMPSTATUSID   = src.USERINFOEMPSTATUSID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID                      = src.USERID,
                tgt.EMPSTATUS                   = src.EMPSTATUS,
                tgt.STARTDATETIME               = src.STARTDATETIME,
                tgt.ENDDATETIME                 = src.ENDDATETIME,
                tgt.MODIFIEDBY                  = src.MODIFIEDBY,
                tgt.MODIFIEDON                  = src.MODIFIEDON,
                tgt.DESCRIPTION                 = src.DESCRIPTION,
                tgt.RETURNTOWORKDATE            = src.RETURNTOWORKDATE,
                tgt.INACTIVEEMPDATAPROCESSDATE  = src.INACTIVEEMPDATAPROCESSDATE
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, USERINFOEMPSTATUSID, USERID, EMPSTATUS,
                STARTDATETIME, ENDDATETIME, MODIFIEDBY, MODIFIEDON,
                DESCRIPTION, RETURNTOWORKDATE, INACTIVEEMPDATAPROCESSDATE
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.USERINFOEMPSTATUSID, src.USERID, src.EMPSTATUS,
                src.STARTDATETIME, src.ENDDATETIME, src.MODIFIEDBY, src.MODIFIEDON,
                src.DESCRIPTION, src.RETURNTOWORKDATE, src.INACTIVEEMPDATAPROCESSDATE
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
        throw new Error("DELTA_LOAD_USERINFOEMPSTATUS failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOISSALARY("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts    = 0;
    var total_updates    = 0;
    var total_deletes    = 0;
    var files_processed  = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

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
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''ab18c18c-ccff-62b6-4975-156ffc566ef8''
            ORDER BY m.last_modified ASC
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
                ON_ERROR = CONTINUE
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.DELTA_LOAD_USERINFOPAYROLLMAPPING("STAGE_NAME" VARCHAR, "CSV_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_inserts     = 0;
    var total_updates     = 0;
    var total_deletes     = 0;
    var files_processed   = 0;
    var total_rows_copied = 0;
    var all_audit_rows    = [];

    try {
        // Step 1: Collect all manifest files for this table into a list + metadata map.
        var get_files_query = `
            SELECT
                SUBSTRING(m.full_file_path, POSITION(''/LandingZone/'' IN m.full_file_path)) AS relative_path,
                m.client_id, m.table_id, m.filename, m.full_file_path, m.last_modified
            FROM STAGE_TAA_DELTA_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.CSV_DELTA_STATUS = ''Y''
                AND cc.CSV_SHARD_NO = ` + CSV_SHARD_NO + `
            WHERE m.table_id = ''f1b0a3f6-49a5-a942-2349-e2c4c7fb15fa''
            ORDER BY m.last_modified ASC
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
        if (file_list.length === 0) { return "No delta files found for USERINFOPAYROLLMAPPING."; }

        // Step 2: TRUNCATE staging once before all batches.
        snowflake.createStatement({sqlText: "TRUNCATE TABLE STG_DELTA_USERINFOPAYROLLMAPPING;"}).execute();

        // Step 3: COPY all files into staging in batches of 1000.
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var files_clause = file_list.slice(start, end).join(", ");

            var copy_sql = `
                COPY INTO STG_DELTA_USERINFOPAYROLLMAPPING (
                    CHANGE_TYPE, LSN, DATABASEPHYSICALNAME,
                    USERINFOPAYROLLMAPPINGID, USERID, PAYROLLCLIENTID,
                    PAYROLLEMPLOYEEID, STARTDATETIME, ENDDATETIME, MODIFIEDBY, MODIFIEDON,
                    EMPLOYEESTATUS, WEID, WORKERVERSION
                )
                FROM (
                    SELECT
                        $3::NUMBER(38,0),
                        TO_NUMBER(SUBSTR($1::TEXT, 3), ''XXXXXXXXXXXXXXXXXXXXXXXX''),
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $5::NUMBER(38,0),
                        $6::NUMBER(38,0),
                        $7::VARCHAR(36),
                        $8::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($9),
                        TRY_TO_TIMESTAMP_NTZ($10),
                        $11::NUMBER(38,0),
                        TRY_TO_TIMESTAMP_NTZ($12),
                        $13::NUMBER(38,0),
                        $14::VARCHAR(100),
                        $15::NUMBER(38,0)
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

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_copied += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''" + row.filename + "'', ''" + row.client_id + "'', ''" + row.table_id + "'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''" + row.load_status + "'', " +
                    error_val + ", ''" + row.full_path + "'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        // Step 4: ONE MERGE across all staged rows. LSN dedup guarantees last-value-wins.
        var merge_sql = `
            MERGE INTO USERINFOPAYROLLMAPPING tgt
            USING (
                SELECT * FROM STG_DELTA_USERINFOPAYROLLMAPPING WHERE CHANGE_TYPE IN (1,2,4)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY DATABASEPHYSICALNAME, USERINFOPAYROLLMAPPINGID
                    ORDER BY LSN DESC NULLS LAST
                ) = 1
            ) src
            ON  tgt.DATABASEPHYSICALNAME     = src.DATABASEPHYSICALNAME
            AND tgt.USERINFOPAYROLLMAPPINGID = src.USERINFOPAYROLLMAPPINGID
            WHEN MATCHED AND src.CHANGE_TYPE = 1 THEN DELETE
            WHEN MATCHED AND src.CHANGE_TYPE = 4 THEN UPDATE SET
                tgt.USERID            = src.USERID,
                tgt.PAYROLLCLIENTID   = src.PAYROLLCLIENTID,
                tgt.PAYROLLEMPLOYEEID = src.PAYROLLEMPLOYEEID,
                tgt.STARTDATETIME     = src.STARTDATETIME,
                tgt.ENDDATETIME       = src.ENDDATETIME,
                tgt.MODIFIEDBY        = src.MODIFIEDBY,
                tgt.MODIFIEDON        = src.MODIFIEDON,
                tgt.EMPLOYEESTATUS    = src.EMPLOYEESTATUS,
                tgt.WEID              = src.WEID,
                tgt.WORKERVERSION     = src.WORKERVERSION
            WHEN NOT MATCHED AND src.CHANGE_TYPE IN (2,4) THEN INSERT (
                DATABASEPHYSICALNAME, USERINFOPAYROLLMAPPINGID, USERID,
                PAYROLLCLIENTID, PAYROLLEMPLOYEEID, STARTDATETIME, ENDDATETIME,
                MODIFIEDBY, MODIFIEDON, EMPLOYEESTATUS, WEID, WORKERVERSION
            ) VALUES (
                src.DATABASEPHYSICALNAME, src.USERINFOPAYROLLMAPPINGID, src.USERID,
                src.PAYROLLCLIENTID, src.PAYROLLEMPLOYEEID, src.STARTDATETIME, src.ENDDATETIME,
                src.MODIFIEDBY, src.MODIFIEDON, src.EMPLOYEESTATUS, src.WEID, src.WORKERVERSION
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
        throw new Error("DELTA_LOAD_USERINFOPAYROLLMAPPING failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_DELTA_FINALIZE()
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

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_LAUNCH_DELTA_LOAD("CLIENT_ID_FILTER" VARCHAR DEFAULT null, "TABLE_NAME_FILTER" VARCHAR DEFAULT null, "STAGE_NAME" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var stage_name_safe = (STAGE_NAME !== null && STAGE_NAME !== undefined &&
                               STAGE_NAME.trim() !== "")
            ? STAGE_NAME.trim() : null;

        if (!stage_name_safe) {
            // If no stage passed, check if one is already persisted
            var existing = snowflake.createStatement({
                sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_DELTA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
            }).execute();
            existing.next();
            stage_name_safe = existing.getColumnValue(1) || null;
        }

        if (!stage_name_safe) {
            throw new Error("STAGE_NAME is required on first call. " +
                            "Example: CALL INGEST_TAA_LAUNCH_DELTA_LOAD(NULL, NULL, ''demo.FAB_CF_WS_N1_STG'');");
        }

        var client_val = (CLIENT_ID_FILTER !== null && CLIENT_ID_FILTER !== undefined &&
                          CLIENT_ID_FILTER.trim() !== "")
            ? CLIENT_ID_FILTER.trim() : null;
        var table_val  = (TABLE_NAME_FILTER !== null && TABLE_NAME_FILTER !== undefined &&
                          TABLE_NAME_FILTER.trim() !== "")
            ? TABLE_NAME_FILTER.trim() : null;

        snowflake.createStatement({sqlText:
            "MERGE INTO INGEST_TAA_DELTA_RUN_CONFIG tgt " +
            "USING (SELECT * FROM VALUES " +
            "  (''STAGE_NAME'',        " + (stage_name_safe ? "''" + stage_name_safe + "''" : "NULL") + "), " +
            "  (''CLIENT_ID_FILTER'',  " + (client_val ? "''" + client_val + "''" : "NULL")  + "), " +
            "  (''TABLE_NAME_FILTER'', " + (table_val  ? "''" + table_val  + "''" : "NULL")  + ") " +
            "AS src(PARAM_NAME, PARAM_VALUE)) src ON tgt.PARAM_NAME = src.PARAM_NAME " +
            "WHEN MATCHED THEN UPDATE SET " +
            "  tgt.PARAM_VALUE = src.PARAM_VALUE, " +
            "  tgt.UPDATED_AT  = CURRENT_TIMESTAMP();"
        }).execute();

        snowflake.createStatement({sqlText: "EXECUTE TASK TAA_DELTA_ROOT;"}).execute();

        var scope = client_val ? " (client: " + client_val + ")" : " (all clients)";
        return "Delta Task DAG triggered" + scope + ".\\n" +
               "Stage: " + stage_name_safe + "\\n" +
               "STAGE_NAME persists in INGEST_TAA_DELTA_RUN_CONFIG for future scheduled runs.\\n" +
               "\\nMonitor progress:\\n" +
               "  SELECT * FROM TABLE(TASK_DEPENDENTS(''TAA_DELTA_ROOT'', TRUE)) ORDER BY SCHEDULED_TIME;\\n" +
               "\\nView history:\\n" +
               "  SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME\\n" +
               "  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => ''TAA_DELTA_ROOT'', RESULT_LIMIT => 10))\\n" +
               "  ORDER BY SCHEDULED_TIME DESC;";
    } catch (err) {
        throw new Error("INGEST_TAA_LAUNCH_DELTA_LOAD failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_FULL_LOAD_PREPARE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var result_message = "";

    try {
        var cfg_result = snowflake.createStatement({sqlText: `
            SELECT PARAM_NAME, PARAM_VALUE
            FROM INGEST_TAA_RUN_CONFIG
            WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')
        `}).execute();

        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }

        var stage_name_safe   = config["STAGE_NAME"]        || null;
        var client_filter     = config["CLIENT_ID_FILTER"]  || null;
        var table_name_filter = config["TABLE_NAME_FILTER"] || null;

        if (!stage_name_safe) {
            // No parameters set -- this is a scheduled auto-run with no pending job.
            // Return silently so the task shows SUCCEEDED rather than FAILED.
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_RUN_CONFIG. " +
                   "Trigger via INGEST_TAA_LAUNCH_FULL_LOAD to run a real load.";
        }

        var is_client_scoped  = (client_filter !== null && client_filter.trim() !== "");
        var client_id_in_list = null;
        if (is_client_scoped) {
            var raw_ids = client_filter.trim().split(",");
            var quoted  = [];
            for (var ci = 0; ci < raw_ids.length; ci++) {
                var cid = raw_ids[ci].trim();
                if (cid.length > 0) { quoted.push("''" + cid + "''"); }
            }
            client_id_in_list = quoted.join(", ");
        }

        var table_filter_map = null;
        if (table_name_filter && table_name_filter.trim() !== "") {
            table_filter_map = {};
            var parts = table_name_filter.toUpperCase().split(",");
            for (var i = 0; i < parts.length; i++) {
                var t = parts[i].trim();
                if (t.length > 0) { table_filter_map[t] = true; }
            }
        }


        result_message += "=== INGEST_TAA_FULL_LOAD_PREPARE ===\\n";
        result_message += "Client scope : " + (is_client_scoped ? client_filter : "ALL CLIENTS") + "\\n";
        result_message += "Stage        : " + stage_name_safe + "\\n";

        // ------------------------------------------------------------------
        // Build the file manifest FIRST so we know exactly which
        // client/table combinations have files before touching target tables.
        // ------------------------------------------------------------------
        result_message += "\\n=== BUILDING FILE MANIFEST ===\\n";
        var file_list_param  = is_client_scoped  ? "''" + client_filter     + "''" : "NULL";
        var table_list_param = table_name_filter ? "''" + table_name_filter + "''" : "NULL";
        var manifest_sql = "CALL BUILD_STAGE_TAA_FULL_FILE_MANIFEST(" +
                           file_list_param + ", " + table_list_param + ", ''" + stage_name_safe + "'');";
        var manifest_result = snowflake.createStatement({sqlText: manifest_sql}).execute();
        manifest_result.next();
        result_message += "  " + manifest_result.getColumnValue(1) + "\\n";

        // ------------------------------------------------------------------
        // Clear target tables -- but ONLY for client/table combinations that
        // actually have files in the manifest just built.  This prevents data
        // loss when a client-scoped run finds no new files for a given table
        // (e.g. all files were already loaded per the audit log).
        // ------------------------------------------------------------------
        result_message += "\\n=== CLEARING TARGET TABLES ===\\n";

        var ctrl = snowflake.createStatement({sqlText: `
            SELECT TABLE_NAME, IS_MULTI_TENANT, SOURCE_TABLE_ID
            FROM INGEST_TAA_TABLE_CONFIG
            WHERE IS_ACTIVE_FULL_LOAD = TRUE
            ORDER BY LOAD_ORDER, TABLE_NAME
        `}).execute();

        while (ctrl.next()) {
            var tbl_name        = ctrl.getColumnValue(1);
            var is_multi_tenant = ctrl.getColumnValue(2);
            var source_table_id = ctrl.getColumnValue(3);

            if (table_filter_map !== null &&
                !table_filter_map[tbl_name.toUpperCase()]) { continue; }

            if (is_client_scoped && !is_multi_tenant) {
                result_message += "  SKIPPED (not multi-tenant): " + tbl_name + "\\n";
                continue;
            }

            // Check whether the manifest has any files for this table
            // (and for the specific clients in scope, if client-scoped).
            var manifest_check_sql;
            if (is_client_scoped) {
                manifest_check_sql =
                    "SELECT COUNT(*) FROM STAGE_TAA_FULL_FILE_MANIFEST " +
                    "WHERE UPPER(table_id) = UPPER(''" + source_table_id + "'') " +
                    "AND client_id IN (" + client_id_in_list + ")";
            } else {
                manifest_check_sql =
                    "SELECT COUNT(*) FROM STAGE_TAA_FULL_FILE_MANIFEST " +
                    "WHERE UPPER(table_id) = UPPER(''" + source_table_id + "'')";
            }
            var check_result = snowflake.createStatement({sqlText: manifest_check_sql}).execute();
            check_result.next();
            var manifest_rows = check_result.getColumnValue(1);

            if (manifest_rows === 0) {
                result_message += "  SKIPPED (no files in manifest): " + tbl_name + "\\n";
                continue;
            }

            var clear_sql;
            if (is_multi_tenant) {
                // Multi-tenant: DELETE only specific clients in scope
                if (is_client_scoped) {
                    clear_sql = "DELETE FROM " + tbl_name +
                                " WHERE DATABASEPHYSICALNAME IN (" + client_id_in_list + ");";
                } else {
                    // Non-client scoped: delete only clients with files in manifest
                    clear_sql = "DELETE FROM " + tbl_name +
                                " WHERE DATABASEPHYSICALNAME IN (" +
                                "  SELECT DISTINCT client_id FROM STAGE_TAA_FULL_FILE_MANIFEST " +
                                "  WHERE UPPER(table_id) = UPPER(''" + source_table_id + "'')" +
                                ");";
                }
            } else {
                // Non-multi-tenant: TRUNCATE entirely
                clear_sql = "TRUNCATE TABLE " + tbl_name + ";";
            }
            snowflake.createStatement({sqlText: clear_sql}).execute();
            result_message += "  Cleared: " + tbl_name + "\\n";
        }

        result_message += "\\nPREPARE COMPLETE -- Wave 1 tasks will now start.\\n";

        return result_message;

    } catch (err) {
        throw new Error("INGEST_TAA_FULL_LOAD_PREPARE failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_FROM_CONFIG("TABLE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var cfg = snowflake.createStatement({
            sqlText: "SELECT PARAM_VALUE FROM INGEST_TAA_RUN_CONFIG WHERE PARAM_NAME = ''STAGE_NAME''"
        }).execute();
        cfg.next();
        var stage = cfg.getColumnValue(1);
        if (!stage) {
            return "SKIPPED: No STAGE_NAME configured in INGEST_TAA_RUN_CONFIG.";
        }
        var call_sql = "CALL FULL_LOAD_" + TABLE_NAME + "(''" + stage + "'', ''" + PARQUET_SHARD_NO + "'')";
        var result = snowflake.createStatement({sqlText: call_sql}).execute();
        result.next();
        return result.getColumnValue(1);
    } catch (err) {
        throw new Error("FULL_LOAD_FROM_CONFIG(" + TABLE_NAME + ", " + PARQUET_SHARD_NO + ") failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_CUSTOMER("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''bf376338-3aaf-4306-9885-db20b386631c''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO CUSTOMER (
                    CustomerID,
                    CustomerName,
                    DatabaseCreationDate,
                    CustomerAlias,
                    CustomerStatus,
                    BrandID,
                    DatabasePhysicalName,
                    DatabaseServer,
                    SupportEmailAddress,
                    IVRAlias,
                    ActiveEmployees,
                    EmailServer,
                    EmailPort,
                    EmailSSLEnabled,
                    EmailAccount,
                    EmailUsername,
                    EmailPassword,
                    EmailDomain,
                    EmailSettingsOverride,
                    QueueDelayUntil,
                    ModifiedBy,
                    ModifiedOn,
                    WirelessEnabled,
                    FingerprintEnabled,
                    CustomerIDExternal,
                    WSTraceEnabled,
                    WSTimeStarted,
                    WSStartedBy,
                    TelepunchAlias,
                    BISClientID,
                    CustomerLastActivatedBy,
                    CustomerLastActivatedOn,
                    CustomerLastDeactivatedBy,
                    CustomerLastDeactivatedOn,
                    IsProxy,
                    MigWorkflow,
                    IsEssentials,
                    IsC2C,
                    DoNotDelete,
                    RollupMultiFEINSharedEmployee,
                    CustomerCreationStatusType,
                    EnableAutoClosingTimeCard,
                    EnterpriseCAIDBill,
                    CEIDBill,
                    PayrollClientIDBill,
                    ClientType
                )
                FROM (
                    SELECT
                    $1:CustomerID::NUMBER(38,0),
                    $1:CustomerName::TEXT,
                    $1:DatabaseCreationDate::TIMESTAMP_NTZ,
                    $1:CustomerAlias::TEXT,
                    $1:CustomerStatus::NUMBER(38,0),
                    $1:BrandID::NUMBER(38,0),
                    $1:DatabasePhysicalName::TEXT,
                    $1:DatabaseServer::TEXT,
                    $1:SupportEmailAddress::TEXT,
                    $1:IVRAlias::NUMBER(38,0),
                    $1:ActiveEmployees::NUMBER(38,0),
                    $1:EmailServer::TEXT,
                    $1:EmailPort::NUMBER(38,0),
                    $1:EmailSSLEnabled::BOOLEAN,
                    $1:EmailAccount::TEXT,
                    $1:EmailUsername::TEXT,
                    $1:EmailPassword::TEXT,
                    $1:EmailDomain::TEXT,
                    $1:EmailSettingsOverride::BOOLEAN,
                    $1:QueueDelayUntil::TIMESTAMP_NTZ,
                    $1:ModifiedBy::NUMBER(38,0),
                    $1:ModifiedOn::TIMESTAMP_NTZ,
                    $1:WirelessEnabled::BOOLEAN,
                    $1:FingerprintEnabled::BOOLEAN,
                    $1:CustomerIDExternal::TEXT,
                    $1:WSTraceEnabled::BOOLEAN,
                    $1:WSTimeStarted::TIMESTAMP_NTZ,
                    $1:WSStartedBy::NUMBER(38,0),
                    $1:TelepunchAlias::NUMBER(38,0),
                    $1:BISClientID::TEXT,
                    $1:CustomerLastActivatedBy::NUMBER(38,0),
                    $1:CustomerLastActivatedOn::TIMESTAMP_NTZ,
                    $1:CustomerLastDeactivatedBy::NUMBER(38,0),
                    $1:CustomerLastDeactivatedOn::TIMESTAMP_NTZ,
                    $1:IsProxy::BOOLEAN,
                    $1:MigWorkflow::NUMBER(38,0),
                    $1:IsEssentials::BOOLEAN,
                    $1:IsC2C::BOOLEAN,
                    $1:DoNotDelete::BOOLEAN,
                    $1:RollupMultiFEINSharedEmployee::NUMBER(38,0),
                    $1:CustomerCreationStatusType::NUMBER(38,0),
                    $1:EnableAutoClosingTimeCard::BOOLEAN,
                    $1:EnterpriseCAIDBill::TEXT,
                    $1:CEIDBill::TEXT,
                    $1:PayrollClientIDBill::TEXT,
                    $1:ClientType::NUMBER(38,0)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Accumulate audit records instead of inserting individually
            if (!all_audit_rows) all_audit_rows = [];
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);
                var status = copy_result.getColumnValue(2);
                var rows_loaded = copy_result.getColumnValue(4);
                var first_error = copy_result.getColumnValue(7);
                
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_ENTERPRISECUSTOMER("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''49f4e893-dbbd-280a-93b3-9edccba30424''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO ENTERPRISECUSTOMER (
                    EnterpriseCustomerID,
                    CustomerID,
                    PNGSSOCAID,
                    CEID,
                    PayrollClientID,
                    ModifiedBy,
                    ModifiedOn,
                    StratusTimeCAID,
                    LegalClientName,
                    CEIDStatus,
                    CEIDStatusDate,
                    ModifiedChangeReason,
                    CEIDSupersededBy,
                    CACAID,
                    HRISCAID,
                    BISClientID,
                    UsedClientMaint
                )
                FROM (
                    SELECT
                        $1:EnterpriseCustomerID::NUMBER(38,0),
                        $1:CustomerID::NUMBER(38,0),
                        $1:PNGSSOCAID::TEXT,
                        $1:CEID::TEXT,
                        $1:PayrollClientID::TEXT,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:StratusTimeCAID::TEXT,
                        $1:LegalClientName::TEXT,
                        $1:CEIDStatus::TEXT,
                        $1:CEIDStatusDate::TIMESTAMP_NTZ,
                        $1:ModifiedChangeReason::TEXT,
                        $1:CEIDSupersededBy::TEXT,
                        $1:CACAID::TEXT,
                        $1:HRISCAID::TEXT,
                        $1:BISClientID::TEXT,
                        $1:UsedClientMaint::BOOLEAN
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                            snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_LLDETAIL("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''46c059a2-1b66-97a0-6dbc-4b1bf1ca4219''
            ORDER BY m.client_id, m.table_id
        `;

        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
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

        total_files_identified = file_list.length;

        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var batch_files  = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");

            var copy_command = `
                COPY INTO LLDETAIL (
                    DATABASEPHYSICALNAME,
                    LLDETAILID,
                    LLID,
                    LLDETAILCODE,
                    LLDETAILNAME,
                    STARTDATE,
                    ENDDATE,
                    MODIFIEDBY,
                    MODIFIEDON,
                    ISDELETED,
                    EMPNOTESREQUIRED,
                    CREATEDON,
                    CREATEDBY,
                    PAYROLLUNIQUEID,
                    ORIGINALCODE,
                    CASTARTDATE,
                    CAENDDATE,
                    PAYROLLCLIENTID,
                    COLORCODE
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:LLDetailID::NUMBER(38,0),
                        $1:LLID::NUMBER(38,0),
                        $1:LLDetailCode::VARCHAR(300),
                        $1:LLDetailName::VARCHAR(300),
                        $1:StartDate::TIMESTAMP_NTZ,
                        $1:EndDate::TIMESTAMP_NTZ,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:IsDeleted::BOOLEAN,
                        $1:EmpNotesRequired::BOOLEAN,
                        $1:CreatedOn::TIMESTAMP_NTZ,
                        $1:CreatedBy::NUMBER(38,0),
                        $1:PayrollUniqueID::NUMBER(38,0),
                        $1:OriginalCode::VARCHAR(300),
                        $1:CAStartDate::TIMESTAMP_NTZ,
                        $1:CAEndDate::TIMESTAMP_NTZ,
                        $1:PayrollClientID::VARCHAR(36),
                        $1:ColorCode::VARCHAR(7)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            while (copy_result.next()) {
                var file_name   = copy_result.getColumnValue(1);
                var status      = copy_result.getColumnValue(2);
                var rows_loaded = copy_result.getColumnValue(4);
                var first_error = copy_result.getColumnValue(7);

                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;

                var metadata    = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";

                var safe_filename  = (metadata.filename       || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id      || "UNKNOWN");
                var safe_table_id  = (metadata.table_id       || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error     = first_error ? first_error.replace(/''/g, "''''") : null;

                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_PAYTYPE("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''f774054a-9744-5cbf-731e-1bdd7df870f7''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO PAYTYPE (
                    DATABASEPHYSICALNAME,
                    ID,
                    PayTypeID,
                    StartDateTime,
                    EndDateTime,
                    IsDeleted,
                    PayTypeName,
                    PayTypeCode,
                    CountTowardsHolidayMin,
                    OverridesAbsence,
                    IsWorkType,
                    ApplyToOvertime,
                    IncludeInBlendedRate,
                    ApplyToTimeOff,
                    CanBeOvertime,
                    DefaultPayLevelRateType,
                    IsOvertimeType,
                    OvertimeFactor,
                    ColorCode,
                    ModifiedBy,
                    ModifiedOn,
                    AllowShiftDiff,
                    DeductFromPayType,
                    DeductFromPayTypeID,
                    IsDurationOnly,
                    IsLLRequired,
                    IsFMLAType,
                    PayAtWeightedRate
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:ID::NUMBER(38,0),
                        $1:PayTypeID::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:IsDeleted::BOOLEAN,
                        $1:PayTypeName::TEXT,
                        $1:PayTypeCode::TEXT,
                        $1:CountTowardsHolidayMin::BOOLEAN,
                        $1:OverridesAbsence::BOOLEAN,
                        $1:IsWorkType::BOOLEAN,
                        $1:ApplyToOvertime::BOOLEAN,
                        $1:IncludeInBlendedRate::BOOLEAN,
                        $1:ApplyToTimeOff::BOOLEAN,
                        $1:CanBeOvertime::BOOLEAN,
                        $1:DefaultPayLevelRateType::NUMBER(38,0),
                        $1:IsOvertimeType::BOOLEAN,
                        $1:OvertimeFactor::NUMBER(18,2),
                        $1:ColorCode::TEXT,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:AllowShiftDiff::BOOLEAN,
                        $1:DeductFromPayType::BOOLEAN,
                        $1:DeductFromPayTypeID::NUMBER(38,0),
                        $1:IsDurationOnly::BOOLEAN,
                        $1:IsLLRequired::BOOLEAN,
                        $1:IsFMLAType::BOOLEAN,
                        $1:PayAtWeightedRate::BOOLEAN
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_SCHEDULE("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''f4830a1d-ae29-8044-7c71-6bd4b5779b70''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO SCHEDULE (
                    DATABASEPHYSICALNAME,
                    ScheduleID,
                    UserID,
                    PayTypeID,
                    StartDateTime,
                    EndDateTime,
                    LLDetailID1,
                    LLDetailID2,
                    LLDetailID3,
                    LLDetailID4,
                    LLDetailID5,
                    LLDetailID6,
                    LLDetailID7,
                    LLDetailID8,
                    LLDetailID9,
                    LLDetailID10,
                    LLDetailID11,
                    LLDetailID12,
                    LLDetailID13,
                    LLDetailID14,
                    LLDetailID15,
                    IsAutoGenerated,
                    ModifiedBy,
                    ModifiedOn,
                    AdvScheduleCapacityDetailID,
                    Note,
                    StartDateTimeUtc,
                    EndDateTimeUtc,
                    CalendarEventID,
                    IsCalendarSync,
                    ScheduleGeneratedSource,
                    UserHasBeenNotified
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:ScheduleID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:PayTypeID::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:LLDetailID1::NUMBER(38,0),
                        $1:LLDetailID2::NUMBER(38,0),
                        $1:LLDetailID3::NUMBER(38,0),
                        $1:LLDetailID4::NUMBER(38,0),
                        $1:LLDetailID5::NUMBER(38,0),
                        $1:LLDetailID6::NUMBER(38,0),
                        $1:LLDetailID7::NUMBER(38,0),
                        $1:LLDetailID8::NUMBER(38,0),
                        $1:LLDetailID9::NUMBER(38,0),
                        $1:LLDetailID10::NUMBER(38,0),
                        $1:LLDetailID11::NUMBER(38,0),
                        $1:LLDetailID12::NUMBER(38,0),
                        $1:LLDetailID13::NUMBER(38,0),
                        $1:LLDetailID14::NUMBER(38,0),
                        $1:LLDetailID15::NUMBER(38,0),
                        $1:IsAutoGenerated::BOOLEAN,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:AdvScheduleCapacityDetailID::NUMBER(38,0),
                        $1:Note::TEXT,
                        $1:StartDateTimeUtc::TIMESTAMP_NTZ,
                        $1:EndDateTimeUtc::TIMESTAMP_NTZ,
                        $1:CalendarEventID::TEXT,
                        $1:IsCalendarSync::BOOLEAN,
                        $1:ScheduleGeneratedSource::NUMBER(38,0),
                        $1:UserHasBeenNotified::BOOLEAN
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFDATA("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''99629826-fe8e-61a4-0371-e3b33791fd23''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMEOFFDATA (
                    DATABASEPHYSICALNAME,
                    TimeOffDataID,
                    UserID,
                    PayTypeID,
                    AccruedSecs,
                    GrantedSecs,
                    ManSecs,
                    UsedSecs,
                    AvailableSecs,
                    ApplyToDateTime,
                    AdjustmentUserID,
                    IsSystemGenerated,
                    Notes,
                    TimeSlicePreID,
                    CreationDateTime,
                    ModifiedBy,
                    ModifiedOn,
                    MakeUpSecs,
                    AnchorPoint,
                    RolloverSecs,
                    ForfeitedSecs,
                    TransferInID,
                    TransferInSecs,
                    TransferOutID,
                    TransferOutSecs,
                    TotalAccruedSecs,
                    ManType,
                    IsRollover,
                    ProcessIndex,
                    DelayedGrantSecs,
                    SecondsWorkedStore,
                    ExpiresDateTime,
                    RolloverTransferInID,
                    RolloverTransferInSecs,
                    RolloverTransferOutID,
                    RolloverTransferOutSecs,
                    LastProcessedEventId,
                    LastProcessedEventDateTime
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeOffDataID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:PayTypeID::NUMBER(38,0),
                        $1:AccruedSecs::NUMBER(38,0),
                        $1:GrantedSecs::NUMBER(38,0),
                        $1:ManSecs::NUMBER(38,0),
                        $1:UsedSecs::NUMBER(38,0),
                        $1:AvailableSecs::NUMBER(38,0),
                        $1:ApplyToDateTime::TIMESTAMP_NTZ,
                        $1:AdjustmentUserID::NUMBER(38,0),
                        $1:IsSystemGenerated::BOOLEAN,
                        $1:Notes::TEXT,
                        $1:TimeSlicePreID::NUMBER(38,0),
                        $1:CreationDateTime::TIMESTAMP_NTZ,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:MakeUpSecs::NUMBER(38,0),
                        $1:AnchorPoint::BOOLEAN,
                        $1:RolloverSecs::NUMBER(38,0),
                        $1:ForfeitedSecs::NUMBER(38,0),
                        $1:TransferInID::NUMBER(38,0),
                        $1:TransferInSecs::NUMBER(38,0),
                        $1:TransferOutID::NUMBER(38,0),
                        $1:TransferOutSecs::NUMBER(38,0),
                        $1:TotalAccruedSecs::NUMBER(38,0),
                        $1:ManType::NUMBER(38,0),
                        $1:IsRollover::BOOLEAN,
                        $1:ProcessIndex::NUMBER(38,0),
                        $1:DelayedGrantSecs::NUMBER(38,0),
                        $1:SecondsWorkedStore::NUMBER(38,0),
                        $1:ExpiresDateTime::TIMESTAMP_NTZ,
                        $1:RolloverTransferInID::NUMBER(38,0),
                        $1:RolloverTransferInSecs::NUMBER(38,0),
                        $1:RolloverTransferOutID::NUMBER(38,0),
                        $1:RolloverTransferOutSecs::NUMBER(38,0),
                        $1:LastProcessedEventId::TEXT,
                        $1:LastProcessedEventDateTime::TIMESTAMP_NTZ
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFREQUEST("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''db78652a-b192-ed5c-b7fd-410e8e8eb47a''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMEOFFREQUEST (
                    DATABASEPHYSICALNAME,
                    TimeOffRequestID,
                    UserID,
                    PayTypeID,
                    TimeOffPolicyDetailID,
                    DateTimeSubmitted,
                    StartDateTime,
                    EndDateTime,
                    IncludeWeekends,
                    DurationPerDaySecs,
                    StatusType,
                    StatusChangedOn,
                    EmpNotes,
                    IsBuyoutRequest,
                    BuyoutSecs,
                    BuyoutAdjustmentID,
                    PayAdjustmentDataID
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeOffRequestID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:PayTypeID::NUMBER(38,0),
                        $1:TimeOffPolicyDetailID::NUMBER(38,0),
                        $1:DateTimeSubmitted::TIMESTAMP_NTZ,
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:IncludeWeekends::BOOLEAN,
                        $1:DurationPerDaySecs::NUMBER(38,0),
                        $1:StatusType::NUMBER(38,0),
                        $1:StatusChangedOn::TIMESTAMP_NTZ,
                        $1:EmpNotes::TEXT,
                        $1:IsBuyoutRequest::BOOLEAN,
                        $1:BuyoutSecs::NUMBER(38,0),
                        $1:BuyoutAdjustmentID::NUMBER(38,0),
                        $1:PayAdjustmentDataID::NUMBER(38,0)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMEOFFREQUESTDETAIL("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''f9e8cf07-8d4f-1c51-df47-da7de058a176''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMEOFFREQUESTDETAIL (
                    DATABASEPHYSICALNAME,
                    TimeOffRequestDetailID,
                    TimeOffRequestID,
                    StartDateTime,
                    EndDateTime,
                    StatusType,
                    IsDeleted,
                    StatusChangedBy,
                    StatusChangedOn,
                    MgrNotes,
                    TimeSlicePreID,
                    AutoResetQualifyByHoursWorked,
                    IsCalendarSync,
                    CalendarEventID
                )
                FROM (
                    SELECT
REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeOffRequestDetailID::NUMBER(38,0),
                        $1:TimeOffRequestID::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:StatusType::NUMBER(38,0),
                        $1:IsDeleted::BOOLEAN,
                        $1:StatusChangedBy::NUMBER(38,0),
                        $1:StatusChangedOn::TIMESTAMP_NTZ,
                        $1:MgrNotes::TEXT,
                        $1:TimeSlicePreID::NUMBER(38,0),
                        $1:AutoResetQualifyByHoursWorked::BOOLEAN,
                        $1:IsCalendarSync::BOOLEAN,
                        $1:CalendarEventID::TEXT
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOST("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''0b30f4a8-bf11-0296-664d-a6996e0dca32''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMESLICEPOST (
                    DATABASEPHYSICALNAME,
                    TimeSlicePostID,
                    UserID,
                    PayTypeID,
                    TimeSlicePreIDIn,
                    TimeSlicePreIDOut,
                    ActualDateTimeIn,
                    ActualDateTimeOut,
                    RoundedDateTimeIn,
                    RoundedDateTimeOut,
                    UTCDateTimeIn,
                    UTCDateTimeOut,
                    TotalPaidDurationSecs,
                    RegDurationSecs,
                    OTDurationSecs,
                    UnpaidDurationSecs,
                    MgrApprovedIn,
                    MgrApprovedOut,
                    MgrNoteIn,
                    MgrNoteOut,
                    EmpApprovedIn,
                    EmpApprovedOut,
                    EmpNoteIn,
                    EmpNoteOut,
                    TimeSheetSubmissionIn,
                    TimeSheetSubmissionOut,
                    PayRate,
                    ChargeRate,
                    TotalEarnings,
                    MissingPunchTypeIn,
                    MissingPunchTypeOut,
                    IsModifiedIn,
                    IsModifiedOut,
                    ScheduleID,
                    ScheduleDetailID,
                    ApplyToDate,
                    ClosedType,
                    TimeSliceGroupID,
                    LLDetailID1,
                    LLDetailID2,
                    LLDetailID3,
                    LLDetailID4,
                    LLDetailID5,
                    LLDetailID6,
                    LLDetailID7,
                    LLDetailID8,
                    LLDetailID9,
                    LLDetailID10,
                    LLDetailID11,
                    LLDetailID12,
                    LLDetailID13,
                    LLDetailID14,
                    LLDetailID15,
                    HashValue,
                    TransactionTypeIn,
                    TransactionTypeOut,
                    TransactionSourceIn,
                    TransactionSourceOut,
                    ApplyToOvertime,
                    PayLevelRateType,
                    HasModifier,
                    IsCanceled,
                    CountTowardsHolidayMin,
                    HasShiftDiff,
                    IsMealPremium,
                    ModifiedBy,
                    ModifiedOn,
                    AdminApprovedIn,
                    AdminApprovedOut,
                    Mgr2ApprovedIn,
                    Mgr2ApprovedOut,
                    LongitudeIn,
                    LongitudeOut,
                    LatitudeIn,
                    LatitudeOut,
                    IsCompTime,
                    CompTimeRequestID,
                    CompTimeOTConvertedSecs,
                    MgrApprovedByIn,
                    MgrApprovedByOut,
                    Mgr2ApprovedByIn,
                    Mgr2ApprovedByOut,
                    AdminApprovedByIn,
                    AdminApprovedByOut,
                    IsForecast,
                    IsReconcile,
                    IsSwipeAndGoIn,
                    IsSwipeAndGoOut,
                    PopulatedFromScheduleIn,
                    PopulatedFromScheduleOut,
                    IsCallBack,
                    AccuracyIn,
                    AccuracyOut,
                    IsBreakPremium,
                    AdditionalPremiumType
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeSlicePostID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:PayTypeID::NUMBER(38,0),
                        $1:TimeSlicePreIDIn::NUMBER(38,0),
                        $1:TimeSlicePreIDOut::NUMBER(38,0),
                        $1:ActualDateTimeIn::TIMESTAMP_NTZ,
                        $1:ActualDateTimeOut::TIMESTAMP_NTZ,
                        $1:RoundedDateTimeIn::TIMESTAMP_NTZ,
                        $1:RoundedDateTimeOut::TIMESTAMP_NTZ,
                        $1:UTCDateTimeIn::TIMESTAMP_NTZ,
                        $1:UTCDateTimeOut::TIMESTAMP_NTZ,
                        $1:TotalPaidDurationSecs::NUMBER(38,0),
                        $1:RegDurationSecs::NUMBER(38,0),
                        $1:OTDurationSecs::NUMBER(38,0),
                        $1:UnpaidDurationSecs::NUMBER(38,0),
                        $1:MgrApprovedIn::BOOLEAN,
                        $1:MgrApprovedOut::BOOLEAN,
                        $1:MgrNoteIn::TEXT,
                        $1:MgrNoteOut::TEXT,
                        $1:EmpApprovedIn::BOOLEAN,
                        $1:EmpApprovedOut::BOOLEAN,
                        $1:EmpNoteIn::TEXT,
                        $1:EmpNoteOut::TEXT,
                        $1:TimeSheetSubmissionIn::BOOLEAN,
                        $1:TimeSheetSubmissionOut::BOOLEAN,
                        $1:PayRate::NUMBER(19,4),
                        $1:ChargeRate::NUMBER(19,4),
                        $1:TotalEarnings::NUMBER(19,4),
                        $1:MissingPunchTypeIn::NUMBER(38,0),
                        $1:MissingPunchTypeOut::NUMBER(38,0),
                        $1:IsModifiedIn::BOOLEAN,
                        $1:IsModifiedOut::BOOLEAN,
                        $1:ScheduleID::NUMBER(38,0),
                        $1:ScheduleDetailID::NUMBER(38,0),
                        $1:ApplyToDate::TIMESTAMP_NTZ,
                        $1:ClosedType::NUMBER(38,0),
                        $1:TimeSliceGroupID::TEXT,
                        $1:LLDetailID1::NUMBER(38,0),
                        $1:LLDetailID2::NUMBER(38,0),
                        $1:LLDetailID3::NUMBER(38,0),
                        $1:LLDetailID4::NUMBER(38,0),
                        $1:LLDetailID5::NUMBER(38,0),
                        $1:LLDetailID6::NUMBER(38,0),
                        $1:LLDetailID7::NUMBER(38,0),
                        $1:LLDetailID8::NUMBER(38,0),
                        $1:LLDetailID9::NUMBER(38,0),
                        $1:LLDetailID10::NUMBER(38,0),
                        $1:LLDetailID11::NUMBER(38,0),
                        $1:LLDetailID12::NUMBER(38,0),
                        $1:LLDetailID13::NUMBER(38,0),
                        $1:LLDetailID14::NUMBER(38,0),
                        $1:LLDetailID15::NUMBER(38,0),
                        $1:HashValue::TEXT,
                        $1:TransactionTypeIn::NUMBER(38,0),
                        $1:TransactionTypeOut::NUMBER(38,0),
                        $1:TransactionSourceIn::NUMBER(38,0),
                        $1:TransactionSourceOut::NUMBER(38,0),
                        $1:ApplyToOvertime::BOOLEAN,
                        $1:PayLevelRateType::NUMBER(38,0),
                        $1:HasModifier::BOOLEAN,
                        $1:IsCanceled::BOOLEAN,
                        $1:CountTowardsHolidayMin::BOOLEAN,
                        $1:HasShiftDiff::BOOLEAN,
                        $1:IsMealPremium::BOOLEAN,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:AdminApprovedIn::BOOLEAN,
                        $1:AdminApprovedOut::BOOLEAN,
                        $1:Mgr2ApprovedIn::BOOLEAN,
                        $1:Mgr2ApprovedOut::BOOLEAN,
                        $1:LongitudeIn::NUMBER(18,4),
                        $1:LongitudeOut::NUMBER(18,4),
                        $1:LatitudeIn::NUMBER(18,4),
                        $1:LatitudeOut::NUMBER(18,4),
                        $1:IsCompTime::BOOLEAN,
                        $1:CompTimeRequestID::NUMBER(38,0),
                        $1:CompTimeOTConvertedSecs::NUMBER(38,0),
                        $1:MgrApprovedByIn::NUMBER(38,0),
                        $1:MgrApprovedByOut::NUMBER(38,0),
                        $1:Mgr2ApprovedByIn::NUMBER(38,0),
                        $1:Mgr2ApprovedByOut::NUMBER(38,0),
                        $1:AdminApprovedByIn::NUMBER(38,0),
                        $1:AdminApprovedByOut::NUMBER(38,0),
                        $1:IsForecast::BOOLEAN,
                        $1:IsReconcile::BOOLEAN,
                        $1:IsSwipeAndGoIn::BOOLEAN,
                        $1:IsSwipeAndGoOut::BOOLEAN,
                        $1:PopulatedFromScheduleIn::BOOLEAN,
                        $1:PopulatedFromScheduleOut::BOOLEAN,
                        $1:IsCallBack::BOOLEAN,
                        $1:AccuracyIn::NUMBER(18,4),
                        $1:AccuracyOut::NUMBER(18,4),
                        $1:IsBreakPremium::BOOLEAN,
                        $1:AdditionalPremiumType::NUMBER(38,0)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOSTEXCEPTIONDETAIL("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''be6c4966-d75e-ef52-7460-75c736afbf26''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMESLICEPOSTEXCEPTIONDETAIL (
                    DATABASEPHYSICALNAME,
                    TimeSlicePostExceptionDetailID,
                    UserID,
                    TimeSlicePostID,
                    TimeSlicePreID,
                    ScheduleID,
                    DateTime,
                    ExceptionPolicyRuleID,
                    ExceptionType,
                    TransactionType,
                    ExceptionParameterSecs,
                    HashValue,
                    IsAcknowledged,
                    ModifiedBy,
                    ModifiedOn,
                    MgrNote,
                    AcknowledgedBy
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeSlicePostExceptionDetailID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:TimeSlicePostID::NUMBER(38,0),
                        $1:TimeSlicePreID::NUMBER(38,0),
                        $1:ScheduleID::NUMBER(38,0),
                        $1:DateTime::TIMESTAMP_NTZ,
                        $1:ExceptionPolicyRuleID::NUMBER(38,0),
                        $1:ExceptionType::NUMBER(38,0),
                        $1:TransactionType::NUMBER(38,0),
                        $1:ExceptionParameterSecs::NUMBER(38,0),
                        $1:HashValue::TEXT,
                        $1:IsAcknowledged::BOOLEAN,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:MgrNote::TEXT,
                        $1:AcknowledgedBy::NUMBER(38,0)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_TIMESLICEPOSTSHIFTDIFFDETAIL("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''bb67bf1f-a87a-1912-57fd-686aee5c7361''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO TIMESLICEPOSTSHIFTDIFFDETAIL (
                    DATABASEPHYSICALNAME,
                    TimeSlicePostShiftDiffDetailID,
                    TimeSlicePostID,
                    StartDateTime,
                    EndDateTime,
                    StartDateTimeUTC,
                    EndDateTimeUTC,
                    Duration,
                    ShiftDiffDetailID,
                    ShiftDiffCode,
                    ShiftDiffFactor,
                    ShiftDiffAdditional,
                    FinalPayRate,
                    HashValue,
                    ModifiedBy,
                    ModifiedOn,
                    ShiftDiffAfterOvertime
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:TimeSlicePostShiftDiffDetailID::NUMBER(38,0),
                        $1:TimeSlicePostID::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:StartDateTimeUTC::TIMESTAMP_NTZ,
                        $1:EndDateTimeUTC::TIMESTAMP_NTZ,
                        $1:Duration::NUMBER(38,0),
                        $1:ShiftDiffDetailID::NUMBER(38,0),
                        $1:ShiftDiffCode::TEXT,
                        $1:ShiftDiffFactor::NUMBER(18,2),
                        $1:ShiftDiffAdditional::NUMBER(19,4),
                        $1:FinalPayRate::NUMBER(19,4),
                        $1:HashValue::TEXT,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:ShiftDiffAfterOvertime::BOOLEAN
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFO("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''c930ce7d-904e-31a5-156d-559bc63e4246''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO USERINFO (
                    DATABASEPHYSICALNAME,
                    USERID,
                    EMPIDENTIFIER,
                    MODIFIEDON,
                    STARTDATE,
                    CLIENTID,
                    PAYROLLEMPLOYEEID,
                    WEID,
                    PEID,
                    PNGSSOUSERGUID,
                    ISSHAREDWORKER,
                    PNGUSERNAME
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:UserID::NUMBER(38,0),
                        $1:EmpIdentifier::VARCHAR(50),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:StartDate::TIMESTAMP_NTZ,
                        $1:ClientID::VARCHAR(50),
                        $1:PayrollEmployeeID::NUMBER(38,0),
                        $1:WEID::VARCHAR(20),
                        $1:PEID::VARCHAR(20),
                        $1:PNGSSOUserGUID::VARCHAR(20),
                        $1:IsSharedWorker::BOOLEAN,
                        $1:PngUserName::VARCHAR(25)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;
            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOEMPSTATUS("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''e1b6510c-9ad1-ba04-1c43-1c8345dc44b1''
            ORDER BY m.client_id, m.table_id
        `;

        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
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

        total_files_identified = file_list.length;

        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var batch_files  = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");

            var copy_command = `
                COPY INTO USERINFOEMPSTATUS (
                    DATABASEPHYSICALNAME,
                    USERINFOEMPSTATUSID,
                    USERID,
                    EMPSTATUS,
                    STARTDATETIME,
                    ENDDATETIME,
                    MODIFIEDBY,
                    MODIFIEDON,
                    DESCRIPTION,
                    RETURNTOWORKDATE,
                    INACTIVEEMPDATAPROCESSDATE
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:UserInfoEmpStatusID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:EmpStatus::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:Description::TEXT,
                        $1:ReturnToWorkDate::TIMESTAMP_NTZ,
                        $1:InActiveEmpDataProcessDate::TIMESTAMP_NTZ
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            while (copy_result.next()) {
                var file_name   = copy_result.getColumnValue(1);
                var status      = copy_result.getColumnValue(2);
                var rows_loaded = copy_result.getColumnValue(4);
                var first_error = copy_result.getColumnValue(7);

                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;

                var metadata    = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";

                var safe_filename  = (metadata.filename       || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id      || "UNKNOWN");
                var safe_table_id  = (metadata.table_id       || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error     = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOISSALARY("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''ab18c18c-ccff-62b6-4975-156ffc566ef8''
            ORDER BY m.client_id, m.table_id
        `;
        
        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
        var file_metadata = {};  // Store metadata for each file
        
        while (file_results.next()) {
            var relative_path = file_results.getColumnValue(1);
            var client_id = file_results.getColumnValue(2);
            var table_id = file_results.getColumnValue(3);
            var filename = file_results.getColumnValue(4);
            var full_file_path = file_results.getColumnValue(5);
            
            file_list.push("''" + relative_path + "''");
            file_metadata[relative_path] = {
                client_id: client_id,
                table_id: table_id,
                filename: filename,
                full_file_path: full_file_path
            };
        }
        
        total_files_identified = file_list.length;
        
        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start = batch_num * batch_size;
            var end = Math.min(start + batch_size, file_list.length);
            var batch_files = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");
            
            var copy_command = `
                COPY INTO USERINFOISSALARY (
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
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:UserInfoIsSalaryID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:IsSalary::BOOLEAN,
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            // Execute the COPY command for this batch
            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            // Process results and log to audit table
            while (copy_result.next()) {
                var file_name = copy_result.getColumnValue(1);      // file name
                var status = copy_result.getColumnValue(2);         // status
                var rows_loaded = copy_result.getColumnValue(4);    // rows_loaded
                var errors_seen = copy_result.getColumnValue(6);    // errors_seen
                var first_error = copy_result.getColumnValue(7);    // first_error
                
                // Relative path from COPY INTO result uses LandingZone/ anchor -- matches manifest key
                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;
                
                var metadata = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";
                
                // Safely escape values for SQL
                var safe_filename = (metadata.filename || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id || "UNKNOWN");
                var safe_table_id = (metadata.table_id || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                var safe_error = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.FULL_LOAD_USERINFOPAYROLLMAPPING("STAGE_NAME" VARCHAR, "PARQUET_SHARD_NO" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();
    var all_audit_rows = [];  // Accumulate audit records from all batches

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                m.client_id,
                m.table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST m
            INNER JOIN CLIENT_CONFIG cc
                ON m.client_id = cc.CLIENT_ID
                AND m.table_id = cc.TABLE_ID
                AND cc.MASTER_STATUS = ''Y''
                AND cc.PARQUET_LOAD_STATUS = ''Y''
                AND cc.PARQUET_SHARD_NO = ` + PARQUET_SHARD_NO + `
            WHERE m.table_id = ''f1b0a3f6-49a5-a942-2349-e2c4c7fb15fa''
            ORDER BY m.client_id, m.table_id
        `;

        var file_results = snowflake.createStatement({sqlText: get_files_query}).execute();
        var file_list = [];
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

        total_files_identified = file_list.length;

        if (total_files_identified === 0) {
            return "No files found to load.";
        }

        // Process files in batches of 1000
        var batch_size  = 1000;
        var batch_count = Math.ceil(file_list.length / batch_size);

        for (var batch_num = 0; batch_num < batch_count; batch_num++) {
            var start        = batch_num * batch_size;
            var end          = Math.min(start + batch_size, file_list.length);
            var batch_files  = file_list.slice(start, end);
            var files_clause = batch_files.join(", ");

            var copy_command = `
                COPY INTO USERINFOPAYROLLMAPPING (
                    DATABASEPHYSICALNAME,
                    USERINFOPAYROLLMAPPINGID,
                    USERID,
                    PAYROLLCLIENTID,
                    PAYROLLEMPLOYEEID,
                    STARTDATETIME,
                    ENDDATETIME,
                    MODIFIEDBY,
                    MODIFIEDON,
                    EMPLOYEESTATUS,
                    WEID,
                    WORKERVERSION
                )
                FROM (
                    SELECT
                        REGEXP_SUBSTR(METADATA$FILENAME::STRING, ''/([^/]+)/Tables/'', 1, 1, ''e''),
                        $1:UserInfoPayrollMappingID::NUMBER(38,0),
                        $1:UserID::NUMBER(38,0),
                        $1:PayrollClientID::VARCHAR(36),
                        $1:PayrollEmployeeID::NUMBER(38,0),
                        $1:StartDateTime::TIMESTAMP_NTZ,
                        $1:EndDateTime::TIMESTAMP_NTZ,
                        $1:ModifiedBy::NUMBER(38,0),
                        $1:ModifiedOn::TIMESTAMP_NTZ,
                        $1:EmployeeStatus::NUMBER(38,0),
                        $1:WEID::VARCHAR(100),
                        $1:WorkerVersion::NUMBER(38,0)
                    FROM @` + STAGE_NAME + `
                    (FILE_FORMAT => ''FF_TAA_ONELAKE_PARQUET'')
                )
                FILE_FORMAT = (TYPE = PARQUET)
                FILES = (` + files_clause + `)
                ON_ERROR = CONTINUE
                FORCE = TRUE
            `;

            var copy_result = snowflake.createStatement({sqlText: copy_command}).execute();

            while (copy_result.next()) {
                var file_name   = copy_result.getColumnValue(1);
                var status      = copy_result.getColumnValue(2);
                var rows_loaded = copy_result.getColumnValue(4);
                var first_error = copy_result.getColumnValue(7);

                var relative_path = file_name.indexOf("/LandingZone/") > -1
                    ? file_name.substring(file_name.indexOf("/LandingZone/"))
                    : file_name;

                var metadata    = file_metadata[relative_path] || {};
                var load_status = (status === "LOADED") ? "SUCCESS" : "FAILED";

                var safe_filename  = (metadata.filename       || file_name).replace(/''/g, "''''");
                var safe_client_id = (metadata.client_id      || "UNKNOWN");
                var safe_table_id  = (metadata.table_id       || "UNKNOWN");
                var safe_full_path = (metadata.full_file_path || file_name).replace(/''/g, "''''");
                var safe_error     = first_error ? first_error.replace(/''/g, "''''") : null;
                
                all_audit_rows.push({
                    filename: safe_filename,
                    client_id: safe_client_id,
                    table_id: safe_table_id,
                    rows_loaded: rows_loaded,
                    batch_num: batch_num + 1,
                    load_status: load_status,
                    error_message: safe_error,
                    full_path: safe_full_path
                });

                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        // Batch insert audit records with retry logic
        if (all_audit_rows && all_audit_rows.length > 0) {
            var values_list = [];
            for (var i = 0; i < all_audit_rows.length; i++) {
                var row = all_audit_rows[i];
                var error_val = row.error_message ? "''" + row.error_message + "''" : "NULL";
                values_list.push(
                    "(''"+row.filename+"'', ''"+row.client_id+"'', ''"+row.table_id+"'', " +
                    row.rows_loaded + ", " + row.batch_num + ", ''"+row.load_status+"'', " +
                    error_val + ", ''"+row.full_path+"'')"
                );
            }
            var values_clause = values_list.join(", ");
            var batch_insert = `INSERT INTO INGEST_TAA_FILE_AUDIT (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path) VALUES ` + values_clause;
            
            var max_retries = 10;
            var retry_count = 0;
            var insert_succeeded = false;
            var last_error = null;
            
            while (retry_count < max_retries && !insert_succeeded) {
                try {
                    snowflake.createStatement({sqlText: batch_insert}).execute();
                    insert_succeeded = true;
                } catch (err) {
                    last_error = err.message;
                    if (err.message.indexOf("lock") > -1 || err.message.indexOf("exceeds the 20 statements") > -1) {
                        retry_count++;
                        if (retry_count < max_retries) {
                                                        snowflake.createStatement({sqlText: "SELECT SYSTEM$WAIT(5);"}).execute();
                            continue;
                        } else {
                            throw new Error("Audit insert failed after " + max_retries + " retries: " + last_error);
                        }
                    } else {
                        throw new Error("Audit insert failed: " + err.message);
                    }
                }
            }
            all_audit_rows = [];
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_FULL_LOAD_FINALIZE()
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
            "SELECT PARAM_NAME, PARAM_VALUE FROM INGEST_TAA_RUN_CONFIG " +
            "WHERE PARAM_NAME IN (''STAGE_NAME'', ''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();
        var config = {};
        while (cfg_result.next()) {
            config[cfg_result.getColumnValue(1)] = cfg_result.getColumnValue(2);
        }
        var stage_name    = config["STAGE_NAME"]        || "(unknown)";
        var client_filter = config["CLIENT_ID_FILTER"]  || null;
        var table_filter  = config["TABLE_NAME_FILTER"] || null;

        msg += "=== INGEST FULL LOAD (Task DAG) ===\\n";
        msg += "Client scope : " + (client_filter || "ALL CLIENTS") + "\\n";
        msg += "Table filter : " + (table_filter  || "ALL ACTIVE TABLES") + "\\n";
        msg += "Stage        : " + stage_name + "\\n";

        // ------------------------------------------------------------------
        // Manifest summary
        // ------------------------------------------------------------------
        var mfst = snowflake.createStatement({sqlText:
            "SELECT COUNT(*) AS files, COUNT(DISTINCT client_id) AS clients, " +
            "COUNT(DISTINCT table_id) AS tables " +
            "FROM STAGE_TAA_FULL_FILE_MANIFEST"
        }).execute();
        mfst.next();
        msg += "\\n=== FILE MANIFEST ===\\n";
        msg += "  Total files  : " + mfst.getColumnValue(1) + "\\n";
        msg += "  Clients      : " + mfst.getColumnValue(2) + "\\n";
        msg += "  Tables       : " + mfst.getColumnValue(3) + "\\n";

        // ------------------------------------------------------------------
        // Per-table load summary from INGEST_TAA_FILE_AUDIT
        // Use INGEST_TAA_TABLE_CONFIG for ordered table names; join to audit.
        // We want rows written during this DAG run -- use STAGE_TAA_FULL_FILE_MANIFEST
        // as the scope anchor since it was populated by this run''s PREPARE step.
        // ------------------------------------------------------------------
        var tbl_result = snowflake.createStatement({sqlText: `
            WITH deduped_audit AS (
                SELECT *
                FROM INGEST_TAA_FILE_AUDIT aud
                WHERE aud.LOAD_STATUS IN (''SUCCESS'', ''FAILED'')
                AND EXISTS (
                    SELECT 1
                    FROM STAGE_TAA_FULL_FILE_MANIFEST mfst
                    WHERE mfst.FULL_FILE_PATH = aud.FULL_STAGE_PATH
                )
                QUALIFY ROW_NUMBER() OVER (PARTITION BY aud.FULL_STAGE_PATH ORDER BY aud.LOAD_ID DESC) = 1
            )
            SELECT
                cfg.TABLE_NAME,
                cfg.LOAD_ORDER,
                SUM(CASE WHEN aud.LOAD_STATUS = ''SUCCESS'' THEN COALESCE(aud.ROWS_LOADED, 0) ELSE 0 END) AS rows_loaded,
                COUNT(CASE WHEN aud.LOAD_STATUS = ''SUCCESS'' THEN aud.LOAD_ID END) AS files_loaded,
                SUM(CASE WHEN aud.LOAD_STATUS = ''FAILED'' THEN 1 ELSE 0 END) AS failed_files
            FROM INGEST_TAA_TABLE_CONFIG cfg
            LEFT JOIN deduped_audit aud
                ON  UPPER(aud.TABLE_ID) = UPPER(cfg.SOURCE_TABLE_ID)
            WHERE cfg.IS_ACTIVE_FULL_LOAD = TRUE
            GROUP BY cfg.TABLE_NAME, cfg.LOAD_ORDER
            ORDER BY cfg.LOAD_ORDER, cfg.TABLE_NAME
        `}).execute();

        msg += "\\n=== LOADING TABLES ===\\n";
        var tbl_num   = 0;
        var total_rows = 0;
        var total_files = 0;
        var total_failed = 0;
        var table_lines = [];

        // Collect first so we know the total count for the [n/N] prefix
        while (tbl_result.next()) {
            table_lines.push({
                name:         tbl_result.getColumnValue(1),
                rows_loaded:  tbl_result.getColumnValue(3),
                files_loaded: tbl_result.getColumnValue(4),
                failed_files: tbl_result.getColumnValue(5)
            });
        }

        for (var i = 0; i < table_lines.length; i++) {
            var t = table_lines[i];
            var status_suffix = t.failed_files > 0 ? " (" + t.failed_files + " file(s) FAILED)" : "";
            msg += "  [" + (i + 1) + "/" + table_lines.length + "] " + t.name + "\\n";
            msg += "      Files loaded: " + t.files_loaded +
                   "  Rows: " + t.rows_loaded + status_suffix + "\\n";
            total_rows  += t.rows_loaded;
            total_files += t.files_loaded;
            total_failed += t.failed_files;
        }

        // ------------------------------------------------------------------
        // Update INGEST_TAA_FULL_LOAD_STATE
        // ------------------------------------------------------------------
        var state_result = snowflake.createStatement({sqlText: `
            MERGE INTO INGEST_TAA_FULL_LOAD_STATE tgt
            USING (
                SELECT
                    client_id,
                    table_id,
                    MAX(tabledata_folder) AS tabledata_folder,
                    MAX(filename)         AS full_load_file,
                    MAX(last_modified)    AS full_load_last_modified
                FROM STAGE_TAA_FULL_FILE_MANIFEST
                GROUP BY client_id, table_id
            ) src
            ON  tgt.client_id = src.client_id
            AND tgt.table_id  = src.table_id
            WHEN MATCHED THEN UPDATE SET
                tgt.tabledata_folder        = src.tabledata_folder,
                tgt.full_load_file          = src.full_load_file,
                tgt.full_load_last_modified = src.full_load_last_modified,
                tgt.state_updated_at        = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                client_id, table_id, tabledata_folder,
                full_load_file, full_load_last_modified, state_updated_at
            ) VALUES (
                src.client_id, src.table_id, src.tabledata_folder,
                src.full_load_file, src.full_load_last_modified, CURRENT_TIMESTAMP()
            )
        `}).execute();
        state_result.next();
        var rows_upserted = state_result.getColumnValue(1) + state_result.getColumnValue(2);

        // ------------------------------------------------------------------
        // Suspend root task
        // ------------------------------------------------------------------
        snowflake.createStatement({sqlText: "ALTER TASK TAA_FULL_ROOT SUSPEND;"}).execute();

        var end_time     = new Date();
        var duration_sec = ((end_time - run_start) / 1000).toFixed(2);

        msg += "\\n=== SUMMARY ===\\n";
        msg += "  Tables        : " + table_lines.length + "\\n";
        msg += "  Total files   : " + total_files + "\\n";
        msg += "  Total rows    : " + total_rows + "\\n";
        if (total_failed > 0) {
            msg += "  Failed files  : " + total_failed + " -- check INGEST_TAA_FILE_AUDIT\\n";
        }
        msg += "\\nFULL LOAD DAG RUN COMPLETE";

        // Reset filter parameters now that the run is fully complete so the
        // next nightly scheduled run always starts clean.
        snowflake.createStatement({sqlText:
            "UPDATE INGEST_TAA_RUN_CONFIG " +
            "SET PARAM_VALUE = NULL, UPDATED_AT = CURRENT_TIMESTAMP() " +
            "WHERE PARAM_NAME IN (''CLIENT_ID_FILTER'', ''TABLE_NAME_FILTER'')"
        }).execute();

        return msg;
    } catch (err) {
        throw new Error("INGEST_TAA_FULL_LOAD_FINALIZE failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.INGEST_TAA_LAUNCH_FULL_LOAD("CLIENT_ID_FILTER" VARCHAR DEFAULT null, "TABLE_NAME_FILTER" VARCHAR DEFAULT null, "STAGE_NAME" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        var stage_name_safe = (STAGE_NAME !== null && STAGE_NAME !== undefined &&
                               STAGE_NAME.trim() !== "")
            ? STAGE_NAME.trim() : null;

        if (!stage_name_safe) {
            throw new Error("STAGE_NAME parameter is required. " +
                            "Example: CALL INGEST_TAA_LAUNCH_FULL_LOAD(NULL, NULL, ''demo.FAB_CF_WS_N1_STG'');");
        }

        var client_val = (CLIENT_ID_FILTER !== null && CLIENT_ID_FILTER !== undefined &&
                          CLIENT_ID_FILTER.trim() !== "")
            ? CLIENT_ID_FILTER.trim() : null;
        var table_val  = (TABLE_NAME_FILTER !== null && TABLE_NAME_FILTER !== undefined &&
                          TABLE_NAME_FILTER.trim() !== "")
            ? TABLE_NAME_FILTER.trim() : null;

        snowflake.createStatement({sqlText:
            "MERGE INTO INGEST_TAA_RUN_CONFIG tgt " +
            "USING (SELECT * FROM VALUES " +
            "  (''STAGE_NAME'',        " + (stage_name_safe ? "''" + stage_name_safe + "''" : "NULL") + "), " +
            "  (''CLIENT_ID_FILTER'',  " + (client_val ? "''" + client_val + "''" : "NULL")  + "), " +
            "  (''TABLE_NAME_FILTER'', " + (table_val  ? "''" + table_val  + "''" : "NULL")  + ") " +
            "AS src(PARAM_NAME, PARAM_VALUE)) src ON tgt.PARAM_NAME = src.PARAM_NAME " +
            "WHEN MATCHED THEN UPDATE SET " +
            "  tgt.PARAM_VALUE = src.PARAM_VALUE, " +
            "  tgt.UPDATED_AT  = CURRENT_TIMESTAMP();"
        }).execute();

        // Resume the root (it suspends itself at the end of each run via INGEST_TAA_FULL_LOAD_FINALIZE).
        // RESUME is idempotent -- safe to call even if already resumed.
        snowflake.createStatement({sqlText: "ALTER TASK TAA_FULL_ROOT RESUME;"}).execute();
        snowflake.createStatement({sqlText: "EXECUTE TASK TAA_FULL_ROOT;"}).execute();

        var scope = client_val ? " (client: " + client_val + ")" : " (all clients)";
        return "Task DAG triggered" + scope + ".\\n" +
               "Stage: " + stage_name_safe + "\\n" +
               "\\nMonitor progress:\\n" +
               "  SELECT * FROM TABLE(TASK_DEPENDENTS(''TAA_FULL_ROOT'', TRUE)) ORDER BY SCHEDULED_TIME;\\n" +
               "\\nView history:\\n" +
               "  SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME\\n" +
               "  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => ''TAA_FULL_ROOT'', RESULT_LIMIT => 10))\\n" +
               "  ORDER BY SCHEDULED_TIME DESC;";
    } catch (err) {
        throw new Error("INGEST_TAA_LAUNCH_FULL_LOAD failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.BUILD_STAGE_TAA_FULL_FILE_MANIFEST("CLIENT_ID_FILTER" VARCHAR, "TABLE_NAME_FILTER" VARCHAR, "STAGE_NAME" VARCHAR)
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

        // Load the Parquet inventory from the static OneLake inventory file.
        // The Fabric notebook overwrites this file in-place on every run.
        var inv_file = "Inventory/file_inventory/Inventory_PARQUET.csv";

        snowflake.createStatement({
            sqlText: "CREATE OR REPLACE TEMPORARY TABLE STAGE_TAA_INV_RAW AS " +
                     "SELECT " +
                     "  REGEXP_SUBSTR($1, ''/LandingZone/.*'') AS full_file_path, " +
                     "  TO_CHAR(TO_TIMESTAMP_NTZ($2, ''YYYY-MM-DD HH24:MI:SS''), " +
                     "          ''DY, DD MON YYYY HH24:MI:SS'') || '' UTC'' AS last_modified " +
                     "FROM @" + STAGE_NAME + "/" + inv_file + " (FILE_FORMAT => ''FF_TAA_INVENTORY_CSV'') " +
                     "WHERE $1 LIKE ''%/FullCopyData/%''"
        }).execute();

        var cnt = snowflake.createStatement({sqlText: "SELECT COUNT(*) FROM STAGE_TAA_INV_RAW"}).execute();
        cnt.next();
        var total_files = cnt.getColumnValue(1);

        var file_list_cte = "WITH file_list AS (SELECT full_file_path, last_modified FROM STAGE_TAA_INV_RAW),";


        // Optional WHERE clauses to restrict inserts to specified client(s) and/or table(s).
        var client_where_clause = is_client_scoped
            ? "AND p.client_id IN (" + client_id_in_list + ")"
            : "";

        // Parse the listing and insert ALL files from the most-recent TableData_* folder
        // per client/table combination.
        // Large tables produce multiple data-N.parquet files in the same folder with the
        // same timestamp -- MAX(tabledata_folder) picks the latest folder, then the join
        // keeps every file inside it.
        var insert_command = `
            INSERT INTO STAGE_TAA_FULL_FILE_MANIFEST
                (FULL_FILE_PATH, CLIENT_ID, TABLE_ID, TABLEDATA_FOLDER, FILENAME, LAST_MODIFIED)
            ` + file_list_cte + `
            parsed_files AS (
                SELECT
                    full_file_path,
                    ''/'' || SUBSTRING(full_file_path, POSITION(''LandingZone/'' IN full_file_path)) AS relative_path,
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
            SELECT distinct p.relative_path, p.client_id, p.table_id,
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
                      AND aud.load_status     in (''SUCCESS'',''FAILED'')
              )
              ` + client_where_clause + `
              ` + table_where_clause + `
            ORDER BY p.client_id, p.table_id, p.filename;
        `;

        var insert_result = snowflake.createStatement({sqlText: insert_command}).execute();
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

        var skipped_msg = already_loaded > 0
            ? "; " + already_loaded + " file(s) skipped (already loaded per audit)"
            : "";

        // Populate CLIENT_CONFIG table with shard numbers and set status columns
        var populate_config = `
            MERGE INTO CLIENT_CONFIG cc
            USING (
                SELECT 
                    client_id,
                    table_id,
                    MAX(tabledata_folder) AS active_foldername,
                    MAX(csv_shard_no) AS csv_shard_no,
                    MAX(parquet_shard_no) AS parquet_shard_no
                FROM (
                    SELECT DISTINCT
                        REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'', 1, 1, ''e'', 1) AS client_id,
                        REGEXP_SUBSTR(full_file_path, ''/Tables/([^/]+)/'', 1, 1, ''e'', 1) AS table_id,
                        tabledata_folder,
                        MOD(ABS(HASH(REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'', 1, 1, ''e'', 1))), 14) + 1 AS csv_shard_no,
                        MOD(ABS(HASH(REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'', 1, 1, ''e'', 1))), 14) + 1 AS parquet_shard_no
                    FROM STAGE_TAA_FULL_FILE_MANIFEST
                    WHERE full_file_path LIKE ''%FullCopyData%parquet''
                )
                GROUP BY client_id, table_id
            ) sm
            ON cc.CLIENT_ID = sm.client_id
               AND cc.TABLE_ID = sm.table_id
            WHEN MATCHED THEN
                UPDATE SET 
                    ACTIVE_FOLDERNAME = sm.active_foldername,
                    CSV_SHARD_NO = sm.csv_shard_no,
                    PARQUET_SHARD_NO = sm.parquet_shard_no,
                    CSV_DELTA_STATUS = ''Y'',
                    PARQUET_LOAD_STATUS = ''Y''
            WHEN NOT MATCHED THEN
                INSERT (CLIENT_ID, TABLE_ID, ACTIVE_FOLDERNAME, CSV_SHARD_NO, PARQUET_SHARD_NO, CSV_DELTA_STATUS, PARQUET_LOAD_STATUS)
                VALUES (sm.client_id, sm.table_id, sm.active_foldername, sm.csv_shard_no, sm.parquet_shard_no, ''Y'', ''Y'')
        `;
        
        snowflake.createStatement({sqlText: populate_config}).execute();

        return "Processed " + total_files + " stage file(s)" + scope_msg +
               "; inserted " + files_inserted + " file(s) into STAGE_TAA_FULL_FILE_MANIFEST" +
               skipped_msg + ". CLIENT_CONFIG table populated.";

    } catch (err) {
        throw new Error("BUILD_STAGE_TAA_FULL_FILE_MANIFEST failed: " + err.message);
    }
';

CREATE OR REPLACE PROCEDURE DL_P_STRATUSTIME_PR.TAA.BUILD_STAGE_TAA_DELTA_MANIFEST("CLIENT_ID_FILTER" VARCHAR, "TABLE_NAME_FILTER" VARCHAR, "STAGE_NAME" VARCHAR)
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
            WITH parsed AS (
                SELECT
                    full_file_path,
                    TO_TIMESTAMP_TZ(last_modified, ''DY, DD MON YYYY HH24:MI:SS TZD'') AS last_modified,
                    REGEXP_SUBSTR(full_file_path, ''/([^/]+)/Tables/'',       1, 1, ''e'', 1) AS client_id,
                    REGEXP_SUBSTR(full_file_path, ''/Tables/([^/]+)/'',       1, 1, ''e'', 1) AS table_id,
                    REGEXP_SUBSTR(full_file_path, ''/(TableData_[^/]+)/'',    1, 1, ''e'', 1) AS tabledata_folder,
                    SPLIT_PART(full_file_path, ''/'', -1)                                     AS filename
                FROM STAGE_TAA_CSV_INV_RAW
            )
            SELECT distinct
                parsed.full_file_path,
                parsed.client_id,
                parsed.table_id,
                parsed.tabledata_folder,
                parsed.filename,
                parsed.last_modified
            FROM parsed
            INNER JOIN CLIENT_CONFIG cfg
                ON  UPPER(cfg.TABLE_ID) = UPPER(parsed.table_id)
                AND UPPER(cfg.CLIENT_ID) = UPPER(parsed.client_id)
                AND cfg.CSV_DELTA_STATUS   = ''Y''
            -- Must be in the same TableData_* folder as the last full load
            WHERE parsed.tabledata_folder  = cfg.ACTIVE_FOLDERNAME
            -- Must not already have been applied
              AND NOT EXISTS (
                    SELECT 1
                    FROM INGEST_TAA_FILE_AUDIT aud
                    WHERE aud.full_stage_path = parsed.full_file_path
                      AND aud.load_status     in (''SUCCESS'',''FAILED'')
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
               (already_loaded > 0 ? "; " + already_loaded + " file(s) skipped" : "") + ".";

    } catch (err) {
        throw new Error("BUILD_STAGE_TAA_DELTA_MANIFEST failed: " + err.message);
    }
';

-- =============================================================================
-- STEP 1: CREATE ROOT TASK
-- =============================================================================
CREATE OR REPLACE TASK TAA_DELTA_ROOT WAREHOUSE = WH_DSDP_ETL_PR SCHEDULE  = 'USING CRON 0 2 * * * America/New_York' ALLOW_OVERLAPPING_EXECUTION = FALSE AS CALL INGEST_TAA_DELTA_PREPARE();

-- =============================================================================
-- STEP 2: CREATE 14 COORDINATOR_SHARD TASKS (One per Shard)
-- =============================================================================
CREATE OR REPLACE TASK TAA_DELTA_SHARD_1_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_2_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_3_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_4_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_5_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_6_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_7_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_8_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_9_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_10_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_11_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_12_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_13_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_14_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_ROOT AS SELECT 1;

-- =============================================================================
-- STEP 3: CREATE 210 SHARD TASKS (15 Tables × 14 Shards)
-- =============================================================================

-- ============= SHARD 1 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 1);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 1);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 1);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 1);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 1);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 1);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 1);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 1);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 1);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 1);

-- ============= SHARD 2 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 2);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 2);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 2);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 2);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 2);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 2);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 2);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 2);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 2);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_2_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 2);

-- ============= SHARD 3 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 3);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 3);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 3);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 3);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 3);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 3);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 3);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 3);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 3);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_3_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 3);

-- ============= SHARD 4 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 4);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 4);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 4);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 4);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 4);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 4);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 4);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 4);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 4);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_4_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 4);

-- ============= SHARD 5 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 5);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 5);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 5);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 5);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 5);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 5);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 5);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 5);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 5);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_5_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 5);

-- ============= SHARD 6 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 6);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 6);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 6);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 6);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 6);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 6);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 6);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 6);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 6);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_6_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 6);

-- ============= SHARD 7 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 7);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 7);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 7);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 7);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 7);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 7);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 7);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 7);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 7);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_7_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 7);

-- ============= SHARD 8 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 8);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 8);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 8);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 8);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 8);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 8);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 8);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 8);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 8);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_8_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 8);

-- ============= SHARD 9 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 9);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 9);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 9);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 9);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 9);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 9);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 9);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 9);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 9);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_9_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 9);

-- ============= SHARD 10 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 10);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 10);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 10);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 10);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 10);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 10);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 10);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 10);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 10);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_10_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 10);

-- ============= SHARD 11 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 11);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 11);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 11);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 11);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 11);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 11);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 11);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 11);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 11);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_11_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 11);

-- ============= SHARD 12 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 12);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 12);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 12);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 12);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 12);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 12);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 12);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 12);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 12);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_12_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 12);

-- ============= SHARD 13 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 13);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 13);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 13);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 13);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 13);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 13);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 13);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 13);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 13);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_13_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 13);

-- ============= SHARD 14 TASKS =============
CREATE OR REPLACE TASK TAA_DELTA_CUSTOMER_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('CUSTOMER', 14);
CREATE OR REPLACE TASK TAA_DELTA_ENTERPRISECUSTOMER_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 14);
CREATE OR REPLACE TASK TAA_DELTA_LLDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('LLDETAIL', 14);
CREATE OR REPLACE TASK TAA_DELTA_PAYTYPE_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('PAYTYPE', 14);
CREATE OR REPLACE TASK TAA_DELTA_SCHEDULE_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('SCHEDULE', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFDATA_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFDATA', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUEST_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOST_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOST', 14);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOISSALARY_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOISSALARY', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 14);
CREATE OR REPLACE TASK TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 14);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 14);
CREATE OR REPLACE TASK TAA_DELTA_USERINFO_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFO', 14);
CREATE OR REPLACE TASK TAA_DELTA_USERINFOEMPSTATUS_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_14_COORDINATOR AS CALL DELTA_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 14);

-- =============================================================================
-- STEP 4: CREATE 14 GATE_SHARD TASKS 
-- =============================================================================
CREATE OR REPLACE TASK TAA_DELTA_SHARD_1_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_1, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_1, TAA_DELTA_LLDETAIL_SHARD_1, TAA_DELTA_PAYTYPE_SHARD_1, TAA_DELTA_SCHEDULE_SHARD_1, TAA_DELTA_TIMEOFFDATA_SHARD_1, TAA_DELTA_TIMEOFFREQUEST_SHARD_1, TAA_DELTA_TIMESLICEPOST_SHARD_1, TAA_DELTA_USERINFOISSALARY_SHARD_1, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_1, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_1, TAA_DELTA_USERINFO_SHARD_1, TAA_DELTA_USERINFOEMPSTATUS_SHARD_1 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_2_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_2, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_2, TAA_DELTA_LLDETAIL_SHARD_2, TAA_DELTA_PAYTYPE_SHARD_2, TAA_DELTA_SCHEDULE_SHARD_2, TAA_DELTA_TIMEOFFDATA_SHARD_2, TAA_DELTA_TIMEOFFREQUEST_SHARD_2, TAA_DELTA_TIMESLICEPOST_SHARD_2, TAA_DELTA_USERINFOISSALARY_SHARD_2, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_2, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_2, TAA_DELTA_USERINFO_SHARD_2, TAA_DELTA_USERINFOEMPSTATUS_SHARD_2 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_3_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_3, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_3, TAA_DELTA_LLDETAIL_SHARD_3, TAA_DELTA_PAYTYPE_SHARD_3, TAA_DELTA_SCHEDULE_SHARD_3, TAA_DELTA_TIMEOFFDATA_SHARD_3, TAA_DELTA_TIMEOFFREQUEST_SHARD_3, TAA_DELTA_TIMESLICEPOST_SHARD_3, TAA_DELTA_USERINFOISSALARY_SHARD_3, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_3, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_3, TAA_DELTA_USERINFO_SHARD_3, TAA_DELTA_USERINFOEMPSTATUS_SHARD_3 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_4_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_4, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_4, TAA_DELTA_LLDETAIL_SHARD_4, TAA_DELTA_PAYTYPE_SHARD_4, TAA_DELTA_SCHEDULE_SHARD_4, TAA_DELTA_TIMEOFFDATA_SHARD_4, TAA_DELTA_TIMEOFFREQUEST_SHARD_4, TAA_DELTA_TIMESLICEPOST_SHARD_4, TAA_DELTA_USERINFOISSALARY_SHARD_4, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_4, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_4, TAA_DELTA_USERINFO_SHARD_4, TAA_DELTA_USERINFOEMPSTATUS_SHARD_4 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_5_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_5, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_5, TAA_DELTA_LLDETAIL_SHARD_5, TAA_DELTA_PAYTYPE_SHARD_5, TAA_DELTA_SCHEDULE_SHARD_5, TAA_DELTA_TIMEOFFDATA_SHARD_5, TAA_DELTA_TIMEOFFREQUEST_SHARD_5, TAA_DELTA_TIMESLICEPOST_SHARD_5, TAA_DELTA_USERINFOISSALARY_SHARD_5, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_5, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_5, TAA_DELTA_USERINFO_SHARD_5, TAA_DELTA_USERINFOEMPSTATUS_SHARD_5 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_6_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_6, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_6, TAA_DELTA_LLDETAIL_SHARD_6, TAA_DELTA_PAYTYPE_SHARD_6, TAA_DELTA_SCHEDULE_SHARD_6, TAA_DELTA_TIMEOFFDATA_SHARD_6, TAA_DELTA_TIMEOFFREQUEST_SHARD_6, TAA_DELTA_TIMESLICEPOST_SHARD_6, TAA_DELTA_USERINFOISSALARY_SHARD_6, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_6, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_6, TAA_DELTA_USERINFO_SHARD_6, TAA_DELTA_USERINFOEMPSTATUS_SHARD_6 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_7_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_7, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_7, TAA_DELTA_LLDETAIL_SHARD_7, TAA_DELTA_PAYTYPE_SHARD_7, TAA_DELTA_SCHEDULE_SHARD_7, TAA_DELTA_TIMEOFFDATA_SHARD_7, TAA_DELTA_TIMEOFFREQUEST_SHARD_7, TAA_DELTA_TIMESLICEPOST_SHARD_7, TAA_DELTA_USERINFOISSALARY_SHARD_7, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_7, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_7, TAA_DELTA_USERINFO_SHARD_7, TAA_DELTA_USERINFOEMPSTATUS_SHARD_7 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_8_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_8, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_8, TAA_DELTA_LLDETAIL_SHARD_8, TAA_DELTA_PAYTYPE_SHARD_8, TAA_DELTA_SCHEDULE_SHARD_8, TAA_DELTA_TIMEOFFDATA_SHARD_8, TAA_DELTA_TIMEOFFREQUEST_SHARD_8, TAA_DELTA_TIMESLICEPOST_SHARD_8, TAA_DELTA_USERINFOISSALARY_SHARD_8, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_8, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_8, TAA_DELTA_USERINFO_SHARD_8, TAA_DELTA_USERINFOEMPSTATUS_SHARD_8 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_9_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_9, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_9, TAA_DELTA_LLDETAIL_SHARD_9, TAA_DELTA_PAYTYPE_SHARD_9, TAA_DELTA_SCHEDULE_SHARD_9, TAA_DELTA_TIMEOFFDATA_SHARD_9, TAA_DELTA_TIMEOFFREQUEST_SHARD_9, TAA_DELTA_TIMESLICEPOST_SHARD_9, TAA_DELTA_USERINFOISSALARY_SHARD_9, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_9, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_9, TAA_DELTA_USERINFO_SHARD_9, TAA_DELTA_USERINFOEMPSTATUS_SHARD_9 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_10_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_10, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_10, TAA_DELTA_LLDETAIL_SHARD_10, TAA_DELTA_PAYTYPE_SHARD_10, TAA_DELTA_SCHEDULE_SHARD_10, TAA_DELTA_TIMEOFFDATA_SHARD_10, TAA_DELTA_TIMEOFFREQUEST_SHARD_10, TAA_DELTA_TIMESLICEPOST_SHARD_10, TAA_DELTA_USERINFOISSALARY_SHARD_10, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_10, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_10, TAA_DELTA_USERINFO_SHARD_10, TAA_DELTA_USERINFOEMPSTATUS_SHARD_10 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_11_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_11, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_11, TAA_DELTA_LLDETAIL_SHARD_11, TAA_DELTA_PAYTYPE_SHARD_11, TAA_DELTA_SCHEDULE_SHARD_11, TAA_DELTA_TIMEOFFDATA_SHARD_11, TAA_DELTA_TIMEOFFREQUEST_SHARD_11, TAA_DELTA_TIMESLICEPOST_SHARD_11, TAA_DELTA_USERINFOISSALARY_SHARD_11, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_11, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_11, TAA_DELTA_USERINFO_SHARD_11, TAA_DELTA_USERINFOEMPSTATUS_SHARD_11 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_12_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_12, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_12, TAA_DELTA_LLDETAIL_SHARD_12, TAA_DELTA_PAYTYPE_SHARD_12, TAA_DELTA_SCHEDULE_SHARD_12, TAA_DELTA_TIMEOFFDATA_SHARD_12, TAA_DELTA_TIMEOFFREQUEST_SHARD_12, TAA_DELTA_TIMESLICEPOST_SHARD_12, TAA_DELTA_USERINFOISSALARY_SHARD_12, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_12, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_12, TAA_DELTA_USERINFO_SHARD_12, TAA_DELTA_USERINFOEMPSTATUS_SHARD_12 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_13_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_13, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_13, TAA_DELTA_LLDETAIL_SHARD_13, TAA_DELTA_PAYTYPE_SHARD_13, TAA_DELTA_SCHEDULE_SHARD_13, TAA_DELTA_TIMEOFFDATA_SHARD_13, TAA_DELTA_TIMEOFFREQUEST_SHARD_13, TAA_DELTA_TIMESLICEPOST_SHARD_13, TAA_DELTA_USERINFOISSALARY_SHARD_13, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_13, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_13, TAA_DELTA_USERINFO_SHARD_13, TAA_DELTA_USERINFOEMPSTATUS_SHARD_13 AS SELECT 1;
CREATE OR REPLACE TASK TAA_DELTA_SHARD_14_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_CUSTOMER_SHARD_14, TAA_DELTA_ENTERPRISECUSTOMER_SHARD_14, TAA_DELTA_LLDETAIL_SHARD_14, TAA_DELTA_PAYTYPE_SHARD_14, TAA_DELTA_SCHEDULE_SHARD_14, TAA_DELTA_TIMEOFFDATA_SHARD_14, TAA_DELTA_TIMEOFFREQUEST_SHARD_14, TAA_DELTA_TIMESLICEPOST_SHARD_14, TAA_DELTA_USERINFOISSALARY_SHARD_14, TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_14, TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14, TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14, TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_14, TAA_DELTA_USERINFO_SHARD_14, TAA_DELTA_USERINFOEMPSTATUS_SHARD_14 AS SELECT 1;

-- =============================================================================
-- STEP 5: CREATE FINALIZE TASK
-- =============================================================================
CREATE OR REPLACE TASK TAA_DELTA_FINALIZE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_DELTA_SHARD_1_GATE, TAA_DELTA_SHARD_2_GATE, TAA_DELTA_SHARD_3_GATE, TAA_DELTA_SHARD_4_GATE, TAA_DELTA_SHARD_5_GATE, TAA_DELTA_SHARD_6_GATE, TAA_DELTA_SHARD_7_GATE, TAA_DELTA_SHARD_8_GATE, TAA_DELTA_SHARD_9_GATE, TAA_DELTA_SHARD_10_GATE, TAA_DELTA_SHARD_11_GATE, TAA_DELTA_SHARD_12_GATE, TAA_DELTA_SHARD_13_GATE, TAA_DELTA_SHARD_14_GATE AS CALL INGEST_TAA_DELTA_FINALIZE();

-- =============================================================================
-- STEP 1: CREATE ROOT TASK
-- =============================================================================
CREATE OR REPLACE TASK TAA_FULL_ROOT WAREHOUSE = WH_DSDP_ETL_PR SCHEDULE = '11520 MINUTE' ALLOW_OVERLAPPING_EXECUTION = FALSE AS CALL INGEST_TAA_FULL_LOAD_PREPARE();

-- =============================================================================
-- STEP 2: CREATE 14 COORDINATOR_SHARD TASKS (One per Shard)
-- =============================================================================
CREATE OR REPLACE TASK TAA_FULL_SHARD_1_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_2_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_3_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_4_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_5_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_6_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_7_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_8_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_9_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_10_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_11_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_12_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_13_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_14_COORDINATOR WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_ROOT AS SELECT 1;

-- =============================================================================
-- STEP 3: CREATE 210 SHARD TASKS (15 Tables × 14 Shards)
-- =============================================================================

-- ============= SHARD 1 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 1);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 1);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 1);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 1);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 1);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 1);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 1);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 1);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 1);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_1 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 1);

-- ============= SHARD 2 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 2);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 2);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 2);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 2);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 2);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 2);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 2);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 2);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 2);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_2 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_2_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 2);

-- ============= SHARD 3 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 3);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 3);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 3);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 3);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 3);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 3);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 3);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 3);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 3);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_3 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_3_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 3);

-- ============= SHARD 4 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 4);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 4);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 4);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 4);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 4);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 4);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 4);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 4);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 4);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_4 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_4_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 4);

-- ============= SHARD 5 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 5);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 5);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 5);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 5);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 5);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 5);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 5);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 5);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 5);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_5 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_5_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 5);

-- ============= SHARD 6 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 6);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 6);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 6);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 6);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 6);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 6);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 6);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 6);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 6);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_6 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_6_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 6);

-- ============= SHARD 7 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 7);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 7);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 7);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 7);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 7);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 7);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 7);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 7);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 7);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_7 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_7_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 7);

-- ============= SHARD 8 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 8);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 8);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 8);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 8);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 8);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 8);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 8);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 8);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 8);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_8 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_8_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 8);

-- ============= SHARD 9 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 9);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 9);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 9);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 9);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 9);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 9);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 9);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 9);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 9);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_9 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_9_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 9);

-- ============= SHARD 10 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 10);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 10);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 10);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 10);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 10);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 10);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 10);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 10);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 10);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_10 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_10_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 10);

-- ============= SHARD 11 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 11);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 11);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 11);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 11);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 11);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 11);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 11);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 11);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 11);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_11 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_11_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 11);

-- ============= SHARD 12 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 12);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 12);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 12);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 12);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 12);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 12);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 12);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 12);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 12);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_12 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_12_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 12);

-- ============= SHARD 13 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 13);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 13);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 13);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 13);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 13);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 13);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 13);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 13);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 13);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_13 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_13_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 13);

-- ============= SHARD 14 TASKS =============
CREATE OR REPLACE TASK TAA_FULL_CUSTOMER_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('CUSTOMER', 14);
CREATE OR REPLACE TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('ENTERPRISECUSTOMER', 14);
CREATE OR REPLACE TASK TAA_FULL_LLDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('LLDETAIL', 14);
CREATE OR REPLACE TASK TAA_FULL_PAYTYPE_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('PAYTYPE', 14);
CREATE OR REPLACE TASK TAA_FULL_SCHEDULE_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('SCHEDULE', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFDATA_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFDATA', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUEST_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUEST', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOST_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOST', 14);
CREATE OR REPLACE TASK TAA_FULL_USERINFOISSALARY_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOISSALARY', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMEOFFREQUESTDETAIL', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTEXCEPTIONDETAIL', 14);
CREATE OR REPLACE TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('TIMESLICEPOSTSHIFTDIFFDETAIL', 14);
CREATE OR REPLACE TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOPAYROLLMAPPING', 14);
CREATE OR REPLACE TASK TAA_FULL_USERINFO_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFO', 14);
CREATE OR REPLACE TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_14 WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_14_COORDINATOR AS CALL FULL_LOAD_FROM_CONFIG('USERINFOEMPSTATUS', 14);

-- =============================================================================
-- STEP 4: CREATE 14 GATE_SHARD TASKS 
-- =============================================================================
CREATE OR REPLACE TASK TAA_FULL_SHARD_1_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_1, TAA_FULL_ENTERPRISECUSTOMER_SHARD_1, TAA_FULL_LLDETAIL_SHARD_1, TAA_FULL_PAYTYPE_SHARD_1, TAA_FULL_SCHEDULE_SHARD_1, TAA_FULL_TIMEOFFDATA_SHARD_1, TAA_FULL_TIMEOFFREQUEST_SHARD_1, TAA_FULL_TIMESLICEPOST_SHARD_1, TAA_FULL_USERINFOISSALARY_SHARD_1, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_1, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_1, TAA_FULL_USERINFO_SHARD_1, TAA_FULL_USERINFOEMPSTATUS_SHARD_1 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_2_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_2, TAA_FULL_ENTERPRISECUSTOMER_SHARD_2, TAA_FULL_LLDETAIL_SHARD_2, TAA_FULL_PAYTYPE_SHARD_2, TAA_FULL_SCHEDULE_SHARD_2, TAA_FULL_TIMEOFFDATA_SHARD_2, TAA_FULL_TIMEOFFREQUEST_SHARD_2, TAA_FULL_TIMESLICEPOST_SHARD_2, TAA_FULL_USERINFOISSALARY_SHARD_2, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_2, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_2, TAA_FULL_USERINFO_SHARD_2, TAA_FULL_USERINFOEMPSTATUS_SHARD_2 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_3_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_3, TAA_FULL_ENTERPRISECUSTOMER_SHARD_3, TAA_FULL_LLDETAIL_SHARD_3, TAA_FULL_PAYTYPE_SHARD_3, TAA_FULL_SCHEDULE_SHARD_3, TAA_FULL_TIMEOFFDATA_SHARD_3, TAA_FULL_TIMEOFFREQUEST_SHARD_3, TAA_FULL_TIMESLICEPOST_SHARD_3, TAA_FULL_USERINFOISSALARY_SHARD_3, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_3, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_3, TAA_FULL_USERINFO_SHARD_3, TAA_FULL_USERINFOEMPSTATUS_SHARD_3 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_4_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_4, TAA_FULL_ENTERPRISECUSTOMER_SHARD_4, TAA_FULL_LLDETAIL_SHARD_4, TAA_FULL_PAYTYPE_SHARD_4, TAA_FULL_SCHEDULE_SHARD_4, TAA_FULL_TIMEOFFDATA_SHARD_4, TAA_FULL_TIMEOFFREQUEST_SHARD_4, TAA_FULL_TIMESLICEPOST_SHARD_4, TAA_FULL_USERINFOISSALARY_SHARD_4, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_4, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_4, TAA_FULL_USERINFO_SHARD_4, TAA_FULL_USERINFOEMPSTATUS_SHARD_4 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_5_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_5, TAA_FULL_ENTERPRISECUSTOMER_SHARD_5, TAA_FULL_LLDETAIL_SHARD_5, TAA_FULL_PAYTYPE_SHARD_5, TAA_FULL_SCHEDULE_SHARD_5, TAA_FULL_TIMEOFFDATA_SHARD_5, TAA_FULL_TIMEOFFREQUEST_SHARD_5, TAA_FULL_TIMESLICEPOST_SHARD_5, TAA_FULL_USERINFOISSALARY_SHARD_5, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_5, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_5, TAA_FULL_USERINFO_SHARD_5, TAA_FULL_USERINFOEMPSTATUS_SHARD_5 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_6_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_6, TAA_FULL_ENTERPRISECUSTOMER_SHARD_6, TAA_FULL_LLDETAIL_SHARD_6, TAA_FULL_PAYTYPE_SHARD_6, TAA_FULL_SCHEDULE_SHARD_6, TAA_FULL_TIMEOFFDATA_SHARD_6, TAA_FULL_TIMEOFFREQUEST_SHARD_6, TAA_FULL_TIMESLICEPOST_SHARD_6, TAA_FULL_USERINFOISSALARY_SHARD_6, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_6, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_6, TAA_FULL_USERINFO_SHARD_6, TAA_FULL_USERINFOEMPSTATUS_SHARD_6 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_7_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_7, TAA_FULL_ENTERPRISECUSTOMER_SHARD_7, TAA_FULL_LLDETAIL_SHARD_7, TAA_FULL_PAYTYPE_SHARD_7, TAA_FULL_SCHEDULE_SHARD_7, TAA_FULL_TIMEOFFDATA_SHARD_7, TAA_FULL_TIMEOFFREQUEST_SHARD_7, TAA_FULL_TIMESLICEPOST_SHARD_7, TAA_FULL_USERINFOISSALARY_SHARD_7, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_7, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_7, TAA_FULL_USERINFO_SHARD_7, TAA_FULL_USERINFOEMPSTATUS_SHARD_7 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_8_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_8, TAA_FULL_ENTERPRISECUSTOMER_SHARD_8, TAA_FULL_LLDETAIL_SHARD_8, TAA_FULL_PAYTYPE_SHARD_8, TAA_FULL_SCHEDULE_SHARD_8, TAA_FULL_TIMEOFFDATA_SHARD_8, TAA_FULL_TIMEOFFREQUEST_SHARD_8, TAA_FULL_TIMESLICEPOST_SHARD_8, TAA_FULL_USERINFOISSALARY_SHARD_8, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_8, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_8, TAA_FULL_USERINFO_SHARD_8, TAA_FULL_USERINFOEMPSTATUS_SHARD_8 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_9_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_9, TAA_FULL_ENTERPRISECUSTOMER_SHARD_9, TAA_FULL_LLDETAIL_SHARD_9, TAA_FULL_PAYTYPE_SHARD_9, TAA_FULL_SCHEDULE_SHARD_9, TAA_FULL_TIMEOFFDATA_SHARD_9, TAA_FULL_TIMEOFFREQUEST_SHARD_9, TAA_FULL_TIMESLICEPOST_SHARD_9, TAA_FULL_USERINFOISSALARY_SHARD_9, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_9, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_9, TAA_FULL_USERINFO_SHARD_9, TAA_FULL_USERINFOEMPSTATUS_SHARD_9 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_10_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_10, TAA_FULL_ENTERPRISECUSTOMER_SHARD_10, TAA_FULL_LLDETAIL_SHARD_10, TAA_FULL_PAYTYPE_SHARD_10, TAA_FULL_SCHEDULE_SHARD_10, TAA_FULL_TIMEOFFDATA_SHARD_10, TAA_FULL_TIMEOFFREQUEST_SHARD_10, TAA_FULL_TIMESLICEPOST_SHARD_10, TAA_FULL_USERINFOISSALARY_SHARD_10, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_10, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_10, TAA_FULL_USERINFO_SHARD_10, TAA_FULL_USERINFOEMPSTATUS_SHARD_10 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_11_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_11, TAA_FULL_ENTERPRISECUSTOMER_SHARD_11, TAA_FULL_LLDETAIL_SHARD_11, TAA_FULL_PAYTYPE_SHARD_11, TAA_FULL_SCHEDULE_SHARD_11, TAA_FULL_TIMEOFFDATA_SHARD_11, TAA_FULL_TIMEOFFREQUEST_SHARD_11, TAA_FULL_TIMESLICEPOST_SHARD_11, TAA_FULL_USERINFOISSALARY_SHARD_11, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_11, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_11, TAA_FULL_USERINFO_SHARD_11, TAA_FULL_USERINFOEMPSTATUS_SHARD_11 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_12_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_12, TAA_FULL_ENTERPRISECUSTOMER_SHARD_12, TAA_FULL_LLDETAIL_SHARD_12, TAA_FULL_PAYTYPE_SHARD_12, TAA_FULL_SCHEDULE_SHARD_12, TAA_FULL_TIMEOFFDATA_SHARD_12, TAA_FULL_TIMEOFFREQUEST_SHARD_12, TAA_FULL_TIMESLICEPOST_SHARD_12, TAA_FULL_USERINFOISSALARY_SHARD_12, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_12, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_12, TAA_FULL_USERINFO_SHARD_12, TAA_FULL_USERINFOEMPSTATUS_SHARD_12 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_13_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_13, TAA_FULL_ENTERPRISECUSTOMER_SHARD_13, TAA_FULL_LLDETAIL_SHARD_13, TAA_FULL_PAYTYPE_SHARD_13, TAA_FULL_SCHEDULE_SHARD_13, TAA_FULL_TIMEOFFDATA_SHARD_13, TAA_FULL_TIMEOFFREQUEST_SHARD_13, TAA_FULL_TIMESLICEPOST_SHARD_13, TAA_FULL_USERINFOISSALARY_SHARD_13, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_13, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_13, TAA_FULL_USERINFO_SHARD_13, TAA_FULL_USERINFOEMPSTATUS_SHARD_13 AS SELECT 1;
CREATE OR REPLACE TASK TAA_FULL_SHARD_14_GATE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_CUSTOMER_SHARD_14, TAA_FULL_ENTERPRISECUSTOMER_SHARD_14, TAA_FULL_LLDETAIL_SHARD_14, TAA_FULL_PAYTYPE_SHARD_14, TAA_FULL_SCHEDULE_SHARD_14, TAA_FULL_TIMEOFFDATA_SHARD_14, TAA_FULL_TIMEOFFREQUEST_SHARD_14, TAA_FULL_TIMESLICEPOST_SHARD_14, TAA_FULL_USERINFOISSALARY_SHARD_14, TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_14, TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14, TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14, TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_14, TAA_FULL_USERINFO_SHARD_14, TAA_FULL_USERINFOEMPSTATUS_SHARD_14 AS SELECT 1;

-- =============================================================================
-- STEP 5: CREATE FINALIZE TASK
-- =============================================================================
CREATE OR REPLACE TASK TAA_FULL_FINALIZE WAREHOUSE = WH_DSDP_ETL_PR AFTER TAA_FULL_SHARD_1_GATE, TAA_FULL_SHARD_2_GATE, TAA_FULL_SHARD_3_GATE, TAA_FULL_SHARD_4_GATE, TAA_FULL_SHARD_5_GATE, TAA_FULL_SHARD_6_GATE, TAA_FULL_SHARD_7_GATE, TAA_FULL_SHARD_8_GATE, TAA_FULL_SHARD_9_GATE, TAA_FULL_SHARD_10_GATE, TAA_FULL_SHARD_11_GATE, TAA_FULL_SHARD_12_GATE, TAA_FULL_SHARD_13_GATE, TAA_FULL_SHARD_14_GATE AS CALL INGEST_TAA_FULL_LOAD_FINALIZE();

-- =============================================================================
-- RESUME ALL 240 TASKS - Wave-Based Shard Execution Architecture
-- Database: DL_P_STRATUSTIME_PR
-- Schema: TAA
-- Order: ROOT -> COORDINATORS -> SHARDS -> GATES -> FINALIZE
-- =============================================================================

-- =============================================================================
-- RESUME ROOT TASK
-- =============================================================================
ALTER TASK TAA_FULL_ROOT SUSPEND;
--ALTER TASK TAA_FULL_ROOT RESUME;

-- =============================================================================
-- RESUME 14 COORDINATOR_SHARD TASKS
-- =============================================================================
ALTER TASK TAA_FULL_SHARD_1_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_2_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_3_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_4_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_5_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_6_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_7_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_8_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_9_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_10_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_11_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_12_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_13_COORDINATOR RESUME;
ALTER TASK TAA_FULL_SHARD_14_COORDINATOR RESUME;

-- =============================================================================
-- RESUME 210 SHARD TASKS (15 Tables × 14 Shards)
-- =============================================================================

-- SHARD 1
ALTER TASK TAA_FULL_CUSTOMER_SHARD_1 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_1 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_1 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_1 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_1 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_1 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_1 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_1 RESUME;

-- SHARD 2
ALTER TASK TAA_FULL_CUSTOMER_SHARD_2 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_2 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_2 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_2 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_2 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_2 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_2 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_2 RESUME;

-- SHARD 3
ALTER TASK TAA_FULL_CUSTOMER_SHARD_3 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_3 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_3 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_3 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_3 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_3 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_3 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_3 RESUME;

-- SHARD 4
ALTER TASK TAA_FULL_CUSTOMER_SHARD_4 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_4 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_4 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_4 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_4 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_4 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_4 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_4 RESUME;

-- SHARD 5
ALTER TASK TAA_FULL_CUSTOMER_SHARD_5 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_5 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_5 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_5 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_5 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_5 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_5 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_5 RESUME;

-- SHARD 6
ALTER TASK TAA_FULL_CUSTOMER_SHARD_6 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_6 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_6 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_6 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_6 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_6 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_6 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_6 RESUME;

-- SHARD 7
ALTER TASK TAA_FULL_CUSTOMER_SHARD_7 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_7 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_7 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_7 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_7 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_7 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_7 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_7 RESUME;

-- SHARD 8
ALTER TASK TAA_FULL_CUSTOMER_SHARD_8 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_8 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_8 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_8 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_8 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_8 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_8 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_8 RESUME;

-- SHARD 9
ALTER TASK TAA_FULL_CUSTOMER_SHARD_9 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_9 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_9 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_9 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_9 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_9 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_9 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_9 RESUME;

-- SHARD 10
ALTER TASK TAA_FULL_CUSTOMER_SHARD_10 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_10 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_10 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_10 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_10 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_10 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_10 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_10 RESUME;

-- SHARD 11
ALTER TASK TAA_FULL_CUSTOMER_SHARD_11 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_11 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_11 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_11 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_11 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_11 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_11 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_11 RESUME;

-- SHARD 12
ALTER TASK TAA_FULL_CUSTOMER_SHARD_12 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_12 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_12 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_12 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_12 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_12 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_12 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_12 RESUME;

-- SHARD 13
ALTER TASK TAA_FULL_CUSTOMER_SHARD_13 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_13 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_13 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_13 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_13 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_13 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_13 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_13 RESUME;

-- SHARD 14
ALTER TASK TAA_FULL_CUSTOMER_SHARD_14 RESUME;
ALTER TASK TAA_FULL_ENTERPRISECUSTOMER_SHARD_14 RESUME;
ALTER TASK TAA_FULL_LLDETAIL_SHARD_14 RESUME;
ALTER TASK TAA_FULL_PAYTYPE_SHARD_14 RESUME;
ALTER TASK TAA_FULL_SCHEDULE_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMEOFFDATA_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUEST_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOST_SHARD_14 RESUME;
ALTER TASK TAA_FULL_USERINFOISSALARY_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMEOFFREQUESTDETAIL_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14 RESUME;
ALTER TASK TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14 RESUME;
ALTER TASK TAA_FULL_USERINFOPAYROLLMAPPING_SHARD_14 RESUME;
ALTER TASK TAA_FULL_USERINFO_SHARD_14 RESUME;
ALTER TASK TAA_FULL_USERINFOEMPSTATUS_SHARD_14 RESUME;

-- =============================================================================
-- RESUME 14 GATE_SHARD TASKS
-- =============================================================================
ALTER TASK TAA_FULL_SHARD_1_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_2_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_3_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_4_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_5_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_6_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_7_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_8_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_9_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_10_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_11_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_12_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_13_GATE RESUME;
ALTER TASK TAA_FULL_SHARD_14_GATE RESUME;

-- =============================================================================
-- RESUME FINALIZE TASK
-- =============================================================================
ALTER TASK TAA_FULL_FINALIZE RESUME;

-- =============================================================================
-- SUMMARY
-- =============================================================================
-- Total Tasks Resumed: 240
-- - 1 ROOT
-- - 14 COORDINATOR_SHARD tasks
-- - 210 Shard tasks (15 tables × 14 shards)
-- - 14 GATE_SHARD tasks
-- - 1 FINALIZE
-- =============================================================================


-- =============================================================================
-- RESUME ALL DELTA TASKS (210 Total)
-- =============================================================================
-- This will resume all delta load tasks that were previously suspended.
-- Note: Resume from the root task down to ensure proper dependency ordering.

-- =============================================================================
-- STEP 1: RESUME ROOT TASK
-- =============================================================================
-- ALTER TASK IF EXISTS TAA_DELTA_ROOT RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ROOT SUSPEND;

-- =============================================================================
-- STEP 2: RESUME 14 COORDINATOR_SHARD TASKS
-- =============================================================================
ALTER TASK IF EXISTS TAA_DELTA_SHARD_1_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_2_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_3_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_4_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_5_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_6_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_7_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_8_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_9_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_10_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_11_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_12_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_13_COORDINATOR RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_14_COORDINATOR RESUME;

-- =============================================================================
-- STEP 3: RESUME 210 SHARD TASKS (15 Tables × 14 Shards)
-- =============================================================================

-- ============= SHARD 1 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_1 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_1 RESUME;

-- ============= SHARD 2 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_2 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_2 RESUME;

-- ============= SHARD 3 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_3 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_3 RESUME;

-- ============= SHARD 4 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_4 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_4 RESUME;

-- ============= SHARD 5 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_5 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_5 RESUME;

-- ============= SHARD 6 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_6 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_6 RESUME;

-- ============= SHARD 7 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_7 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_7 RESUME;

-- ============= SHARD 8 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_8 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_8 RESUME;

-- ============= SHARD 9 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_9 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_9 RESUME;

-- ============= SHARD 10 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_10 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_10 RESUME;

-- ============= SHARD 11 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_11 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_11 RESUME;

-- ============= SHARD 12 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_12 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_12 RESUME;

-- ============= SHARD 13 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_13 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_13 RESUME;

-- ============= SHARD 14 TASKS =============
ALTER TASK IF EXISTS TAA_DELTA_CUSTOMER_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_LLDETAIL_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_PAYTYPE_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SCHEDULE_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFDATA_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOST_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOISSALARY_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFO_SHARD_14 RESUME;
ALTER TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS_SHARD_14 RESUME;

-- =============================================================================
-- STEP 4: RESUME 14 GATE_SHARD TASKS
-- =============================================================================
ALTER TASK IF EXISTS TAA_DELTA_SHARD_1_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_2_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_3_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_4_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_5_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_6_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_7_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_8_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_9_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_10_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_11_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_12_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_13_GATE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_SHARD_14_GATE RESUME;

-- =============================================================================
-- STEP 5: RESUME FINALIZE TASK
-- =============================================================================
ALTER TASK IF EXISTS TAA_DELTA_FINALIZE RESUME;
ALTER TASK IF EXISTS TAA_DELTA_ROOT SUSPEND;
ALTER TASK IF EXISTS TAA_FULL_ROOT SUSPEND;

DROP TASK IF EXISTS TAA_DELTA_CUSTOMER;
DROP TASK IF EXISTS TAA_DELTA_ENTERPRISECUSTOMER;
DROP TASK IF EXISTS TAA_DELTA_LLDETAIL;
DROP TASK IF EXISTS TAA_DELTA_PAYTYPE;
DROP TASK IF EXISTS TAA_DELTA_SCHEDULE;
DROP TASK IF EXISTS TAA_DELTA_TIMEOFFDATA;
DROP TASK IF EXISTS TAA_DELTA_TIMEOFFREQUEST;
DROP TASK IF EXISTS TAA_DELTA_TIMEOFFREQUESTDETAIL;
DROP TASK IF EXISTS TAA_DELTA_TIMESLICEPOST;
DROP TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTEXCEPTIONDETAIL;
DROP TASK IF EXISTS TAA_DELTA_TIMESLICEPOSTSHIFTDIFFDETAIL;
DROP TASK IF EXISTS TAA_DELTA_USERINFO;
DROP TASK IF EXISTS TAA_DELTA_USERINFOEMPSTATUS;
DROP TASK IF EXISTS TAA_DELTA_USERINFOISSALARY;
DROP TASK IF EXISTS TAA_DELTA_USERINFOPAYROLLMAPPING;
DROP TASK IF EXISTS TAA_FULL_CUSTOMER;
DROP TASK IF EXISTS TAA_FULL_ENTERPRISECUSTOMER;
DROP TASK IF EXISTS TAA_FULL_LLDETAIL;
DROP TASK IF EXISTS TAA_FULL_PAYTYPE;
DROP TASK IF EXISTS TAA_FULL_SCHEDULE;
DROP TASK IF EXISTS TAA_FULL_TIMEOFFDATA;
DROP TASK IF EXISTS TAA_FULL_TIMEOFFREQUEST;
DROP TASK IF EXISTS TAA_FULL_TIMEOFFREQUESTDETAIL;
DROP TASK IF EXISTS TAA_FULL_TIMESLICEPOST;
DROP TASK IF EXISTS TAA_FULL_TIMESLICEPOSTEXCEPTIONDETAIL;
DROP TASK IF EXISTS TAA_FULL_TIMESLICEPOSTSHIFTDIFFDETAIL;
DROP TASK IF EXISTS TAA_FULL_USERINFO;
DROP TASK IF EXISTS TAA_FULL_USERINFOEMPSTATUS;
DROP TASK IF EXISTS TAA_FULL_USERINFOISSALARY;
DROP TASK IF EXISTS TAA_FULL_USERINFOPAYROLLMAPPING;

ALTER TASK IF EXISTS TAA_DELTA_ROOT RESUME;
