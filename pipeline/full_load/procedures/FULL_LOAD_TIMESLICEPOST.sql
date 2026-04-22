USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE FULL_LOAD_TIMESLICEPOST(
    STAGE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var total_rows_loaded = 0;
    var files_processed = 0;
    var total_files_identified = 0;
    var load_start_time = new Date();

    try {
        // Get all files to load and build the FILES clause
        var get_files_query = `
            SELECT 
                SUBSTRING(full_file_path, POSITION(''/LandingZone/'' IN full_file_path)) AS relative_path,
                client_id,
                table_id,
                filename,
                full_file_path
            FROM STAGE_TAA_FULL_FILE_MANIFEST
            WHERE table_id = ''0b30f4a8-bf11-0296-664d-a6996e0dca32''
            ORDER BY client_id, table_id
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
                
                // Insert audit record
                var audit_insert = `
                    INSERT INTO INGEST_TAA_FILE_AUDIT 
                    (file_name, client_id, table_id, rows_loaded, batch_number, load_status, error_message, full_stage_path)
                    VALUES (
                        ''` + safe_filename + `'',
                        ''` + safe_client_id + `'',
                        ''` + safe_table_id + `'',
                        ` + rows_loaded + `,
                        ` + (batch_num + 1) + `,
                        ''` + load_status + `'',
                        ` + (safe_error ? "''" + safe_error + "''" : "NULL") + `,
                        ''` + safe_full_path + `''
                    )
                `;
                
                snowflake.createStatement({sqlText: audit_insert}).execute();
                
                total_rows_loaded += rows_loaded;
                files_processed++;
            }
        }

        return "Load complete. Processed " + batch_count + " batch(es). Files processed: " + files_processed + " out of " + total_files_identified + " identified, Total rows loaded: " + total_rows_loaded;

    } catch (err) {
        throw new Error(err.message);
    }
';

--
