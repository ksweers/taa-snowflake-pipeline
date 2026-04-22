USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE PROCEDURE FULL_LOAD_USERINFOISSALARY(
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
            WHERE table_id = ''ab18c18c-ccff-62b6-4975-156ffc566ef8''
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
