USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TABLE STAGE_TAA_DELTA_MANIFEST (
    FULL_FILE_PATH      VARCHAR(16777216),
    CLIENT_ID           VARCHAR(16777216),
    TABLE_ID            VARCHAR(16777216),
    TABLEDATA_FOLDER    VARCHAR(16777216),   -- e.g. TableData_00009694000009e80002
    FILENAME            VARCHAR(16777216),
    LAST_MODIFIED       TIMESTAMP_TZ(9)
);

-- =============================================================================
-- INFRASTRUCTURE: Full load state tracking
-- Records the last_modified timestamp and TableData_* folder of the full
-- Parquet file that was loaded per client/table. Used to gate which delta
-- files are eligible (only CSVs newer than the full load, same folder).
-- Populated/updated by INGEST_TAA_FULL_LOAD at the end of each full run.
-- =============================================================================
