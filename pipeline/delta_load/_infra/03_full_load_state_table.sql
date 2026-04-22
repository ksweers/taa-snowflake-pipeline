USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TABLE INGEST_TAA_FULL_LOAD_STATE (
    CLIENT_ID               VARCHAR(16777216)   NOT NULL,
    TABLE_ID                VARCHAR(36)         NOT NULL,
    TABLEDATA_FOLDER        VARCHAR(16777216)   NOT NULL,  -- TableData_* folder of last full load file
    FULL_LOAD_FILE          VARCHAR(16777216)   NOT NULL,  -- filename of the full parquet loaded
    FULL_LOAD_LAST_MODIFIED TIMESTAMP_TZ(9)    NOT NULL,  -- last_modified of that parquet file
    STATE_UPDATED_AT        TIMESTAMP_NTZ(9)   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_INGEST_TAA_FULL_LOAD_STATE PRIMARY KEY (CLIENT_ID, TABLE_ID)
);


-- =============================================================================
-- PROCEDURE: BUILD_STAGE_TAA_DELTA_MANIFEST
-- =============================================================================
-- Lists the stage for ChangeData CSV files and inserts only those that:
--   (a) belong to the same TableData_* folder as the last full load, and
--   (b) have last_modified AFTER the full load file's last_modified, and
--   (c) have not already been recorded in INGEST_TAA_FILE_AUDIT.
-- Ordered ascending by last_modified so DELTA_LOAD procs apply changes in order.
-- =============================================================================
