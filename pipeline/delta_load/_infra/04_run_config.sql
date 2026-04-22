USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TABLE INGEST_TAA_DELTA_RUN_CONFIG (
    PARAM_NAME   VARCHAR(100)      NOT NULL,
    PARAM_VALUE  VARCHAR(16777216),
    UPDATED_AT   TIMESTAMP_NTZ(9)  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_INGEST_TAA_DELTA_RUN_CONFIG PRIMARY KEY (PARAM_NAME)
);

-- Seed the three parameter rows.
MERGE INTO INGEST_TAA_DELTA_RUN_CONFIG tgt
USING (
    SELECT * FROM VALUES
        ('STAGE_NAME',        CAST(NULL AS VARCHAR)),
        ('CLIENT_ID_FILTER',  CAST(NULL AS VARCHAR)),
        ('TABLE_NAME_FILTER', CAST(NULL AS VARCHAR))
    AS src(PARAM_NAME, PARAM_VALUE)
) src ON tgt.PARAM_NAME = src.PARAM_NAME
WHEN NOT MATCHED THEN INSERT (PARAM_NAME, PARAM_VALUE)
VALUES (src.PARAM_NAME, src.PARAM_VALUE);


-- =============================================================================
-- PROCEDURE: DELTA_LOAD_FROM_CONFIG  (internal task dispatcher)
-- =============================================================================
-- Called by each Wave 1 / Wave 2 task.
-- Reads STAGE_NAME from INGEST_TAA_DELTA_RUN_CONFIG then dispatches to the
-- corresponding DELTA_LOAD_<TABLE> procedure.
-- =============================================================================
