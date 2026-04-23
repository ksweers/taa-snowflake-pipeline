USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE TABLE INGEST_TAA_RUN_CONFIG (
    PARAM_NAME   VARCHAR(100)      NOT NULL,
    PARAM_VALUE  VARCHAR(16777216),
    UPDATED_AT   TIMESTAMP_NTZ(9)  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_INGEST_TAA_RUN_CONFIG PRIMARY KEY (PARAM_NAME)
);

-- Seed the three parameter rows so MERGE in the launcher always finds them.
MERGE INTO INGEST_TAA_RUN_CONFIG tgt
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
-- PROCEDURE: INGEST_TAA_FULL_LOAD_PREPARE
-- =============================================================================
-- No-argument payload for TAA_FULL_ROOT.
-- Reads run parameters from INGEST_TAA_RUN_CONFIG then:
--   1. Clears target tables (multi-tenant: DELETE per client; others: TRUNCATE).
--   2. Calls BUILD_STAGE_TAA_FULL_FILE_MANIFEST.
-- =============================================================================
