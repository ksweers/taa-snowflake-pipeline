USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

=============================================================================
-- INFRASTRUCTURE: File format for CSV delta files
-- =============================================================================

CREATE OR REPLACE FILE FORMAT FF_TAA_ONELAKE_CSV
    TYPE            = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER     = 0          -- no header row in delta files
    NULL_IF         = ('NULL', 'null')   -- only convert literal NULL/null; empty strings preserved as-is
    EMPTY_FIELD_AS_NULL = FALSE          -- empty strings must not become NULL (would violate NOT NULL columns)
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'   -- strip enclosing double quotes before length/type validation
    TRIM_SPACE      = FALSE;

-- =============================================================================
-- INFRASTRUCTURE: Delta file manifest
-- Mirrors STAGE_TAA_FULL_FILE_MANIFEST but for CSV delta files.
-- Populated fresh each run by BUILD_STAGE_TAA_DELTA_MANIFEST.
-- =============================================================================
