USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

CREATE OR REPLACE FILE FORMAT FF_TAA_ONELAKE_PARQUET
TYPE = PARQUET;

-- CSV format for the Fabric OneLake file-inventory files produced by
-- Fabric_N2_File_Ingestion_Inventory.  The inventory CSVs have one header row
-- and contain FilePath + ModifiedDate columns (no quoting, no embedded commas).
CREATE OR REPLACE FILE FORMAT FF_TAA_INVENTORY_CSV
    TYPE                         = CSV
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                      = ('');

--
