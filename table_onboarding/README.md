# TAA Pipeline — Table Onboarding Tool

Generates deployment-ready SQL for adding a new table to both the TAA full Parquet load pipeline and the TAA delta CSV load pipeline. Fill in a JSON config describing the table; the tool produces a single `.sql` file containing every object needed, in the correct deployment order.

---

## Files

| File | Description |
|---|---|
| `table_onboarding_tool.py` | The generator script |
| `table_config_template.json` | Blank config template with full field documentation |
| `example_userinfoissalary.json` | Working example — run this to validate output against the live pipeline code |

---

## Prerequisites

- Python 3.8+
- No third-party packages required (standard library only)

---

## Quick Start

```bash
# 1. Copy the template and fill it in
cp table_config_template.json my_new_table.json

# 2. Edit my_new_table.json with the new table's details

# 3. Generate the SQL
python table_onboarding_tool.py my_new_table.json

# Output: onboard_MY_NEW_TABLE.sql (same directory as the config)

# Optional: specify an output path explicitly
python table_onboarding_tool.py my_new_table.json --output path/to/output.sql
```

To validate the tool against the existing live pipeline code:
```bash
python table_onboarding_tool.py example_userinfoissalary.json
# Produces onboard_USERINFOISSALARY.sql -- compare against the live procedures
```

---

## What Gets Generated

One `.sql` file containing all required objects in deployment order:

| # | Object | Description |
|---|---|---|
| 1 | Target table DDL | `CREATE OR REPLACE TABLE <TABLE_NAME>` with all columns and PK constraint |
| 2 | `FULL_LOAD_<TABLE>` | Full Parquet load procedure: reads manifest, batched COPY INTO, per-file audit |
| 3 | Config INSERT | `INSERT INTO INGEST_TAA_TABLE_CONFIG` registering the new table |
| 4 | Full load task DDL | `CREATE OR REPLACE TASK TAA_FL_W[1\|2]_<TABLE>` |
| 5 | Full load DAG wiring | `ALTER TASK TAA_FL_FINALIZE ADD AFTER` + RESUME statements |
| 6 | `STG_DELTA_<TABLE>` | Staging table for CSV delta COPY INTO |
| 7 | `DELTA_LOAD_<TABLE>` | Delta procedure: COPY to staging, single MERGE with LSN dedup + change_type routing |
| 8 | Delta task DDL | `CREATE OR REPLACE TASK TAA_DL_W[1\|2]_<TABLE>` |
| 9 | Delta DAG wiring | `ALTER TASK TAA_DL_FINALIZE ADD AFTER` + RESUME statements |

---

## Config File Reference

See `table_config_template.json` for the full annotated template. Key fields:

### Top-level fields

| Field | Required | Description |
|---|---|---|
| `table_name` | ✅ | Snowflake target table name. Uppercased automatically. |
| `table_uuid` | ✅ | UUID from the OneLake file path: `.../Tables/<UUID>/TableData_*/`. Used to filter the manifest by table. |
| `columns` | ✅ | Ordered list of column definitions (see below). |
| `is_multi_tenant` | | `true` if `DATABASEPHYSICALNAME` is derived from the file path (default, most tables). `false` for non-tenant tables like `CUSTOMER` and `ENTERPRISECUSTOMER` where it is a real payload column. |
| `wave` | | `1` = no FK dependency (runs in parallel with all other Wave 1 tables). `2` = must wait for a specific Wave 1 table. Default: `1`. |
| `wave2_parent` | | Required when `wave: 2`. The Wave 1 table this table depends on (e.g. `"USERINFO"`). |
| `load_order` | | Numeric order in `INGEST_TAA_TABLE_CONFIG`. Used in sequential/filtered contexts. Suggest `10` for Wave 1, `20` for Wave 2. Default: `10`. |
| `is_active_full_load` | | Whether this table participates in full loads. Default: `true`. |
| `is_active_delta_load` | | Whether this table participates in delta loads. Default: `true`. |
| `description` | | Free-text description stored in `INGEST_TAA_TABLE_CONFIG`. |
| `warehouse` | | Snowflake warehouse for task execution. Default: `WH_DS_AUTOMATION_TST`. |

### Column fields

| Field | Required | Description |
|---|---|---|
| `name` | ✅ | Column name exactly as it should appear in Snowflake DDL. |
| `sf_type` | ✅ | Snowflake data type — e.g. `NUMBER(38,0)`, `VARCHAR(16777216)`, `BOOLEAN`, `TIMESTAMP_NTZ(9)`. |
| `pk` | | `true` if this column is part of the primary key. Used for the PK constraint, MERGE ON clause, and QUALIFY PARTITION BY. |
| `not_null` | | `true` adds a `NOT NULL` constraint. Default: `true`. Set `false` for nullable columns. |

### Multi-tenant vs. non-multi-tenant tables

**Multi-tenant** (`is_multi_tenant: true`) — the majority of tables:
- `DATABASEPHYSICALNAME` is **not** a column in the Parquet payload or CSV delta file.
- It is derived at load time from the file path using `REGEXP_SUBSTR(METADATA$FILENAME, '/([^/]+)/Tables/', ...)`.
- Include `DATABASEPHYSICALNAME` as the first column in the `columns` array with `pk: true`.

**Non-multi-tenant** (`is_multi_tenant: false`) — `CUSTOMER`, `ENTERPRISECUSTOMER`:
- `DATABASEPHYSICALNAME` (or equivalent) **is** a real column in the data files.
- It occupies a positional CSV slot like any other column.
- Client-scoped full loads skip these tables automatically (the pipeline handles this).

### CSV column positions (delta load)

Delta CSV files always have four metadata columns before the data payload:

| Position | Content |
|---|---|
| `$1` | Hex-encoded LSN (Log Sequence Number) |
| `$2` | Internal metadata |
| `$3` | `CHANGE_TYPE` (1=DELETE, 2=INSERT, 3=skip, 4=UPDATE) |
| `$4` | Internal metadata |
| `$5`+ | Actual table data columns in source schema order |

For multi-tenant tables, `DATABASEPHYSICALNAME` does **not** occupy a `$N` slot — it is derived from `METADATA$FILENAME`. The tool handles this automatically. The `columns` array should list data columns starting from the first real data field; the tool maps them to `$5`, `$6`, `$7`, ... in order.

> **Important:** Always verify the generated CSV positional mapping against a real ChangeData file before deploying. Run a `LIST @<stage> PATTERN='.*<TABLE_ID>.*ChangeData.*'` and inspect a sample file if you are unsure of the column order.

---

## Deployment Process

The ALTER TASK statements in the generated script briefly suspend and resume both DAG root tasks (`TAA_FL_ROOT` and `TAA_DL_ROOT`). Perform the deployment steps in order, ideally during a maintenance window or when no pipeline run is in progress.

### Step 1 — Review the generated SQL
Open the output file and verify:
- Column names and types match the Snowflake target schema
- `table_uuid` appears correctly in the manifest `WHERE table_id =` filters
- CSV column positions in `DELTA_LOAD_<TABLE>` match the actual file format
- Wave assignment and `AFTER` dependency are correct

### Step 2 — Deploy the full load objects
```sql
-- Run from the generated file, full load section only:
-- 1. Target table DDL
-- 2. FULL_LOAD_<TABLE> procedure
-- 3. INGEST_TAA_TABLE_CONFIG INSERT
-- 4. TAA_FL_W[1|2]_<TABLE> task DDL
-- 5. ALTER TASK TAA_FL_FINALIZE ADD AFTER ... / RESUME
```

### Step 3 — Trigger a full load and verify
```sql
CALL INGEST_TAA_LAUNCH_FULL_LOAD(NULL, 'NEW_TABLE_NAME', 'EXST_STRATUSTIME_ONELAKE_N2A');
```
Check `INGEST_TAA_FILE_AUDIT` for rows with the new table's UUID and `LOAD_STATUS = 'SUCCESS'`.  
Check `INGEST_TAA_FULL_LOAD_STATE` — a row for the new table must exist before delta loads are eligible.

### Step 4 — Deploy the delta load objects
```sql
-- Run from the generated file, delta load section only:
-- 1. STG_DELTA_<TABLE> staging table DDL
-- 2. DELTA_LOAD_<TABLE> procedure
-- 3. TAA_DL_W[1|2]_<TABLE> task DDL
-- 4. ALTER TASK TAA_DL_FINALIZE ADD AFTER ... / RESUME
```

### Step 5 — Verify delta load
```sql
CALL INGEST_TAA_LAUNCH_DELTA_LOAD(NULL, 'NEW_TABLE_NAME', 'EXST_STRATUSTIME_ONELAKE_N2A');
```
Monitor via task history and check `INGEST_TAA_FILE_AUDIT` for delta file records.

---

## Type Mapping Reference

The tool maps Snowflake types to the appropriate cast expressions automatically:

| Snowflake type | Parquet cast (`FULL_LOAD`) | CSV cast (`DELTA_LOAD`) |
|---|---|---|
| `NUMBER(38,0)` | `$1:Col::NUMBER(38,0)` | `$N::NUMBER(38,0)` |
| `VARCHAR(...)` / `TEXT` | `$1:Col::TEXT` | `$N::TEXT` |
| `BOOLEAN` | `$1:Col::BOOLEAN` | `$N::BOOLEAN` |
| `TIMESTAMP_NTZ(9)` | `$1:Col::TIMESTAMP_NTZ` | `TRY_TO_TIMESTAMP_NTZ($N)` |
| `TIMESTAMP_TZ(9)` | `$1:Col::TIMESTAMP_TZ` | `TRY_TO_TIMESTAMP_NTZ($N)` |
| `FLOAT` / `DOUBLE` | `$1:Col::FLOAT` | `$N::FLOAT` |
| `DATE` | `$1:Col::DATE` | `TRY_TO_DATE($N)` |

`TRY_TO_*` wrappers are used for date/time types in CSV loads to avoid hard failures on malformed timestamps — consistent with the existing pipeline pattern.

---

## Example — Adding a Wave 2 Table

```json
{
    "table_name":          "NEWCHILDTABLE",
    "table_uuid":          "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "is_multi_tenant":     true,
    "wave":                2,
    "wave2_parent":        "USERINFO",
    "load_order":          20,
    "is_active_full_load": true,
    "is_active_delta_load":true,
    "description":         "Child records linked to USERINFO.",
    "columns": [
        { "name": "DATABASEPHYSICALNAME", "sf_type": "VARCHAR(16777216)", "pk": true,  "not_null": true },
        { "name": "NEWCHILDTABLEID",      "sf_type": "NUMBER(38,0)",       "pk": true,  "not_null": true },
        { "name": "USERID",               "sf_type": "NUMBER(38,0)",       "pk": false, "not_null": true },
        { "name": "SOMEVALUE",            "sf_type": "VARCHAR(500)",        "pk": false, "not_null": false },
        { "name": "MODIFIEDON",           "sf_type": "TIMESTAMP_NTZ(9)",   "pk": false, "not_null": true }
    ]
}
```

This generates:
- A task `TAA_FL_W2_NEWCHILDTABLE` that runs `AFTER TAA_FL_W1_USERINFO`
- A task `TAA_DL_W2_NEWCHILDTABLE` that runs `AFTER TAA_DL_W1_USERINFO`
- MERGE deduplicating on `(DATABASEPHYSICALNAME, NEWCHILDTABLEID)` with LSN DESC
- CSV positions: `$5=NEWCHILDTABLEID`, `$6=USERID`, `$7=SOMEVALUE`, `$8=MODIFIEDON`
