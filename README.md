# taa-snowflake-pipeline

CI/CD repository for the **TAA → Snowflake** ingestion pipeline. This repo manages all Snowflake object definitions (tables, procedures, task DAGs) for both the **full Parquet load** and **delta CSV load** pipelines, along with the tooling to deploy and onboard new tables.

---

## What this is

**TAA (Time and Attendance Application)** is a multi-tenant SaaS platform. Client data is exported from TAA on a scheduled basis and lands in **Microsoft OneLake** (Fabric) as raw files — Parquet snapshots for full loads and CSV change-data files for deltas. This pipeline picks up those files and loads them into **Snowflake** for downstream analytics and reporting.

### Data flow

```
TAA (multi-tenant)
      │
      │  nightly export
      ▼
Microsoft OneLake
  LandingZone/
    <client_id>/
      Tables/
        <table_uuid>/
          TableData_*.parquet   ← full load snapshots
          ChangeData_*.csv      ← delta / CDC files
      │
      │  Snowflake external stage
      ▼
Snowflake (this pipeline)
  ├── Full load  — replaces target tables from Parquet snapshots
  └── Delta load — applies CDC changes (INSERT / UPDATE / DELETE)
        from CSV files to target tables via MERGE
```

### Two pipelines, one codebase

| | Full load | Delta load |
|---|---|---|
| **Source format** | Parquet | CSV (change-data capture) |
| **Trigger** | Snowflake Task DAG (`TAA_FL_ROOT`) | Snowflake Task DAG (`TAA_DL_ROOT`) |
| **Orchestration** | `INGEST_TAA_LAUNCH_FULL_LOAD` → `INGEST_TAA_FULL_LOAD_PREPARE` → wave tasks → `INGEST_TAA_FULL_LOAD_FINALIZE` | `INGEST_TAA_LAUNCH_DELTA_LOAD` → `INGEST_TAA_DELTA_PREPARE` → wave tasks → `INGEST_TAA_DELTA_FINALIZE` |
| **Per-table procedure** | `FULL_LOAD_<TABLE>` — COPY INTO target | `DELTA_LOAD_<TABLE>` — COPY INTO staging → MERGE |
| **Deduplication** | Manifest skips already-audited files | QUALIFY on LSN DESC before MERGE |
| **Multi-tenancy** | `DATABASEPHYSICALNAME` derived from file path | Same |

### Task DAG structure

Both pipelines use a **two-wave fan-out DAG** to maximize parallelism while respecting FK dependencies:

```
ROOT
 └── PREPARE
      ├── CUSTOMER
      ├── ENTERPRISECUSTOMER
      ├── PAYTYPE
      ├── SCHEDULE
      ├── TIMEOFFDATA
      ├── USERINFO          
      ├── USERINFOISSALARY  
      ├── TIMEOFFREQUEST    
      ├── TIMESLICEPOST     
      ├── ...               
      │
      └── FINALIZE
```

### Key Snowflake objects

| Object | Purpose |
|---|---|
| `INGEST_TAA_TABLE_CONFIG` | Registry of all tables managed by the pipeline, with UUIDs, wave assignments, and active flags |
| `INGEST_TAA_FILE_AUDIT` | Per-file load history — used to skip already-processed files on re-runs |
| `STAGE_TAA_FULL_FILE_MANIFEST` | Staging table populated at the start of each full load run with all Parquet files to process |
| `STAGE_TAA_DELTA_MANIFEST` | Same for delta CSV files |
| `INGEST_TAA_FULL_LOAD_STATE` | Tracks last successful full load per table — gates delta loads from running before a full load baseline exists |
| `STG_DELTA_<TABLE>` | Per-table staging table for CSV COPY before the MERGE step |
| `FF_TAA_ONELAKE_PARQUET` | Snowflake file format for Parquet reads |
| `FF_TAA_ONELAKE_CSV` | Snowflake file format for CSV reads |

---