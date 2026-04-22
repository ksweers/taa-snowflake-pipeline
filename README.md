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
      ├── W1_CUSTOMER
      ├── W1_ENTERPRISECUSTOMER
      ├── W1_PAYTYPE
      ├── W1_SCHEDULE
      ├── W1_TIMEOFFDATA
      ├── W1_USERINFO          ← Wave 1: no FK deps
      │
      ├── W2_USERINFOISSALARY  ─┐
      ├── W2_TIMEOFFREQUEST    ─┤
      ├── W2_TIMESLICEPOST     ─┤ Wave 2: depend on Wave 1
      ├── ...                  ─┘
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

## Repository structure

```
taa-snowflake-pipeline/
├── .github/
│   └── workflows/
│       ├── validate.yml        # PR validation: dry-run deploy + config checks
│       └── deploy.yml          # Deployment: fires on merge to main / staging
│
├── pipeline/
│   ├── full_load/
│   │   ├── _infra/             # File format, manifest table, config table, audit table
│   │   ├── procedures/         # One file per FULL_LOAD_<TABLE> procedure
│   │   └── tasks/              # task_dag.sql — full DAG definition + RESUME
│   └── delta_load/
│       ├── _infra/             # File format, delta manifest, run config, orchestrators
│       ├── procedures/         # One file per DELTA_LOAD_<TABLE> procedure
│       └── tasks/              # task_dag.sql — delta DAG definition + RESUME
│
├── scripts/
│   └── deploy.py               # Change-aware Snowflake deployment engine
│
├── table_onboarding/
│   ├── table_onboarding_tool.py    # Generates onboarding SQL from a JSON config
│   ├── table_config_template.json  # Blank template with inline field docs
│   ├── example_userinfoissalary.json
│   └── README.md               # Onboarding tool documentation
│
├── environments/
│   ├── dev.env                 # Dev credentials template (never commit real values)
│   ├── staging.env             # Staging credentials template
│   └── prod.env                # Prod credentials template
│
├── requirements.txt
├── .gitignore
└── README.md                   ← you are here
```

---

## Quick start

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd taa-snowflake-pipeline
pip install -r requirements.txt
```

### 2. Configure your dev environment

Copy `environments/dev.env` and fill in your Snowflake credentials:

```bash
# Edit environments/dev.env with your values
# (or export the same variables to your shell)
```

Key variables:

| Variable | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `xy12345.us-east-1`) |
| `SNOWFLAKE_USER` | Service account or personal user |
| `SNOWFLAKE_AUTHENTICATOR` | `private_key_jwt`, `externalbrowser`, or `snowflake` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to `.p8` key file (key-pair auth only) |
| `SNOWFLAKE_DATABASE` | Target database |
| `SNOWFLAKE_SCHEMA` | Target schema |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse |
| `SNOWFLAKE_ROLE` | Deployment role |

### 3. Deploy to Snowflake

```bash
# Deploy only files changed vs the previous commit (normal workflow)
python scripts/deploy.py --env dev --changed-only

# Deploy everything (first deploy or full refresh)
python scripts/deploy.py --env dev --full

# Dry run — shows what would execute, no changes made
python scripts/deploy.py --env dev --full --dry-run

# Deploy a specific file
python scripts/deploy.py --env dev --files pipeline/full_load/procedures/full_load_customer.sql
```

---

## Adding a new table

Use the onboarding tool to generate all required SQL automatically:

```bash
# 1. Copy the template and fill it in
cp table_onboarding/table_config_template.json table_onboarding/my_new_table.json
# edit my_new_table.json with your table's columns, UUID, wave, etc.

# 2. Generate the SQL
python table_onboarding/table_onboarding_tool.py table_onboarding/my_new_table.json

# 3. Review the generated file: table_onboarding/onboard_MY_NEW_TABLE.sql
# 4. Split it into the pipeline/ structure and commit
```

See [table_onboarding/README.md](table_onboarding/README.md) for the full guide.

---

## CI/CD flow

### Pull Request → `validate.yml`

Every PR targeting `main` or `staging` runs:

1. **Dry-run deploy** — connects to Snowflake with dev credentials and executes all changed SQL in dry-run mode, catching syntax and permission errors before merge.
2. **Onboarding config check** — if any `table_onboarding/*.json` files changed, re-runs the tool and verifies the generated SQL is committed.
3. **Deployment summary** — posts a PR comment listing every Snowflake object that will be deployed on merge.

### Merge → `deploy.yml`

On merge to `main` (→ **prod**) or `staging` (→ **staging**):

1. Writes the Snowflake private key from the GitHub Secret to a temp file.
2. Builds the environment file from GitHub Environment secrets.
3. Runs `deploy.py --changed-only` to deploy only the files that changed in this merge.
4. Cleans up the temp key file.
5. Posts a deployment summary to the GitHub Actions job summary.

### Manual deploy

Any environment can be deployed manually via **Actions → Deploy to Snowflake → Run workflow**, with a choice of `changed-only` or `full` mode.

---

## GitHub Secrets configuration

Configure the following secrets under **Settings → Environments** for each environment (`dev`, `staging`, `prod`):

| Secret | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PRIVATE_KEY` | Contents of the `.p8` private key file |
| `SNOWFLAKE_DATABASE` | Target database for this environment |
| `SNOWFLAKE_SCHEMA` | Target schema |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse |
| `SNOWFLAKE_ROLE` | Deployment role |

---

## Deployment order

`deploy.py` enforces strict dependency order regardless of the files passed:

```
full_load/_infra/     → full_load/procedures/     → full_load/tasks/
delta_load/_infra/    → delta_load/procedures/    → delta_load/tasks/
```

Within each group, files are sorted alphabetically by name (the numeric prefixes in infra files ensure correct order).

---

## Variable substitution

All SQL files use `&{VAR_NAME}` placeholders for environment-specific values. `deploy.py` substitutes these from the active `.env` file before execution. The `validate.yml` workflow checks that every `&{VAR}` in changed files has a corresponding key in the env file.

Standard placeholders:

| Placeholder | Resolved from |
|---|---|
| `&{SNOWFLAKE_DATABASE}` | `SNOWFLAKE_DATABASE` in `.env` |
| `&{SNOWFLAKE_SCHEMA}` | `SNOWFLAKE_SCHEMA` in `.env` |
| `&{SNOWFLAKE_WAREHOUSE}` | `SNOWFLAKE_WAREHOUSE` in `.env` |
