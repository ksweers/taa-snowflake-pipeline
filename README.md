# taa-snowflake-pipeline

CI/CD repository for the **TAA → Snowflake** ingestion pipeline. This repo manages all Snowflake object definitions (tables, procedures, task DAGs) for both the **full Parquet load** and **delta CSV load** pipelines, along with the tooling to split, deploy, and onboard new tables.

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
│   ├── split_monolith.py       # Splits monolithic SQL files → individual object files
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

### 3. Split the monolithic SQL files (first-time setup)

If `pipeline/` is empty, generate the individual object files from the source monoliths:

```bash
python scripts/split_monolith.py \
  --full-load  /path/to/onelake_taa_full_parquet_ingest.sql \
  --delta-load /path/to/onelake_taa_delta_csv_ingest.sql \
  --output-dir pipeline/
```

### 4. Deploy to Snowflake

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

---

## Known issues / pending work

- **`TABLE_NAME_FILTER` in `FULL_LOAD_FROM_CONFIG`** — the filter currently only controls which tables are cleared in PREPARE, not which DAG tasks fire. A fix has been designed but not yet applied.
- Wave 2 tasks have a hard-coded parent task name — updating a table's wave requires manual task DDL edits.
