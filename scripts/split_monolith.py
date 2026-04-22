"""
split_monolith.py
=================
Reads the two monolithic TAA pipeline SQL files and splits them into the
one-file-per-object repo structure expected by deploy.py and the CI/CD pipeline.

Usage:
    python scripts/split_monolith.py \\
        --full-load   path/to/onelake_taa_full_parquet_ingest.sql \\
        --delta-load  path/to/onelake_taa_delta_csv_ingest.sql \\
        --output-dir  pipeline/               # defaults to ./pipeline

Output tree (written under --output-dir):
    full_load/
        _infra/
            01_file_format.sql
            02_stage_manifest_table.sql
            03_table_config.sql
            04_file_audit.sql
            05_run_config.sql
            06_target_tables.sql        # all 12 target table DDLs in one file
        procedures/
            BUILD_STAGE_TAA_FULL_FILE_MANIFEST.sql
            FULL_LOAD_CUSTOMER.sql
            ...
            INGEST_TAA_FULL_LOAD_PREPARE.sql
            INGEST_TAA_FULL_LOAD_FINALIZE.sql
            INGEST_TAA_LAUNCH_FULL_LOAD.sql
            FULL_LOAD_FROM_CONFIG.sql
        tasks/
            task_dag.sql
    delta_load/
        _infra/
            01_file_format.sql
            02_delta_manifest_table.sql
            03_full_load_state_table.sql
            04_run_config.sql
            05_staging_tables.sql       # all STG_DELTA_* tables in one file
        procedures/
            BUILD_STAGE_TAA_DELTA_MANIFEST.sql
            DELTA_LOAD_USERINFOISSALARY.sql
            ...
            DELTA_LOAD_FROM_CONFIG.sql
            INGEST_TAA_DELTA_PREPARE.sql
            INGEST_TAA_DELTA_FINALIZE.sql
            INGEST_TAA_LAUNCH_DELTA_LOAD.sql
        tasks/
            task_dag.sql

Each output file is prefixed with:
    USE DATABASE &{SNOWFLAKE_DATABASE};
    USE SCHEMA   &{SNOWFLAKE_SCHEMA};

so deploy.py can substitute the correct environment values.
"""

import os
import re
import sys
import argparse
from pathlib import Path


# ---------------------------------------------------------------------------
# SQL header injected at the top of every generated file
# ---------------------------------------------------------------------------
FILE_HEADER = """\
USE DATABASE &{{SNOWFLAKE_DATABASE}};
USE SCHEMA   &{{SNOWFLAKE_SCHEMA}};

"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_file(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return f.readlines()


def write_sql(out_path: Path, lines: list[str], strip_leading_blanks: bool = True) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(lines).strip()
    if strip_leading_blanks:
        # Remove leading comment-only blocks that are just section dividers
        content = content.lstrip("-").lstrip()
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(FILE_HEADER)
        f.write(content)
        f.write("\n")
    print(f"  wrote  {out_path}")


def object_name_from_line(line: str) -> str | None:
    """
    Extract the object name from a CREATE OR REPLACE ... line.
    Returns None if the line doesn't match.

    Handles:
        CREATE OR REPLACE PROCEDURE FOO(...)
        CREATE OR REPLACE TABLE FOO (
        CREATE OR REPLACE TASK FOO
        CREATE OR REPLACE FILE FORMAT FOO
    """
    m = re.match(
        r'(?i)create\s+or\s+replace\s+(?:procedure|table|task|file\s+format)\s+([A-Za-z0-9_]+)',
        line.strip()
    )
    return m.group(1).upper() if m else None


def object_type_from_line(line: str) -> str | None:
    m = re.match(
        r'(?i)create\s+or\s+replace\s+(procedure|table|task|file\s+format)',
        line.strip()
    )
    if m:
        return m.group(1).upper().replace(" ", "_")
    return None


# ---------------------------------------------------------------------------
# Core splitter
# ---------------------------------------------------------------------------

def split_file(source_lines: list[str]) -> list[dict]:
    """
    Walk the source lines and split into segments, one per top-level
    CREATE OR REPLACE object. Each segment is a dict:
        {
            'type':  'PROCEDURE' | 'TABLE' | 'TASK' | 'FILE_FORMAT',
            'name':  'FULL_LOAD_CUSTOMER',
            'lines': [list of source lines for this object, including
                      any immediately preceding comment block],
        }
    Also captures:
        - INSERT INTO ... statements immediately after a TABLE (for seed data)
        - MERGE INTO ... statements immediately after a TABLE (for seed data)
        - ALTER TASK ... RESUME statements (grouped into the tasks segment)
        - 'USE DATABASE / USE SCHEMA' lines (discarded -- we re-add them per file)
    """
    segments = []
    i = 0
    n = len(source_lines)

    # Scan for the USE DATABASE / USE SCHEMA header lines and skip them
    header_end = 0
    for idx, line in enumerate(source_lines[:30]):
        if re.match(r'(?i)use\s+(database|schema)', line.strip()):
            header_end = idx + 1

    i = header_end

    # Walk line by line
    current_comment_block: list[str] = []
    current_segment: dict | None = None

    # Accumulator for ALTER TASK RESUME / SUSPEND lines at the end (enable section)
    enable_lines: list[str] = []

    while i < n:
        line = source_lines[i]
        stripped = line.strip()

        # Skip blank lines between objects (but collect them into current segment)
        if not stripped:
            if current_segment:
                current_segment["lines"].append(line)
            else:
                current_comment_block.append(line)
            i += 1
            continue

        # Comment lines -- accumulate into current segment or pre-segment buffer
        if stripped.startswith("--"):
            if current_segment:
                current_segment["lines"].append(line)
            else:
                current_comment_block.append(line)
            i += 1
            continue

        # USE DATABASE / USE SCHEMA -- skip
        if re.match(r'(?i)use\s+(database|schema)', stripped):
            current_comment_block = []
            i += 1
            continue

        # INSERT INTO / MERGE INTO -- seed data after a table DDL
        if re.match(r'(?i)(insert\s+into|merge\s+into)', stripped):
            if current_segment:
                # Append until semicolon on its own line or end of statement
                current_segment["lines"].append(line)
                i += 1
                # Keep reading until we see a line that ends the statement
                while i < n:
                    current_segment["lines"].append(source_lines[i])
                    if re.match(r'(?i)\s*\)\s*;', source_lines[i]) or \
                       source_lines[i].strip() == ';' or \
                       (source_lines[i].strip().endswith(';') and
                        not source_lines[i].strip().startswith('--')):
                        i += 1
                        break
                    i += 1
            else:
                i += 1
            continue

        # ALTER TASK ... RESUME / SUSPEND -- part of the enable section
        if re.match(r'(?i)alter\s+task', stripped):
            enable_lines.append(line)
            i += 1
            continue

        # CREATE OR REPLACE ...
        obj_name = object_name_from_line(line)
        obj_type = object_type_from_line(line)

        if obj_name and obj_type:
            # Save any open segment
            if current_segment:
                segments.append(current_segment)

            current_segment = {
                "type":  obj_type,
                "name":  obj_name,
                "lines": list(current_comment_block) + [line],
            }
            current_comment_block = []

            # For procedures, read until the closing '; -- end of AS '...' block
            if obj_type == "PROCEDURE":
                i += 1
                in_proc_body = False
                while i < n:
                    current_segment["lines"].append(source_lines[i])
                    if re.match(r"(?i)as\s+'", source_lines[i].strip()):
                        in_proc_body = True
                    if in_proc_body and source_lines[i].strip() in ("';", "'\n"):
                        i += 1
                        # Consume a trailing semicolon-only line if present
                        if i < n and source_lines[i].strip() == ";":
                            current_segment["lines"].append(source_lines[i])
                            i += 1
                        break
                    i += 1
                continue

            # For tasks, read until the CALL ... ; line
            if obj_type == "TASK":
                i += 1
                while i < n:
                    current_segment["lines"].append(source_lines[i])
                    if source_lines[i].strip().upper().startswith("CALL ") and \
                       source_lines[i].strip().endswith(";"):
                        i += 1
                        break
                    i += 1
                continue

            # For tables, read until the closing ); line
            if obj_type in ("TABLE", "FILE_FORMAT"):
                i += 1
                while i < n:
                    current_segment["lines"].append(source_lines[i])
                    s = source_lines[i].strip()
                    if s in (");", ");"):
                        i += 1
                        break
                    # Single-line table or file format (ends with ;)
                    if s.endswith(";") and obj_type == "FILE_FORMAT":
                        i += 1
                        break
                    i += 1
                continue

        i += 1

    # Save last open segment
    if current_segment:
        segments.append(current_segment)

    # Attach the enable lines as a synthetic segment
    if enable_lines:
        segments.append({
            "type":  "_ENABLE",
            "name":  "_ENABLE",
            "lines": enable_lines,
        })

    return segments


# ---------------------------------------------------------------------------
# Full load mapping
# ---------------------------------------------------------------------------

# Tables that go into 06_target_tables.sql (load data into Snowflake)
FULL_LOAD_TARGET_TABLES = {
    "CUSTOMER", "ENTERPRISECUSTOMER", "PAYTYPE", "SCHEDULE", "TIMEOFFDATA",
    "TIMEOFFREQUEST", "TIMEOFFREQUESTDETAIL", "TIMESLICEPOST",
    "TIMESLICEPOSTEXCEPTIONDETAIL", "TIMESLICEPOSTSHIFTDIFFDETAIL",
    "USERINFO", "USERINFOISSALARY",
}

FULL_LOAD_INFRA_TABLES = {
    "STAGE_TAA_FULL_FILE_MANIFEST": "02_stage_manifest_table.sql",
    "INGEST_TAA_TABLE_CONFIG":      "03_table_config.sql",
    "INGEST_TAA_FILE_AUDIT":        "04_file_audit.sql",
    "INGEST_TAA_RUN_CONFIG":        "05_run_config.sql",
}

FULL_LOAD_INFRA_FF = {
    "FF_TAA_ONELAKE_PARQUET": "01_file_format.sql",
}


def map_full_load_segment(seg: dict, out_base: Path,
                          target_table_lines: list) -> None:
    t = seg["type"]
    n = seg["name"]

    if t == "FILE_FORMAT":
        dest = out_base / "_infra" / FULL_LOAD_INFRA_FF.get(n, f"ff_{n.lower()}.sql")
        write_sql(dest, seg["lines"])

    elif t == "TABLE":
        if n in FULL_LOAD_INFRA_TABLES:
            dest = out_base / "_infra" / FULL_LOAD_INFRA_TABLES[n]
            write_sql(dest, seg["lines"])
        elif n in FULL_LOAD_TARGET_TABLES:
            target_table_lines.extend(seg["lines"])
            target_table_lines.append("\n")
        # else: unknown table -- skip

    elif t == "PROCEDURE":
        dest = out_base / "procedures" / f"{n}.sql"
        write_sql(dest, seg["lines"])

    elif t == "TASK":
        pass  # tasks are batched separately

    elif t == "_ENABLE":
        pass  # enable lines batched separately


def write_full_load(segments: list[dict], out_base: Path) -> None:
    print("\n[full_load]")
    target_table_lines: list[str] = []
    task_lines: list[str] = []
    enable_lines: list[str] = []

    for seg in segments:
        if seg["type"] == "TASK":
            task_lines.extend(seg["lines"])
            task_lines.append("\n")
        elif seg["type"] == "_ENABLE":
            enable_lines.extend(seg["lines"])
        else:
            map_full_load_segment(seg, out_base, target_table_lines)

    if target_table_lines:
        dest = out_base / "_infra" / "06_target_tables.sql"
        write_sql(dest, target_table_lines)

    if task_lines or enable_lines:
        dest = out_base / "tasks" / "task_dag.sql"
        all_task_lines = task_lines + ["\n-- =================================================\n",
                                       "-- ENABLE THE TASK DAG\n",
                                       "-- =================================================\n\n"] + enable_lines
        write_sql(dest, all_task_lines)


# ---------------------------------------------------------------------------
# Delta load mapping
# ---------------------------------------------------------------------------

DELTA_LOAD_INFRA_TABLES = {
    "STAGE_TAA_DELTA_MANIFEST":  "02_delta_manifest_table.sql",
    "INGEST_TAA_FULL_LOAD_STATE":"03_full_load_state_table.sql",
    "INGEST_TAA_DELTA_RUN_CONFIG":"04_run_config.sql",
}

DELTA_LOAD_INFRA_FF = {
    "FF_TAA_ONELAKE_CSV": "01_file_format.sql",
}

DELTA_STG_PREFIX = "STG_DELTA_"


def map_delta_load_segment(seg: dict, out_base: Path,
                           stg_table_lines: list) -> None:
    t = seg["type"]
    n = seg["name"]

    if t == "FILE_FORMAT":
        dest = out_base / "_infra" / DELTA_LOAD_INFRA_FF.get(n, f"ff_{n.lower()}.sql")
        write_sql(dest, seg["lines"])

    elif t == "TABLE":
        if n in DELTA_LOAD_INFRA_TABLES:
            dest = out_base / "_infra" / DELTA_LOAD_INFRA_TABLES[n]
            write_sql(dest, seg["lines"])
        elif n.startswith(DELTA_STG_PREFIX):
            stg_table_lines.extend(seg["lines"])
            stg_table_lines.append("\n")
        # else: unknown -- skip

    elif t == "PROCEDURE":
        dest = out_base / "procedures" / f"{n}.sql"
        write_sql(dest, seg["lines"])

    elif t == "TASK":
        pass

    elif t == "_ENABLE":
        pass


def write_delta_load(segments: list[dict], out_base: Path) -> None:
    print("\n[delta_load]")
    stg_table_lines: list[str] = []
    task_lines: list[str] = []
    enable_lines: list[str] = []

    for seg in segments:
        if seg["type"] == "TASK":
            task_lines.extend(seg["lines"])
            task_lines.append("\n")
        elif seg["type"] == "_ENABLE":
            enable_lines.extend(seg["lines"])
        else:
            map_delta_load_segment(seg, out_base, stg_table_lines)

    if stg_table_lines:
        dest = out_base / "_infra" / "05_staging_tables.sql"
        write_sql(dest, stg_table_lines)

    if task_lines or enable_lines:
        dest = out_base / "tasks" / "task_dag.sql"
        all_task_lines = task_lines + ["\n-- =================================================\n",
                                       "-- ENABLE THE TASK DAG\n",
                                       "-- =================================================\n\n"] + enable_lines
        write_sql(dest, all_task_lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Split TAA monolithic SQL files into one-file-per-object repo structure."
    )
    parser.add_argument("--full-load",  required=True, help="Path to onelake_taa_full_parquet_ingest.sql")
    parser.add_argument("--delta-load", required=True, help="Path to onelake_taa_delta_csv_ingest.sql")
    parser.add_argument("--output-dir", default="pipeline",
                        help="Root output directory (default: ./pipeline)")
    args = parser.parse_args()

    out = Path(args.output_dir)

    # --- Full load ---
    print(f"Reading: {args.full_load}")
    fl_lines   = read_file(args.full_load)
    fl_segs    = split_file(fl_lines)
    fl_out     = out / "full_load"
    write_full_load(fl_segs, fl_out)

    # --- Delta load ---
    print(f"\nReading: {args.delta_load}")
    dl_lines   = read_file(args.delta_load)
    dl_segs    = split_file(dl_lines)
    dl_out     = out / "delta_load"
    write_delta_load(dl_segs, dl_out)

    print("\nDone. Review the output files before committing.")
    print("Run 'python scripts/deploy.py --env dev --full' to validate a full deploy.")


if __name__ == "__main__":
    main()
