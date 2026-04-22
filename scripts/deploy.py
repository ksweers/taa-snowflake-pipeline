"""
deploy.py
=========
Change-aware Snowflake deployment script for the TAA ingestion pipeline.

Modes:
    --full          Deploy every SQL file in dependency order.
                    Used for first-time environment setup.

    --changed-only  Deploy only files that changed in the last git commit
                    (or between two specified refs). Used by CI/CD on merge.

    --files a.sql b.sql ...
                    Deploy specific files explicitly. Useful for targeted
                    hotfixes without triggering a full deploy.

Usage:
    # First-time environment setup:
    python deploy.py --env dev --full

    # CI/CD deploy (changed files only, driven by GitHub Actions):
    python deploy.py --env prod --changed-only

    # Deploy specific files:
    python deploy.py --env staging --files pipeline/full_load/procedures/FULL_LOAD_CUSTOMER.sql

    # Dry run (print what would be executed, don't connect to Snowflake):
    python deploy.py --env dev --full --dry-run

Environment files:
    environments/dev.env, staging.env, prod.env
    Each contains KEY=VALUE pairs (no quotes needed). Required keys:
        SNOWFLAKE_ACCOUNT
        SNOWFLAKE_USER
        SNOWFLAKE_AUTHENTICATOR   (snowflake | externalbrowser | private_key_jwt)
        SNOWFLAKE_PRIVATE_KEY_PATH  (if using key-pair auth)
        SNOWFLAKE_PASSWORD          (if using password auth -- not recommended for prod)
        SNOWFLAKE_DATABASE
        SNOWFLAKE_SCHEMA
        SNOWFLAKE_WAREHOUSE
        SNOWFLAKE_ROLE              (optional)

    All keys from the env file are also used as substitution variables in SQL
    files, so &{SNOWFLAKE_DATABASE} in a .sql file becomes the actual value.

Deployment order:
    Files are executed in this fixed dependency order regardless of which
    mode is used. Only files that actually need deploying are executed.

        1. pipeline/full_load/_infra/01_*.sql  ... 06_*.sql
        2. pipeline/full_load/procedures/*.sql
        3. pipeline/full_load/tasks/task_dag.sql
        4. pipeline/delta_load/_infra/01_*.sql ... 05_*.sql
        5. pipeline/delta_load/procedures/*.sql
        6. pipeline/delta_load/tasks/task_dag.sql

    Within each group, files are sorted by filename (numeric prefix drives infra order).
"""

import os
import re
import sys
import glob
import argparse
import subprocess
from pathlib import Path


# ---------------------------------------------------------------------------
# Deployment order: all pipeline SQL files in dependency sequence
# ---------------------------------------------------------------------------

def ordered_pipeline_files(repo_root: Path) -> list[Path]:
    """
    Return all pipeline SQL files in the correct deployment order.
    Groups:
        full_load/_infra  (sorted by filename)
        full_load/procedures (sorted by filename -- alphabetical is fine,
                              all procs are independent)
        full_load/tasks
        delta_load/_infra
        delta_load/procedures
        delta_load/tasks
    """
    groups = [
        repo_root / "pipeline" / "full_load"  / "_infra",
        repo_root / "pipeline" / "full_load"  / "procedures",
        repo_root / "pipeline" / "full_load"  / "tasks",
        repo_root / "pipeline" / "delta_load" / "_infra",
        repo_root / "pipeline" / "delta_load" / "procedures",
        repo_root / "pipeline" / "delta_load" / "tasks",
    ]
    result = []
    for group_dir in groups:
        if group_dir.exists():
            files = sorted(group_dir.glob("*.sql"))
            result.extend(files)
    return result


# ---------------------------------------------------------------------------
# Environment loading and SQL variable substitution
# ---------------------------------------------------------------------------

def load_env(env_file: Path) -> dict[str, str]:
    """Load KEY=VALUE pairs from an env file. Strips quotes and comments."""
    env: dict[str, str] = {}
    if not env_file.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file}")
    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip()
            value = value.strip().strip('"').strip("'")
            env[key] = value
    return env


def substitute_vars(sql: str, env: dict[str, str]) -> str:
    """Replace &{KEY} placeholders with values from the env dict."""
    def replacer(m):
        key = m.group(1)
        if key not in env:
            raise KeyError(f"SQL references undefined variable: &{{{key}}}. "
                           f"Add it to your .env file.")
        return env[key]
    return re.sub(r'&\{(\w+)\}', replacer, sql)


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def changed_files_in_commit(repo_root: Path,
                             base_ref: str = "HEAD~1",
                             head_ref: str = "HEAD") -> set[Path]:
    """
    Return the set of .sql files that changed between base_ref and head_ref.
    Paths are returned as absolute Paths.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", base_ref, head_ref],
            capture_output=True, text=True, cwd=str(repo_root), check=True
        )
        changed = set()
        for line in result.stdout.splitlines():
            p = (repo_root / line.strip()).resolve()
            if p.suffix.lower() == ".sql":
                changed.add(p)
        return changed
    except subprocess.CalledProcessError as e:
        print(f"ERROR: git diff failed: {e.stderr}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Snowflake execution
# ---------------------------------------------------------------------------

def connect_snowflake(env: dict[str, str]):
    """Create and return a Snowflake connection."""
    try:
        import snowflake.connector
    except ImportError:
        print("ERROR: snowflake-connector-python is not installed.\n"
              "       Run: pip install snowflake-connector-python",
              file=sys.stderr)
        sys.exit(1)

    conn_kwargs: dict = {
        "account":   env["SNOWFLAKE_ACCOUNT"],
        "user":      env["SNOWFLAKE_USER"],
        "database":  env["SNOWFLAKE_DATABASE"],
        "schema":    env["SNOWFLAKE_SCHEMA"],
        "warehouse": env["SNOWFLAKE_WAREHOUSE"],
    }

    if env.get("SNOWFLAKE_ROLE"):
        conn_kwargs["role"] = env["SNOWFLAKE_ROLE"]

    auth = env.get("SNOWFLAKE_AUTHENTICATOR", "snowflake").lower()

    if auth == "private_key_jwt":
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        key_path = env.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
        passphrase = env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", None)
        if not key_path:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH must be set for private_key_jwt auth.")
        with open(key_path, "rb") as key_file:
            private_key = load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None
            )
        from cryptography.hazmat.primitives.serialization import (
            Encoding, PrivateFormat, NoEncryption
        )
        pkb = private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption()
        )
        conn_kwargs["private_key"]     = pkb
        conn_kwargs["authenticator"]   = "snowflake"

    elif auth == "externalbrowser":
        conn_kwargs["authenticator"] = "externalbrowser"

    else:
        # Password auth -- acceptable for dev, not recommended for prod
        conn_kwargs["password"] = env.get("SNOWFLAKE_PASSWORD", "")

    return snowflake.connector.connect(**conn_kwargs)


def execute_sql_file(conn, sql_file: Path, env: dict[str, str],
                     dry_run: bool = False) -> bool:
    """
    Read, substitute, and execute one SQL file.
    Splits on ';' boundaries (respects procedure body quoting).
    Returns True on success, False on failure.
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    try:
        sql = substitute_vars(raw_sql, env)
    except KeyError as e:
        print(f"  ERROR  variable substitution failed: {e}")
        return False

    statements = split_sql_statements(sql)

    if dry_run:
        print(f"  DRY-RUN  {sql_file.name}  ({len(statements)} statement(s))")
        return True

    cursor = conn.cursor()
    try:
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue
            cursor.execute(stmt)
        print(f"  OK       {sql_file.name}  ({len(statements)} statement(s))")
        return True
    except Exception as e:
        print(f"  FAILED   {sql_file.name}\n           {e}")
        return False
    finally:
        cursor.close()


def split_sql_statements(sql: str) -> list[str]:
    """
    Split a SQL string into individual statements on ';' boundaries,
    correctly handling Snowflake JS procedure bodies delimited by AS '...';
    (single-quoted blocks where internal quotes are doubled).
    """
    statements = []
    current: list[str] = []
    in_single_quote = False
    i = 0
    chars = sql

    while i < len(chars):
        ch = chars[i]

        if ch == "'" and not in_single_quote:
            in_single_quote = True
            current.append(ch)
            i += 1
        elif ch == "'" and in_single_quote:
            # Doubled single-quote inside string: ''
            if i + 1 < len(chars) and chars[i + 1] == "'":
                current.append("''")
                i += 2
            else:
                in_single_quote = False
                current.append(ch)
                i += 1
        elif ch == ";" and not in_single_quote:
            current.append(ch)
            stmt = "".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []
            i += 1
        else:
            current.append(ch)
            i += 1

    # Last statement without trailing semicolon
    remaining = "".join(current).strip()
    if remaining and not remaining.startswith("--"):
        statements.append(remaining)

    return statements


# ---------------------------------------------------------------------------
# Deployment runner
# ---------------------------------------------------------------------------

def run_deploy(files_to_deploy: list[Path], env: dict[str, str],
               dry_run: bool = False) -> bool:
    """Connect and execute all files in order. Returns True if all succeeded."""
    if not files_to_deploy:
        print("Nothing to deploy.")
        return True

    print(f"\nDeploying {len(files_to_deploy)} file(s) to "
          f"{env['SNOWFLAKE_DATABASE']}.{env['SNOWFLAKE_SCHEMA']} "
          f"[{'DRY RUN' if dry_run else 'LIVE'}]\n")

    if dry_run:
        for f in files_to_deploy:
            print(f"  DRY-RUN  {f}")
        return True

    conn = connect_snowflake(env)
    success = True
    try:
        for sql_file in files_to_deploy:
            ok = execute_sql_file(conn, sql_file, env, dry_run=False)
            if not ok:
                success = False
                # Continue deploying remaining files rather than aborting --
                # a single proc failure shouldn't block independent objects.
    finally:
        conn.close()

    print()
    if success:
        print("Deployment complete -- all files succeeded.")
    else:
        print("Deployment finished with errors. Check output above.")

    return success


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    repo_root = Path(__file__).resolve().parent.parent  # one level up from scripts/

    parser = argparse.ArgumentParser(description="TAA Snowflake pipeline deployer.")
    parser.add_argument("--env", required=True,
                        help="Environment name (dev | staging | prod) or path to a .env file.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full", action="store_true",
                       help="Deploy all pipeline files in dependency order.")
    group.add_argument("--changed-only", action="store_true",
                       help="Deploy only files changed in the last git commit.")
    group.add_argument("--files", nargs="+", metavar="FILE",
                       help="Deploy specific SQL files.")
    parser.add_argument("--base-ref", default="HEAD~1",
                        help="Base git ref for --changed-only diff (default: HEAD~1).")
    parser.add_argument("--head-ref", default="HEAD",
                        help="Head git ref for --changed-only diff (default: HEAD).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be deployed without connecting to Snowflake.")
    args = parser.parse_args()

    # Load environment
    env_path = Path(args.env) if os.path.exists(args.env) else \
               repo_root / "environments" / f"{args.env}.env"
    env = load_env(env_path)
    print(f"Environment: {env_path}")
    print(f"Target:      {env.get('SNOWFLAKE_DATABASE')}.{env.get('SNOWFLAKE_SCHEMA')}")

    # Determine which files to deploy
    all_ordered = ordered_pipeline_files(repo_root)

    if args.full:
        files_to_deploy = all_ordered

    elif args.changed_only:
        changed = changed_files_in_commit(repo_root, args.base_ref, args.head_ref)
        # Keep only files in the ordered list AND that were changed,
        # preserving the correct deployment order.
        files_to_deploy = [f for f in all_ordered if f.resolve() in changed]
        print(f"\nChanged SQL files ({len(files_to_deploy)}):")
        for f in files_to_deploy:
            print(f"  {f.relative_to(repo_root)}")

    elif args.files:
        explicit = {Path(f).resolve() for f in args.files}
        # Maintain dependency order for explicitly requested files too
        files_to_deploy = [f for f in all_ordered if f.resolve() in explicit]
        # Add any requested files that aren't in the ordered list (e.g. ad-hoc scripts)
        ordered_set = {f.resolve() for f in all_ordered}
        extra = [Path(f).resolve() for f in args.files if Path(f).resolve() not in ordered_set]
        files_to_deploy.extend(extra)

    success = run_deploy(files_to_deploy, env, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
