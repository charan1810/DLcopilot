from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"
SQLITE_APP_STORE_PATH = REPO_ROOT / "backend" / "app" / "copilot_app.db"

SOURCE_APP_DATABASE = "datacopilot"
TARGET_DATABASE = "dlcopilot"

PUBLIC_TABLE_COLUMNS = {
    "users": [
        "id",
        "email",
        "full_name",
        "hashed_password",
        "role",
        "is_active",
        "created_at",
    ],
    "connections": [
        "id",
        "owner_user_id",
        "name",
        "db_type",
        "host",
        "port",
        "database_name",
        "schema_name",
        "username",
        "password_encrypted",
        "account",
        "warehouse",
        "role",
        "is_active",
    ],
    "module_access": [
        "id",
        "user_id",
        "module_name",
        "can_view",
        "can_execute",
        "can_admin",
    ],
}

APP_STORE_TABLE_COLUMNS = {
    "connections": [
        "id",
        "name",
        "db_type",
        "host",
        "port",
        "database_name",
        "schema_name",
        "username",
        "password",
        "account",
        "warehouse",
        "role",
        "is_active",
    ],
    "recipes": [
        "id",
        "connection_id",
        "database_name",
        "schema_name",
        "object_name",
        "recipe_name",
        "user_prompt",
        "statement_type",
        "selected_tables_json",
        "sql_text",
        "explanation",
        "created_at",
    ],
    "prompt_history": [
        "id",
        "connection_id",
        "database_name",
        "schema_name",
        "object_name",
        "user_prompt",
        "statement_type",
        "sql_text",
        "created_at",
    ],
    "pipelines": [
        "id",
        "name",
        "description",
        "connection_id",
        "database_name",
        "schema_name",
        "source_object",
        "target_object",
        "created_at",
        "updated_at",
    ],
    "pipeline_steps": [
        "id",
        "pipeline_id",
        "step_order",
        "step_name",
        "step_type",
        "sql_text",
        "is_active",
        "created_at",
        "updated_at",
    ],
    "pipeline_runs": [
        "id",
        "pipeline_id",
        "status",
        "trigger_type",
        "started_at",
        "ended_at",
        "duration_seconds",
        "total_steps",
        "success_steps",
        "failed_steps",
        "error_message",
        "initiated_by",
        "run_log",
    ],
    "pipeline_run_steps": [
        "id",
        "pipeline_run_id",
        "pipeline_id",
        "step_id",
        "step_order",
        "step_name",
        "status",
        "execution_order",
        "started_at",
        "ended_at",
        "duration_seconds",
        "executed_sql",
        "rows_affected",
        "error_message",
        "step_log",
    ],
    "pipeline_schedules": [
        "id",
        "pipeline_id",
        "schedule_type",
        "cron_expression",
        "interval_minutes",
        "is_active",
        "last_run_at",
        "next_run_at",
        "created_at",
        "updated_at",
    ],
}


def load_env_value(key: str, env_path: Path) -> str:
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip()
    raise KeyError(f"{key} is not defined in {env_path}")


def parse_postgres_url(url: str) -> dict[str, str | int]:
    normalized = url.replace("postgresql+psycopg2://", "postgresql://", 1)
    parsed = urlsplit(normalized)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "user": unquote(parsed.username or "postgres"),
        "password": unquote(parsed.password or ""),
    }


def find_psql() -> str:
    found = shutil.which("psql")
    if found:
        return found

    for candidate in [
        Path("C:/Program Files/PostgreSQL/18/bin/psql.exe"),
        Path("C:/Program Files/PostgreSQL/17/bin/psql.exe"),
        Path("C:/Program Files/PostgreSQL/16/bin/psql.exe"),
    ]:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError("psql was not found")


def run_psql(psql_path: str, conn: dict[str, str | int], database: str, sql: str, capture_output: bool = False) -> str:
    env = os.environ.copy()
    env["PGPASSWORD"] = str(conn["password"])
    command = [
        psql_path,
        "-h",
        str(conn["host"]),
        "-p",
        str(conn["port"]),
        "-U",
        str(conn["user"]),
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-At" if capture_output else "-q",
        "-c",
        sql,
    ]
    result = subprocess.run(command, check=True, env=env, cwd=REPO_ROOT, capture_output=capture_output, text=True)
    return result.stdout if capture_output else ""


def run_psql_file_copy(psql_path: str, conn: dict[str, str | int], database: str, copy_sql: str) -> None:
    env = os.environ.copy()
    env["PGPASSWORD"] = str(conn["password"])
    command = [
        psql_path,
        "-h",
        str(conn["host"]),
        "-p",
        str(conn["port"]),
        "-U",
        str(conn["user"]),
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        copy_sql,
    ]
    subprocess.run(command, check=True, env=env, cwd=REPO_ROOT)


def ensure_target_tables(psql_path: str, conn: dict[str, str | int]) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS public.users (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        full_name VARCHAR(255) NOT NULL,
        hashed_password VARCHAR(255) NOT NULL,
        role VARCHAR(50) NOT NULL DEFAULT 'tester',
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS public.connections (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        owner_user_id INTEGER NOT NULL REFERENCES public.users(id),
        name VARCHAR(255) NOT NULL,
        db_type VARCHAR(50) NOT NULL,
        host VARCHAR(255),
        port VARCHAR(50),
        database_name VARCHAR(255),
        schema_name VARCHAR(255),
        username VARCHAR(255),
        password_encrypted TEXT,
        account VARCHAR(255),
        warehouse VARCHAR(255),
        role VARCHAR(255),
        is_active BOOLEAN NOT NULL DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS public.module_access (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES public.users(id),
        module_name VARCHAR(100) NOT NULL,
        can_view BOOLEAN NOT NULL DEFAULT FALSE,
        can_execute BOOLEAN NOT NULL DEFAULT FALSE,
        can_admin BOOLEAN NOT NULL DEFAULT FALSE,
        CONSTRAINT uq_user_module UNIQUE (user_id, module_name)
    );

    CREATE SCHEMA IF NOT EXISTS app_store;

    CREATE TABLE IF NOT EXISTS app_store.connections (
        id INTEGER PRIMARY KEY,
        name TEXT,
        db_type TEXT,
        host TEXT,
        port TEXT,
        database_name TEXT,
        schema_name TEXT,
        username TEXT,
        password TEXT,
        account TEXT,
        warehouse TEXT,
        role TEXT,
        is_active BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS app_store.recipes (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        connection_id INTEGER,
        database_name TEXT,
        schema_name TEXT,
        object_name TEXT,
        recipe_name TEXT,
        user_prompt TEXT,
        statement_type TEXT,
        selected_tables_json TEXT,
        sql_text TEXT,
        explanation TEXT,
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS app_store.prompt_history (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        connection_id INTEGER,
        database_name TEXT,
        schema_name TEXT,
        object_name TEXT,
        user_prompt TEXT,
        statement_type TEXT,
        sql_text TEXT,
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS app_store.pipelines (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        connection_id INTEGER NOT NULL,
        database_name TEXT,
        schema_name TEXT,
        source_object TEXT,
        target_object TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_store.pipeline_steps (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        pipeline_id BIGINT NOT NULL REFERENCES app_store.pipelines(id) ON DELETE CASCADE,
        step_order INTEGER NOT NULL,
        step_name TEXT NOT NULL,
        step_type TEXT NOT NULL DEFAULT 'sql',
        sql_text TEXT NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_store.pipeline_runs (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        pipeline_id BIGINT NOT NULL REFERENCES app_store.pipelines(id) ON DELETE CASCADE,
        status TEXT NOT NULL DEFAULT 'PENDING',
        trigger_type TEXT NOT NULL DEFAULT 'MANUAL',
        started_at TEXT NOT NULL,
        ended_at TEXT,
        duration_seconds DOUBLE PRECISION,
        total_steps INTEGER NOT NULL DEFAULT 0,
        success_steps INTEGER NOT NULL DEFAULT 0,
        failed_steps INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        initiated_by TEXT,
        run_log TEXT
    );

    CREATE TABLE IF NOT EXISTS app_store.pipeline_run_steps (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        pipeline_run_id BIGINT NOT NULL REFERENCES app_store.pipeline_runs(id) ON DELETE CASCADE,
        pipeline_id BIGINT NOT NULL REFERENCES app_store.pipelines(id) ON DELETE CASCADE,
        step_id BIGINT NOT NULL REFERENCES app_store.pipeline_steps(id) ON DELETE CASCADE,
        step_order INTEGER NOT NULL,
        step_name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        execution_order INTEGER DEFAULT 0,
        started_at TEXT,
        ended_at TEXT,
        duration_seconds DOUBLE PRECISION,
        executed_sql TEXT,
        rows_affected INTEGER DEFAULT 0,
        error_message TEXT,
        step_log TEXT
    );

    CREATE TABLE IF NOT EXISTS app_store.pipeline_schedules (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        pipeline_id BIGINT NOT NULL UNIQUE REFERENCES app_store.pipelines(id) ON DELETE CASCADE,
        schedule_type TEXT NOT NULL DEFAULT 'interval',
        cron_expression TEXT,
        interval_minutes INTEGER,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        last_run_at TEXT,
        next_run_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
    run_psql(psql_path, conn, TARGET_DATABASE, ddl)


def export_public_table(psql_path: str, conn: dict[str, str | int], database: str, table_name: str, columns: list[str], csv_path: Path) -> None:
    column_sql = ", ".join(columns)
    sql = f"COPY (SELECT {column_sql} FROM public.{table_name} ORDER BY id) TO STDOUT WITH CSV"
    env = os.environ.copy()
    env["PGPASSWORD"] = str(conn["password"])
    command = [
        psql_path,
        "-h",
        str(conn["host"]),
        "-p",
        str(conn["port"]),
        "-U",
        str(conn["user"]),
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        subprocess.run(command, check=True, env=env, cwd=REPO_ROOT, stdout=handle)


def import_csv_to_table(psql_path: str, conn: dict[str, str | int], database: str, schema_name: str, table_name: str, columns: list[str], csv_path: Path) -> None:
    qualified = f"{schema_name}.{table_name}"
    run_psql(psql_path, conn, database, f"TRUNCATE TABLE {qualified} RESTART IDENTITY CASCADE")
    if csv_path.stat().st_size == 0:
        return
    escaped_path = str(csv_path).replace("\\", "\\\\")
    copy_sql = f"\\copy {qualified} ({', '.join(columns)}) FROM '{escaped_path}' WITH (FORMAT csv)"
    run_psql_file_copy(psql_path, conn, database, copy_sql)

    if "id" in columns and table_name != "connections":
        run_psql(
            psql_path,
            conn,
            database,
            f"SELECT setval(pg_get_serial_sequence('{qualified}', 'id'), COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM {qualified}"
        )


def export_sqlite_table(sqlite_path: Path, table_name: str, columns: list[str], csv_path: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    order_clause = " ORDER BY id" if "id" in columns else ""
    cur.execute(f"SELECT {', '.join(columns)} FROM {table_name}{order_clause}")
    rows = cur.fetchall()
    conn.close()

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def migrate_public_tables(psql_path: str, conn: dict[str, str | int], temp_dir: Path) -> None:
    for table_name, columns in PUBLIC_TABLE_COLUMNS.items():
        csv_path = temp_dir / f"public_{table_name}.csv"
        export_public_table(psql_path, conn, SOURCE_APP_DATABASE, table_name, columns, csv_path)
        import_csv_to_table(psql_path, conn, TARGET_DATABASE, "public", table_name, columns, csv_path)


def migrate_sqlite_app_store(psql_path: str, conn: dict[str, str | int], temp_dir: Path) -> None:
    for table_name, columns in APP_STORE_TABLE_COLUMNS.items():
        csv_path = temp_dir / f"app_store_{table_name}.csv"
        export_sqlite_table(SQLITE_APP_STORE_PATH, table_name, columns, csv_path)
        import_csv_to_table(psql_path, conn, TARGET_DATABASE, "app_store", table_name, columns, csv_path)


def verify_counts(psql_path: str, conn: dict[str, str | int]) -> None:
    summary_sql = """
    SELECT 'public.users', COUNT(*) FROM public.users
    UNION ALL
    SELECT 'public.module_access', COUNT(*) FROM public.module_access
    UNION ALL
    SELECT 'public.connections', COUNT(*) FROM public.connections
    UNION ALL
    SELECT 'app_store.connections', COUNT(*) FROM app_store.connections
    UNION ALL
    SELECT 'app_store.pipelines', COUNT(*) FROM app_store.pipelines
    UNION ALL
    SELECT 'app_store.prompt_history', COUNT(*) FROM app_store.prompt_history
    ORDER BY 1
    """
    output = run_psql(psql_path, conn, TARGET_DATABASE, summary_sql, capture_output=True)
    print(output.strip())


def main() -> int:
    app_db_url = load_env_value("APP_DB_URL", ENV_PATH)
    conn = parse_postgres_url(app_db_url)
    psql_path = find_psql()

    with tempfile.TemporaryDirectory(prefix="dlcopilot-migrate-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        print(f"Ensuring target tables exist in {TARGET_DATABASE}...")
        ensure_target_tables(psql_path, conn)

        print(f"Migrating public app tables from {SOURCE_APP_DATABASE} to {TARGET_DATABASE}...")
        migrate_public_tables(psql_path, conn, temp_dir)

        print("Migrating legacy app_store tables from SQLite to PostgreSQL...")
        migrate_sqlite_app_store(psql_path, conn, temp_dir)

        print("Verification counts:")
        verify_counts(psql_path, conn)

    print("Consolidation complete. You can now point APP_DB_URL to dlcopilot and restart the backend.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())