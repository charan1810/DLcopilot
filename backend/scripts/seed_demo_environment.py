from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"
SEED_SQL_PATH = Path(__file__).with_name("seed_dlcopilot_ecommerce.sql")
UNUSED_LOCAL_DB_PATH = REPO_ROOT / "backend" / "app" / "local_app.db"
DEMO_DATABASE = "dlcopilot"


def load_env_value(key: str, env_path: Path) -> str:
    if not env_path.exists():
        raise FileNotFoundError(f"Missing environment file: {env_path}")

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
    if parsed.scheme != "postgresql":
        raise ValueError("APP_DB_URL must be a PostgreSQL SQLAlchemy URL")

    if not parsed.hostname or not parsed.username or not parsed.path:
        raise ValueError("APP_DB_URL is missing required PostgreSQL connection details")

    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": unquote(parsed.username),
        "password": unquote(parsed.password or ""),
        "database": parsed.path.lstrip("/"),
    }


def find_psql() -> str:
    found = shutil.which("psql")
    if found:
        return found

    candidates = [
        Path("C:/Program Files/PostgreSQL/18/bin/psql.exe"),
        Path("C:/Program Files/PostgreSQL/17/bin/psql.exe"),
        Path("C:/Program Files/PostgreSQL/16/bin/psql.exe"),
        Path("C:/Program Files/PostgreSQL/15/bin/psql.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError("psql was not found. Install PostgreSQL client tools or add psql to PATH.")


def run_psql(psql_path: str, conn: dict[str, str | int], database: str, *args: str) -> None:
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
        *args,
    ]
    subprocess.run(command, check=True, env=env, cwd=REPO_ROOT)


def rebuild_demo_database(psql_path: str, conn: dict[str, str | int], demo_database: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", demo_database):
        raise ValueError(f"Unsafe database name: {demo_database}")

    run_psql(psql_path, conn, demo_database, "-f", str(SEED_SQL_PATH))


def reset_saved_connections(psql_path: str, conn: dict[str, str | int], demo_database: str) -> None:
    run_psql(
        psql_path,
        conn,
        demo_database,
        "-c",
        """
        UPDATE app_store.connections
        SET db_type = 'postgres',
            schema_name = 'src'
        WHERE LOWER(COALESCE(database_name, '')) = LOWER('dlcopilot');
        """,
    )

    if UNUSED_LOCAL_DB_PATH.exists():
        UNUSED_LOCAL_DB_PATH.unlink()

    legacy_app_db_path = REPO_ROOT / "backend" / "app" / "copilot_app.db"
    if legacy_app_db_path.exists():
        legacy_app_db_path.unlink()


def main() -> int:
    try:
        app_db_url = load_env_value("APP_DB_URL", ENV_PATH)
        postgres_conn = parse_postgres_url(app_db_url)
        psql_path = find_psql()

        print(f"Rebuilding {DEMO_DATABASE}.src with e-commerce demo data...")
        rebuild_demo_database(psql_path, postgres_conn, DEMO_DATABASE)

        print("Updating saved connection defaults...")
        reset_saved_connections(psql_path, postgres_conn, DEMO_DATABASE)

        print("Demo environment is ready.")
        print("POC flow: PostgreSQL-first (src schema), no Snowflake required.")
        print(f"Business location: {DEMO_DATABASE}.src")
        print("Saved UI connection: DLcopilot Ecommerce Demo")
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"psql command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())