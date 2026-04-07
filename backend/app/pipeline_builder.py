from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
import sqlite3
import logging
import re
from pathlib import Path

from app.core.security import get_current_user, require_role

try:
    import psycopg2
except ImportError:
    psycopg2 = None

logger = logging.getLogger("pipeline_builder")

_dev_or_admin = require_role("admin", "developer")

router = APIRouter(prefix="/api", tags=["pipeline-builder"])

# Must point to the SAME SQLite app DB used by main.py
APP_DB_PATH = Path(__file__).resolve().parent / "copilot_app.db"


def utc_now_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def get_sqlite_conn():
    conn = sqlite3.connect(APP_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row) -> Dict[str, Any]:
    return dict(row) if row else {}


def ensure_pipeline_tables():
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            connection_id INTEGER NOT NULL,
            database_name TEXT,
            schema_name TEXT,
            source_object TEXT,
            target_object TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER NOT NULL,
            step_order INTEGER NOT NULL,
            step_name TEXT NOT NULL,
            step_type TEXT NOT NULL DEFAULT 'sql',
            sql_text TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            trigger_type TEXT NOT NULL DEFAULT 'MANUAL',
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_seconds REAL,
            total_steps INTEGER NOT NULL DEFAULT 0,
            success_steps INTEGER NOT NULL DEFAULT 0,
            failed_steps INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            initiated_by TEXT,
            run_log TEXT,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_run_id INTEGER NOT NULL,
            pipeline_id INTEGER NOT NULL,
            step_id INTEGER NOT NULL,
            step_order INTEGER NOT NULL,
            step_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            execution_order INTEGER DEFAULT 0,
            started_at TEXT,
            ended_at TEXT,
            duration_seconds REAL,
            executed_sql TEXT,
            rows_affected INTEGER DEFAULT 0,
            error_message TEXT,
            step_log TEXT,
            FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE,
            FOREIGN KEY (step_id) REFERENCES pipeline_steps(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER NOT NULL UNIQUE,
            schedule_type TEXT NOT NULL DEFAULT 'interval',
            cron_expression TEXT,
            interval_minutes INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            last_run_at TEXT,
            next_run_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
        )
    """)

    _migrate_pipeline_tables(conn)
    conn.commit()
    conn.close()


def _migrate_pipeline_tables(conn):
    """Add new columns to existing tables if they don't exist yet."""
    cur = conn.cursor()
    migrations = [
        ("pipeline_runs", "trigger_type", "TEXT NOT NULL DEFAULT 'MANUAL'"),
        ("pipeline_runs", "duration_seconds", "REAL"),
        ("pipeline_runs", "error_message", "TEXT"),
        ("pipeline_runs", "initiated_by", "TEXT"),
        ("pipeline_run_steps", "execution_order", "INTEGER DEFAULT 0"),
        ("pipeline_run_steps", "duration_seconds", "REAL"),
        ("pipeline_run_steps", "executed_sql", "TEXT"),
    ]
    for table, column, col_type in migrations:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        except sqlite3.OperationalError:
            pass  # column already exists


def get_connection_row(connection_id: int):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id, db_type, host, port, database_name, schema_name, username, password
            FROM connections
            WHERE id = ?
        """, (connection_id,))
        row = cur.fetchone()
    except sqlite3.OperationalError as e:
        conn.close()
        raise HTTPException(
            status_code=500,
            detail=f"Connections table issue: {str(e)}. Ensure main.py creates the connections table in copilot_app.db."
        )

    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Connection not found. Save the connection again from the UI so it is written into SQLite."
        )

    return row


def get_postgres_conn(connection_id: int):
    if psycopg2 is None:
        raise HTTPException(status_code=500, detail="psycopg2 is not installed")

    row = get_connection_row(connection_id)
    db_type = (row["db_type"] or "").lower()

    if db_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline execution currently supports PostgreSQL only. Found: {db_type}"
        )

    try:
        conn = psycopg2.connect(
            host=row["host"],
            port=int(row["port"]) if row["port"] else 5432,
            dbname=row["database_name"] or "postgres",
            user=row["username"],
            password=row["password"] or "",
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to PostgreSQL: {str(e)}")


def fetch_pipeline_with_steps(pipeline_id: int):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,))
    pipeline = cur.fetchone()
    if not pipeline:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("""
        SELECT *
        FROM pipeline_steps
        WHERE pipeline_id = ?
        ORDER BY step_order ASC, id ASC
    """, (pipeline_id,))
    steps = cur.fetchall()

    conn.close()

    data = row_to_dict(pipeline)
    data["steps"] = [row_to_dict(s) for s in steps]
    return data


def insert_run_step(pipeline_run_id: int, pipeline_id: int, step: Dict[str, Any], execution_order: int = 0) -> int:
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipeline_run_steps (
            pipeline_run_id, pipeline_id, step_id, step_order, step_name,
            status, execution_order, started_at, ended_at, duration_seconds,
            executed_sql, rows_affected, error_message, step_log
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_run_id,
        pipeline_id,
        step["id"],
        step["step_order"],
        step["step_name"],
        "PENDING",
        execution_order,
        None,
        None,
        None,
        step.get("sql_text", ""),
        0,
        None,
        None
    ))

    run_step_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_step_id


def update_run_step(run_step_id: int, **kwargs):
    allowed = {
        "status",
        "started_at",
        "ended_at",
        "duration_seconds",
        "executed_sql",
        "rows_affected",
        "error_message",
        "step_log",
    }

    fields = []
    values = []

    for key, value in kwargs.items():
        if key in allowed:
            fields.append(f"{key} = ?")
            values.append(value)

    if not fields:
        return

    values.append(run_step_id)

    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE pipeline_run_steps
        SET {", ".join(fields)}
        WHERE id = ?
    """, values)
    conn.commit()
    conn.close()


def update_run_status(
    run_id: int,
    status: str,
    ended_at: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    success_steps: Optional[int] = None,
    failed_steps: Optional[int] = None,
    error_message: Optional[str] = None,
    run_log: Optional[str] = None
):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    fields = ["status = ?"]
    values = [status]

    if ended_at is not None:
        fields.append("ended_at = ?")
        values.append(ended_at)

    if duration_seconds is not None:
        fields.append("duration_seconds = ?")
        values.append(duration_seconds)

    if success_steps is not None:
        fields.append("success_steps = ?")
        values.append(success_steps)

    if failed_steps is not None:
        fields.append("failed_steps = ?")
        values.append(failed_steps)

    if error_message is not None:
        fields.append("error_message = ?")
        values.append(error_message)

    if run_log is not None:
        fields.append("run_log = ?")
        values.append(run_log)

    values.append(run_id)

    cur.execute(f"""
        UPDATE pipeline_runs
        SET {", ".join(fields)}
        WHERE id = ?
    """, values)
    conn.commit()
    conn.close()


def compute_duration(start_iso: str, end_iso: str) -> float:
    """Compute duration in seconds between two ISO timestamps."""
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        return round((e - s).total_seconds(), 2)
    except Exception:
        return 0.0


def safe_error_text(e: Exception) -> str:
    try:
        return str(e)[:2000]
    except Exception:
        return "Unknown error"


DDL_KEYWORDS = {"create", "alter", "drop", "truncate"}
_SQL_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_COMMENT_LINE_RE = re.compile(r"--[^\n\r]*")


def _strip_sql_comments(sql_text: str) -> str:
    if not sql_text:
        return ""
    without_block = _SQL_COMMENT_BLOCK_RE.sub(" ", sql_text)
    return _SQL_COMMENT_LINE_RE.sub(" ", without_block)


def _leading_sql_verb(sql_text: str) -> str:
    cleaned = _strip_sql_comments(sql_text).lstrip()
    if not cleaned:
        return ""
    first_token = cleaned.split(None, 1)[0]
    return first_token.lower().strip()


def _extract_ddl_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ddl_steps: List[Dict[str, Any]] = []
    for step in steps:
        sql_text = step.get("sql_text", "") or ""
        verb = _leading_sql_verb(sql_text)
        if verb not in DDL_KEYWORDS:
            continue

        first_non_empty_line = ""
        for line in sql_text.splitlines():
            stripped = line.strip()
            if stripped:
                first_non_empty_line = stripped
                break

        ddl_steps.append({
            "step_id": step.get("id"),
            "step_order": step.get("step_order"),
            "step_name": step.get("step_name", ""),
            "statement_type": verb.upper(),
            "snippet": first_non_empty_line[:180],
        })

    return ddl_steps


class PipelineCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    connection_id: int
    database_name: Optional[str] = ""
    schema_name: Optional[str] = ""
    source_object: Optional[str] = ""
    target_object: Optional[str] = ""


class PipelineUpdate(BaseModel):
    name: str
    description: Optional[str] = ""
    connection_id: int
    database_name: Optional[str] = ""
    schema_name: Optional[str] = ""
    source_object: Optional[str] = ""
    target_object: Optional[str] = ""


class PipelineStepCreate(BaseModel):
    step_name: str
    step_type: str = "sql"
    sql_text: str
    is_active: int = 1


class PipelineStepUpdate(BaseModel):
    step_name: Optional[str] = None
    sql_text: Optional[str] = None
    is_active: Optional[int] = None


class PipelineImportStepsRequest(BaseModel):
    steps: List[PipelineStepCreate]


class ExecutePipelineRequest(BaseModel):
    stop_on_error: bool = True
    from_step_order: Optional[int] = None
    trigger_type: str = "MANUAL"
    initiated_by: Optional[str] = None
    allow_ddl_execute: bool = False


class RetryPipelineRequest(BaseModel):
    from_step_order: Optional[int] = None
    initiated_by: Optional[str] = None
    allow_ddl_execute: bool = False


class PipelineScheduleCreate(BaseModel):
    schedule_type: str = "interval"
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: int = 1


class PipelineScheduleUpdate(BaseModel):
    schedule_type: Optional[str] = None
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: Optional[int] = None


ensure_pipeline_tables()


@router.get("/pipelines")
def list_pipelines(
    connection_id: Optional[int] = None,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    _u=Depends(get_current_user),
):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    query = "SELECT * FROM pipelines WHERE 1=1"
    params = []

    if connection_id is not None:
        query += " AND connection_id = ?"
        params.append(connection_id)

    if database_name:
        query += " AND database_name = ?"
        params.append(database_name)

    if schema_name:
        query += " AND schema_name = ?"
        params.append(schema_name)

    query += " ORDER BY updated_at DESC, id DESC"

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [row_to_dict(r) for r in rows]


@router.post("/pipelines")
def create_pipeline(payload: PipelineCreate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipelines (
            name, description, connection_id, database_name, schema_name,
            source_object, target_object, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload.name,
        payload.description,
        payload.connection_id,
        payload.database_name,
        payload.schema_name,
        payload.source_object,
        payload.target_object,
        now,
        now
    ))

    pipeline_id = cur.lastrowid
    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.get("/pipelines/runs")
def list_all_pipeline_runs(limit: int = 50, _u=Depends(get_current_user)):
    """List recent runs across all pipelines."""
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT pr.*, p.name as pipeline_name
        FROM pipeline_runs pr
        LEFT JOIN pipelines p ON p.id = pr.pipeline_id
        ORDER BY pr.id DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return [row_to_dict(r) for r in rows]


@router.get("/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: int, _u=Depends(get_current_user)):
    return fetch_pipeline_with_steps(pipeline_id)


@router.put("/pipelines/{pipeline_id}")
def update_pipeline(pipeline_id: int, payload: PipelineUpdate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("""
        UPDATE pipelines
        SET name = ?, description = ?, connection_id = ?, database_name = ?,
            schema_name = ?, source_object = ?, target_object = ?, updated_at = ?
        WHERE id = ?
    """, (
        payload.name,
        payload.description,
        payload.connection_id,
        payload.database_name,
        payload.schema_name,
        payload.source_object,
        payload.target_object,
        now,
        pipeline_id
    ))

    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: int, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("DELETE FROM pipeline_run_steps WHERE pipeline_id = ?", (pipeline_id,))
    cur.execute("DELETE FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,))
    cur.execute("DELETE FROM pipeline_steps WHERE pipeline_id = ?", (pipeline_id,))
    cur.execute("DELETE FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    cur.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))

    conn.commit()
    conn.close()

    _remove_schedule_from_scheduler(pipeline_id)

    return {"status": "success", "message": "Pipeline deleted successfully"}


@router.post("/pipelines/{pipeline_id}/steps")
def add_pipeline_step(pipeline_id: int, payload: PipelineStepCreate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    pipeline = cur.fetchone()
    if not pipeline:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("""
        SELECT COALESCE(MAX(step_order), 0) AS max_order
        FROM pipeline_steps
        WHERE pipeline_id = ?
    """, (pipeline_id,))
    max_order_row = cur.fetchone()
    next_order = (max_order_row["max_order"] or 0) + 1

    cur.execute("""
        INSERT INTO pipeline_steps (
            pipeline_id, step_order, step_name, step_type, sql_text, is_active,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_id,
        next_order,
        payload.step_name,
        payload.step_type,
        payload.sql_text,
        payload.is_active,
        now,
        now
    ))

    cur.execute("UPDATE pipelines SET updated_at = ? WHERE id = ?", (now, pipeline_id))
    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.post("/pipelines/{pipeline_id}/steps/import")
def import_pipeline_steps(pipeline_id: int, payload: PipelineImportStepsRequest, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    pipeline = cur.fetchone()
    if not pipeline:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("DELETE FROM pipeline_steps WHERE pipeline_id = ?", (pipeline_id,))

    step_order = 1
    for step in payload.steps:
        cur.execute("""
            INSERT INTO pipeline_steps (
                pipeline_id, step_order, step_name, step_type, sql_text, is_active,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_id,
            step_order,
            step.step_name,
            step.step_type,
            step.sql_text,
            step.is_active,
            now,
            now
        ))
        step_order += 1

    cur.execute("UPDATE pipelines SET updated_at = ? WHERE id = ?", (now, pipeline_id))
    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.delete("/pipelines/{pipeline_id}/steps/{step_id}")
def delete_pipeline_step(pipeline_id: int, step_id: int, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM pipeline_steps
        WHERE id = ? AND pipeline_id = ?
    """, (step_id, pipeline_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    deleted_order = row["step_order"]

    cur.execute("DELETE FROM pipeline_steps WHERE id = ? AND pipeline_id = ?", (step_id, pipeline_id))
    cur.execute("""
        UPDATE pipeline_steps
        SET step_order = step_order - 1, updated_at = ?
        WHERE pipeline_id = ? AND step_order > ?
    """, (now, pipeline_id, deleted_order))
    cur.execute("UPDATE pipelines SET updated_at = ? WHERE id = ?", (now, pipeline_id))

    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.put("/pipelines/{pipeline_id}/steps/{step_id}")
def update_pipeline_step(pipeline_id: int, step_id: int, payload: PipelineStepUpdate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM pipeline_steps
        WHERE id = ? AND pipeline_id = ?
    """, (step_id, pipeline_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline step not found")

    fields = []
    values = []

    if payload.step_name is not None:
        fields.append("step_name = ?")
        values.append(payload.step_name)
    if payload.sql_text is not None:
        fields.append("sql_text = ?")
        values.append(payload.sql_text)
    if payload.is_active is not None:
        fields.append("is_active = ?")
        values.append(payload.is_active)

    if not fields:
        conn.close()
        return fetch_pipeline_with_steps(pipeline_id)

    fields.append("updated_at = ?")
    values.append(now)
    values.append(step_id)
    values.append(pipeline_id)

    cur.execute(f"""
        UPDATE pipeline_steps
        SET {", ".join(fields)}
        WHERE id = ? AND pipeline_id = ?
    """, values)
    cur.execute("UPDATE pipelines SET updated_at = ? WHERE id = ?", (now, pipeline_id))

    conn.commit()
    conn.close()

    return fetch_pipeline_with_steps(pipeline_id)


@router.post("/pipelines/{pipeline_id}/execute")
def execute_pipeline(pipeline_id: int, payload: ExecutePipelineRequest, _u=Depends(_dev_or_admin)):
    return _run_pipeline(
        pipeline_id,
        stop_on_error=payload.stop_on_error,
        from_step_order=payload.from_step_order,
        trigger_type=payload.trigger_type or "MANUAL",
        initiated_by=payload.initiated_by,
        allow_ddl_execute=payload.allow_ddl_execute,
    )


@router.post("/pipelines/{pipeline_id}/run")
def run_pipeline(pipeline_id: int, payload: ExecutePipelineRequest, _u=Depends(_dev_or_admin)):
    """Alias for execute_pipeline matching the new API spec."""
    return _run_pipeline(
        pipeline_id,
        stop_on_error=payload.stop_on_error,
        from_step_order=payload.from_step_order,
        trigger_type=payload.trigger_type or "MANUAL",
        initiated_by=payload.initiated_by,
        allow_ddl_execute=payload.allow_ddl_execute,
    )


@router.post("/pipeline-runs/{run_id}/retry")
def retry_pipeline_run(run_id: int, payload: Optional[RetryPipelineRequest] = None, _u=Depends(_dev_or_admin)):
    """Retry a failed pipeline run, optionally from a specific step."""
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,))
    original_run = cur.fetchone()
    conn.close()

    if not original_run:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    original = row_to_dict(original_run)
    if original["status"] not in ("FAILED", "PARTIAL_SUCCESS"):
        raise HTTPException(status_code=400, detail="Only failed or partially successful runs can be retried")

    from_step = None
    if payload and payload.from_step_order is not None:
        from_step = payload.from_step_order
    else:
        # Find the first failed step order from the original run
        conn = get_sqlite_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT step_order FROM pipeline_run_steps
            WHERE pipeline_run_id = ? AND status = 'FAILED'
            ORDER BY step_order ASC LIMIT 1
        """, (run_id,))
        failed_step = cur.fetchone()
        conn.close()
        if failed_step:
            from_step = failed_step["step_order"]

    return _run_pipeline(
        original["pipeline_id"],
        stop_on_error=True,
        from_step_order=from_step,
        trigger_type="RETRY",
        initiated_by=payload.initiated_by if payload else None,
        allow_ddl_execute=payload.allow_ddl_execute if payload else False,
    )


def _run_pipeline(
    pipeline_id: int,
    stop_on_error: bool = True,
    from_step_order: Optional[int] = None,
    trigger_type: str = "MANUAL",
    initiated_by: Optional[str] = None,
    allow_ddl_execute: bool = False,
):
    """Core execution engine for running a pipeline."""
    pipeline = fetch_pipeline_with_steps(pipeline_id)
    all_active_steps = [s for s in pipeline.get("steps", []) if int(s.get("is_active", 1)) == 1]

    if not all_active_steps:
        raise HTTPException(status_code=400, detail="No active steps found in pipeline")

    # Filter steps starting from a specific step order if requested
    if from_step_order is not None:
        steps = [s for s in all_active_steps if s["step_order"] >= from_step_order]
        if not steps:
            raise HTTPException(
                status_code=400,
                detail=f"No active steps found from step_order {from_step_order}"
            )
    else:
        steps = all_active_steps

    if not allow_ddl_execute:
        ddl_steps = _extract_ddl_steps(steps)
        if ddl_steps:
            step_labels = ", ".join(
                f"{s['step_order']} ({s['statement_type']})"
                for s in ddl_steps
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    "DDL execution is blocked by default for safety. "
                    f"Detected DDL in step(s): {step_labels}. "
                    "Re-run with explicit approval (allow_ddl_execute=true)."
                ),
            )

    started_at = utc_now_str()
    logger.info(f"Pipeline {pipeline_id} execution started: trigger={trigger_type}, steps={len(steps)}")

    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_runs (
            pipeline_id, status, trigger_type, started_at, ended_at,
            duration_seconds, total_steps, success_steps, failed_steps,
            error_message, initiated_by, run_log
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_id,
        "RUNNING",
        trigger_type,
        started_at,
        None,
        None,
        len(steps),
        0,
        0,
        None,
        initiated_by,
        None,
    ))
    run_id = cur.lastrowid
    conn.commit()
    conn.close()

    success_steps = 0
    failed_steps = 0
    run_logs = []
    pg_conn = None

    try:
        pg_conn = get_postgres_conn(pipeline["connection_id"])

        for exec_idx, step in enumerate(steps, start=1):
            run_step_id = insert_run_step(run_id, pipeline_id, step, execution_order=exec_idx)
            step_started = utc_now_str()
            update_run_step(run_step_id, status="RUNNING", started_at=step_started)
            logger.info(f"  Step {step['step_order']} ({step['step_name']}): RUNNING")

            try:
                cursor = pg_conn.cursor()
                cursor.execute(step["sql_text"])
                rows_affected = cursor.rowcount if cursor.rowcount is not None else 0
                pg_conn.commit()
                cursor.close()

                step_ended = utc_now_str()
                step_duration = compute_duration(step_started, step_ended)

                update_run_step(
                    run_step_id,
                    status="SUCCESS",
                    ended_at=step_ended,
                    duration_seconds=step_duration,
                    rows_affected=rows_affected,
                    step_log=f"Executed successfully. Rows affected: {rows_affected}"
                )

                success_steps += 1
                run_logs.append(f"Step {step['step_order']} - {step['step_name']}: SUCCESS ({step_duration}s, {rows_affected} rows)")
                logger.info(f"  Step {step['step_order']} ({step['step_name']}): SUCCESS - {rows_affected} rows in {step_duration}s")

            except Exception as step_error:
                if pg_conn:
                    pg_conn.rollback()

                error_text = safe_error_text(step_error)
                step_ended = utc_now_str()
                step_duration = compute_duration(step_started, step_ended)

                update_run_step(
                    run_step_id,
                    status="FAILED",
                    ended_at=step_ended,
                    duration_seconds=step_duration,
                    rows_affected=0,
                    error_message=error_text,
                    step_log=f"Execution failed: {error_text}"
                )

                failed_steps += 1
                run_logs.append(f"Step {step['step_order']} - {step['step_name']}: FAILED - {error_text}")
                logger.error(f"  Step {step['step_order']} ({step['step_name']}): FAILED - {error_text}")

                if stop_on_error:
                    ended_at = utc_now_str()
                    update_run_status(
                        run_id,
                        status="FAILED",
                        ended_at=ended_at,
                        duration_seconds=compute_duration(started_at, ended_at),
                        success_steps=success_steps,
                        failed_steps=failed_steps,
                        error_message=error_text,
                        run_log="\n".join(run_logs)
                    )
                    # Update schedule last_run_at
                    _update_schedule_last_run(pipeline_id)
                    return get_pipeline_run(run_id)

        final_status = "SUCCESS" if failed_steps == 0 else "PARTIAL_SUCCESS"
        ended_at = utc_now_str()

        update_run_status(
            run_id,
            status=final_status,
            ended_at=ended_at,
            duration_seconds=compute_duration(started_at, ended_at),
            success_steps=success_steps,
            failed_steps=failed_steps,
            run_log="\n".join(run_logs)
        )

        logger.info(f"Pipeline {pipeline_id} execution finished: {final_status}")
        _update_schedule_last_run(pipeline_id)
        return get_pipeline_run(run_id)

    except HTTPException:
        ended_at = utc_now_str()
        update_run_status(
            run_id,
            status="FAILED",
            ended_at=ended_at,
            duration_seconds=compute_duration(started_at, ended_at),
            success_steps=success_steps,
            failed_steps=failed_steps + 1,
            error_message="Pipeline failed before completion",
            run_log="\n".join(run_logs + ["Pipeline failed before completion"])
        )
        raise

    except Exception as e:
        error_text = safe_error_text(e)
        ended_at = utc_now_str()
        update_run_status(
            run_id,
            status="FAILED",
            ended_at=ended_at,
            duration_seconds=compute_duration(started_at, ended_at),
            success_steps=success_steps,
            failed_steps=failed_steps + 1,
            error_message=error_text,
            run_log="\n".join(run_logs + [f"Pipeline execution error: {error_text}"])
        )
        raise HTTPException(status_code=500, detail=error_text)

    finally:
        if pg_conn:
            try:
                pg_conn.close()
            except Exception:
                pass


def _update_schedule_last_run(pipeline_id: int):
    """Update the schedule's last_run_at after a pipeline execution."""
    now = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE pipeline_schedules SET last_run_at = ?, updated_at = ? WHERE pipeline_id = ?",
        (now, now, pipeline_id)
    )
    conn.commit()
    conn.close()


@router.get("/pipelines/{pipeline_id}/runs")
def list_pipeline_runs(pipeline_id: int, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    pipeline = cur.fetchone()
    if not pipeline:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("""
        SELECT *
        FROM pipeline_runs
        WHERE pipeline_id = ?
        ORDER BY id DESC
    """, (pipeline_id,))
    rows = cur.fetchall()
    conn.close()

    return [row_to_dict(r) for r in rows]


@router.get("/pipeline-runs/{run_id}")
def get_pipeline_run(run_id: int, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT pr.*, p.name as pipeline_name
        FROM pipeline_runs pr
        LEFT JOIN pipelines p ON p.id = pr.pipeline_id
        WHERE pr.id = ?
    """, (run_id,))
    run = cur.fetchone()
    if not run:
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    cur.execute("""
        SELECT *
        FROM pipeline_run_steps
        WHERE pipeline_run_id = ?
        ORDER BY execution_order ASC, step_order ASC, id ASC
    """, (run_id,))
    step_rows = cur.fetchall()
    conn.close()

    data = row_to_dict(run)
    data["steps"] = [row_to_dict(s) for s in step_rows]
    return data


# =============================================================================
# Schedule CRUD
# =============================================================================

@router.post("/pipelines/{pipeline_id}/schedule")
def create_pipeline_schedule(pipeline_id: int, payload: PipelineScheduleCreate, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("SELECT id FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Schedule already exists for this pipeline. Use PUT to update.")

    if payload.schedule_type == "cron" and not payload.cron_expression:
        raise HTTPException(status_code=400, detail="cron_expression is required for cron schedule type")
    if payload.schedule_type == "interval" and not payload.interval_minutes:
        raise HTTPException(status_code=400, detail="interval_minutes is required for interval schedule type")

    now = utc_now_str()
    cur.execute("""
        INSERT INTO pipeline_schedules (
            pipeline_id, schedule_type, cron_expression, interval_minutes,
            is_active, last_run_at, next_run_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pipeline_id,
        payload.schedule_type,
        payload.cron_expression,
        payload.interval_minutes,
        payload.is_active,
        None,
        None,
        now,
        now,
    ))
    schedule_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Register with scheduler
    _sync_schedule_to_scheduler(pipeline_id)

    return _get_schedule_response(pipeline_id)


@router.get("/pipelines/{pipeline_id}/schedule")
def get_pipeline_schedule(pipeline_id: int, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipelines WHERE id = ?", (pipeline_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Pipeline not found")

    cur.execute("SELECT * FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"schedule": None, "pipeline_id": pipeline_id}

    return _get_schedule_response(pipeline_id)


@router.put("/pipelines/{pipeline_id}/schedule")
def update_pipeline_schedule(pipeline_id: int, payload: PipelineScheduleUpdate, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="No schedule found for this pipeline")

    fields = []
    values = []

    if payload.schedule_type is not None:
        fields.append("schedule_type = ?")
        values.append(payload.schedule_type)
    if payload.cron_expression is not None:
        fields.append("cron_expression = ?")
        values.append(payload.cron_expression)
    if payload.interval_minutes is not None:
        fields.append("interval_minutes = ?")
        values.append(payload.interval_minutes)
    if payload.is_active is not None:
        fields.append("is_active = ?")
        values.append(payload.is_active)

    if not fields:
        conn.close()
        return _get_schedule_response(pipeline_id)

    now = utc_now_str()
    fields.append("updated_at = ?")
    values.append(now)
    values.append(pipeline_id)

    cur.execute(f"""
        UPDATE pipeline_schedules
        SET {", ".join(fields)}
        WHERE pipeline_id = ?
    """, values)
    conn.commit()
    conn.close()

    _sync_schedule_to_scheduler(pipeline_id)
    return _get_schedule_response(pipeline_id)


@router.delete("/pipelines/{pipeline_id}/schedule")
def delete_pipeline_schedule(pipeline_id: int, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="No schedule found for this pipeline")

    cur.execute("DELETE FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    conn.commit()
    conn.close()

    _remove_schedule_from_scheduler(pipeline_id)

    return {"status": "success", "message": "Schedule deleted"}


def _get_schedule_response(pipeline_id: int) -> dict:
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ps.*, p.name as pipeline_name
        FROM pipeline_schedules ps
        LEFT JOIN pipelines p ON p.id = ps.pipeline_id
        WHERE ps.pipeline_id = ?
    """, (pipeline_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"schedule": None, "pipeline_id": pipeline_id}
    data = row_to_dict(row)
    # Add next_run_at from scheduler if available
    next_run = _get_next_run_time(pipeline_id)
    if next_run:
        data["next_run_at"] = next_run
    return data


# =============================================================================
# Scheduler integration stubs — filled by main.py at startup
# =============================================================================

_scheduler_ref = None


def set_scheduler(scheduler):
    """Called by main.py to provide the APScheduler reference."""
    global _scheduler_ref
    _scheduler_ref = scheduler
    _load_all_schedules()


def _load_all_schedules():
    """Register all active schedules from the DB at startup."""
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT pipeline_id FROM pipeline_schedules WHERE is_active = 1")
    rows = cur.fetchall()
    conn.close()
    for row in rows:
        _sync_schedule_to_scheduler(row["pipeline_id"])


def _sync_schedule_to_scheduler(pipeline_id: int):
    """Add or update the scheduler job for a pipeline."""
    if _scheduler_ref is None:
        return

    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pipeline_schedules WHERE pipeline_id = ?", (pipeline_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        _remove_schedule_from_scheduler(pipeline_id)
        return

    schedule = row_to_dict(row)
    job_id = f"pipeline_schedule_{pipeline_id}"

    # Remove existing job first
    try:
        _scheduler_ref.remove_job(job_id)
    except Exception:
        pass

    if not schedule.get("is_active"):
        return

    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger

    try:
        if schedule["schedule_type"] == "cron" and schedule.get("cron_expression"):
            parts = schedule["cron_expression"].strip().split()
            if len(parts) == 5:
                trigger = CronTrigger(
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4],
                )
            else:
                trigger = CronTrigger.from_crontab(schedule["cron_expression"])

            _scheduler_ref.add_job(
                _scheduled_pipeline_run,
                trigger=trigger,
                args=[pipeline_id],
                id=job_id,
                replace_existing=True,
                name=f"Pipeline {pipeline_id} (cron)",
            )
        elif schedule["schedule_type"] == "interval" and schedule.get("interval_minutes"):
            trigger = IntervalTrigger(minutes=schedule["interval_minutes"])
            _scheduler_ref.add_job(
                _scheduled_pipeline_run,
                trigger=trigger,
                args=[pipeline_id],
                id=job_id,
                replace_existing=True,
                name=f"Pipeline {pipeline_id} (interval {schedule['interval_minutes']}m)",
            )

        # Update next_run_at
        job = _scheduler_ref.get_job(job_id)
        if job and job.next_run_time:
            next_run_str = job.next_run_time.isoformat(timespec="seconds")
            now = utc_now_str()
            db_conn = get_sqlite_conn()
            db_cur = db_conn.cursor()
            db_cur.execute(
                "UPDATE pipeline_schedules SET next_run_at = ?, updated_at = ? WHERE pipeline_id = ?",
                (next_run_str, now, pipeline_id)
            )
            db_conn.commit()
            db_conn.close()

    except Exception as e:
        logger.error(f"Failed to sync schedule for pipeline {pipeline_id}: {e}")


def _remove_schedule_from_scheduler(pipeline_id: int):
    if _scheduler_ref is None:
        return
    job_id = f"pipeline_schedule_{pipeline_id}"
    try:
        _scheduler_ref.remove_job(job_id)
    except Exception:
        pass


def _get_next_run_time(pipeline_id: int) -> Optional[str]:
    if _scheduler_ref is None:
        return None
    job_id = f"pipeline_schedule_{pipeline_id}"
    try:
        job = _scheduler_ref.get_job(job_id)
        if job and job.next_run_time:
            return job.next_run_time.isoformat(timespec="seconds")
    except Exception:
        pass
    return None


def _scheduled_pipeline_run(pipeline_id: int):
    """Executed by cron/interval scheduler."""
    logger.info(f"Scheduled run triggered for pipeline {pipeline_id}")
    try:
        _run_pipeline(
            pipeline_id,
            stop_on_error=True,
            trigger_type="SCHEDULED",
            initiated_by="scheduler",
        )
    except Exception as e:
        logger.error(f"Scheduled run failed for pipeline {pipeline_id}: {e}")