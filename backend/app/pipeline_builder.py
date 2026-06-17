from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime, timezone
import json
import logging
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.core.app_store import AppStoreOperationalError, get_app_store_conn, init_app_store
from app.core.security import get_current_user, require_role

try:
    import psycopg2
except ImportError:
    psycopg2 = None

logger = logging.getLogger("pipeline_builder")

_dev_or_admin = require_role("admin", "architect", "developer")

router = APIRouter(prefix="/api", tags=["pipeline-builder"])

def utc_now_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def get_sqlite_conn():
    return get_app_store_conn()


def row_to_dict(row) -> Dict[str, Any]:
    return dict(row) if row else {}


def _serialize_mapping_config(mapping_config: Any) -> str:
    if not mapping_config:
        return "{}"
    return json.dumps(mapping_config)


def _parse_mapping_config(raw_value: Any) -> Dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def hydrate_pipeline_record(row) -> Dict[str, Any]:
    data = row_to_dict(row)
    if not data:
        return {}
    data["mapping_config"] = _parse_mapping_config(data.get("mapping_config"))
    return data


def ensure_pipeline_tables():
    init_app_store()


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
    except AppStoreOperationalError as e:
        conn.close()
        raise HTTPException(
            status_code=500,
            detail=f"Connections table issue: {str(e)}. Ensure the PostgreSQL app_store schema is initialized."
        )

    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Connection not found. Save the connection again from the UI so it is written into PostgreSQL."
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

    data = hydrate_pipeline_record(pipeline)
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
    mapping_config: Optional[Dict[str, Any]] = None


class PipelineUpdate(BaseModel):
    name: str
    description: Optional[str] = ""
    connection_id: int
    database_name: Optional[str] = ""
    schema_name: Optional[str] = ""
    source_object: Optional[str] = ""
    target_object: Optional[str] = ""
    mapping_config: Optional[Dict[str, Any]] = None


class PipelineStepCreate(BaseModel):
    step_name: str
    step_type: str = "sql"
    sql_text: str
    is_active: bool = True


class PipelineStepUpdate(BaseModel):
    step_name: Optional[str] = None
    sql_text: Optional[str] = None
    is_active: Optional[bool] = None


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


class ValidatePipelineStepsRequest(BaseModel):
    steps: List[PipelineStepCreate] = []
    stop_on_error: bool = False
    timeout_ms: int = 5000


class PipelineScheduleCreate(BaseModel):
    schedule_type: str = "interval"
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: bool = True


class PipelineScheduleUpdate(BaseModel):
    schedule_type: Optional[str] = None
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: Optional[bool] = None


class WorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    connection_id: int
    database_name: Optional[str] = ""
    execution_mode: str = "serial"
    stop_on_error: bool = True
    is_active: bool = True


class WorkflowUpdate(BaseModel):
    name: str
    description: Optional[str] = ""
    connection_id: int
    database_name: Optional[str] = ""
    execution_mode: str = "serial"
    stop_on_error: bool = True
    is_active: bool = True


class WorkflowNodeInput(BaseModel):
    id: Optional[int] = None
    pipeline_id: int
    node_name: Optional[str] = None
    pos_x: float = 120
    pos_y: float = 120


class WorkflowEdgeInput(BaseModel):
    from_node_id: int
    to_node_id: int


class WorkflowGraphUpdate(BaseModel):
    nodes: List[WorkflowNodeInput] = []
    edges: List[WorkflowEdgeInput] = []


class RunWorkflowRequest(BaseModel):
    trigger_type: str = "MANUAL"
    initiated_by: Optional[str] = None
    allow_ddl_execute: bool = False
    execution_mode: Optional[str] = None
    stop_on_error: Optional[bool] = None
    max_parallel_nodes: int = 4


class WorkflowScheduleCreate(BaseModel):
    schedule_type: str = "interval"
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: bool = True


class WorkflowScheduleUpdate(BaseModel):
    schedule_type: Optional[str] = None
    cron_expression: Optional[str] = None
    interval_minutes: Optional[int] = None
    is_active: Optional[bool] = None


ensure_pipeline_tables()


def _is_active_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return bool(value)


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

    return [hydrate_pipeline_record(r) for r in rows]


@router.post("/pipelines")
def create_pipeline(payload: PipelineCreate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()

    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipelines (
            name, description, connection_id, database_name, schema_name,
            source_object, target_object, mapping_config, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload.name,
        payload.description,
        payload.connection_id,
        payload.database_name,
        payload.schema_name,
        payload.source_object,
        payload.target_object,
        _serialize_mapping_config(payload.mapping_config),
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
            schema_name = ?, source_object = ?, target_object = ?, mapping_config = ?, updated_at = ?
        WHERE id = ?
    """, (
        payload.name,
        payload.description,
        payload.connection_id,
        payload.database_name,
        payload.schema_name,
        payload.source_object,
        payload.target_object,
        _serialize_mapping_config(payload.mapping_config),
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
    all_active_steps = [
        s for s in pipeline.get("steps", [])
        if _is_active_flag(s.get("is_active", True))
    ]

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


@router.post("/pipelines/{pipeline_id}/validate-steps")
def validate_pipeline_steps(pipeline_id: int, payload: ValidatePipelineStepsRequest, _u=Depends(_dev_or_admin)):
    pipeline = fetch_pipeline_with_steps(pipeline_id)

    if payload.steps:
        steps_to_validate: List[Dict[str, Any]] = []
        for idx, step in enumerate(payload.steps, start=1):
            steps_to_validate.append({
                "id": idx,
                "step_order": idx,
                "step_name": step.step_name,
                "sql_text": step.sql_text,
                "is_active": step.is_active,
            })
    else:
        steps_to_validate = [
            s for s in pipeline.get("steps", [])
            if _is_active_flag(s.get("is_active", True))
        ]

    if not steps_to_validate:
        raise HTTPException(status_code=400, detail="No steps available to validate")

    timeout_ms = max(1000, min(int(payload.timeout_ms or 5000), 30000))
    stop_on_error = bool(payload.stop_on_error)

    pg_conn = None
    validation_results: List[Dict[str, Any]] = []

    try:
        pg_conn = get_postgres_conn(pipeline["connection_id"])

        for step in steps_to_validate:
            step_order = int(step.get("step_order") or 0)
            step_name = step.get("step_name") or f"Step {step_order}"
            sql_text = (step.get("sql_text") or "").strip()

            if not sql_text:
                validation_results.append({
                    "step_order": step_order,
                    "step_name": step_name,
                    "valid": False,
                    "error": "SQL is empty.",
                })
                if stop_on_error:
                    break
                continue

            try:
                cursor = pg_conn.cursor()
                cursor.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
                cursor.execute(sql_text)
                cursor.close()

                # Validation mode must not persist any changes.
                pg_conn.rollback()

                validation_results.append({
                    "step_order": step_order,
                    "step_name": step_name,
                    "valid": True,
                    "error": None,
                })
            except Exception as step_error:
                pg_conn.rollback()
                validation_results.append({
                    "step_order": step_order,
                    "step_name": step_name,
                    "valid": False,
                    "error": safe_error_text(step_error),
                })
                if stop_on_error:
                    break

        all_valid = all(item.get("valid") for item in validation_results) if validation_results else False
        return {
            "pipeline_id": pipeline_id,
            "valid": all_valid,
            "total_steps": len(steps_to_validate),
            "validated_steps": len(validation_results),
            "steps": validation_results,
            "validation_mode": "dry_run_transaction_rollback",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {safe_error_text(e)}")
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
    cur.execute("SELECT pipeline_id FROM pipeline_schedules WHERE is_active = TRUE")
    rows = cur.fetchall()
    cur.execute("SELECT workflow_id FROM workflow_schedules WHERE is_active = TRUE")
    workflow_rows = cur.fetchall()
    conn.close()
    for row in rows:
        _sync_schedule_to_scheduler(row["pipeline_id"])
    for row in workflow_rows:
        _sync_workflow_schedule_to_scheduler(row["workflow_id"])


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


def _normalize_execution_mode(value: str) -> str:
    normalized = (value or "serial").strip().lower()
    if normalized not in {"serial", "parallel"}:
        raise HTTPException(status_code=400, detail="execution_mode must be 'serial' or 'parallel'")
    return normalized


def _validate_workflow_graph(node_ids: List[int], edges: List[Dict[str, Any]]) -> None:
    node_set = set(node_ids)
    indegree = {node_id: 0 for node_id in node_set}
    adjacency: Dict[int, set] = {node_id: set() for node_id in node_set}

    for edge in edges:
        from_node_id = int(edge.get("from_node_id") or 0)
        to_node_id = int(edge.get("to_node_id") or 0)

        if from_node_id not in node_set or to_node_id not in node_set:
            raise HTTPException(status_code=400, detail="Workflow edge references an unknown node")
        if from_node_id == to_node_id:
            raise HTTPException(status_code=400, detail="Workflow edge cannot point to the same node")
        if to_node_id in adjacency[from_node_id]:
            continue

        adjacency[from_node_id].add(to_node_id)
        indegree[to_node_id] += 1

    queue = deque(node_id for node_id, deg in indegree.items() if deg == 0)
    visited = 0

    while queue:
        current = queue.popleft()
        visited += 1
        for downstream in adjacency[current]:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                queue.append(downstream)

    if visited != len(node_set):
        raise HTTPException(status_code=400, detail="Workflow graph contains a cycle")


def _fetch_workflow_graph(workflow_id: int) -> Dict[str, Any]:
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM workflows WHERE id = ?", (workflow_id,))
    workflow_row = cur.fetchone()
    if not workflow_row:
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute(
        """
        SELECT wn.*, p.name AS pipeline_name
        FROM workflow_nodes wn
        JOIN pipelines p ON p.id = wn.pipeline_id
        WHERE wn.workflow_id = ?
        ORDER BY wn.id ASC
        """,
        (workflow_id,),
    )
    node_rows = cur.fetchall()

    cur.execute(
        """
        SELECT *
        FROM workflow_edges
        WHERE workflow_id = ?
        ORDER BY id ASC
        """,
        (workflow_id,),
    )
    edge_rows = cur.fetchall()
    conn.close()

    workflow = row_to_dict(workflow_row)
    workflow["nodes"] = [row_to_dict(row) for row in node_rows]
    workflow["edges"] = [row_to_dict(row) for row in edge_rows]
    return workflow


def _upsert_workflow_run_node(
    workflow_run_id: int,
    workflow_id: int,
    node_id: int,
    pipeline_id: int,
    execution_group: int = 0,
):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO workflow_run_nodes (
            workflow_run_id, workflow_id, node_id, pipeline_id,
            status, execution_group, started_at, ended_at, duration_seconds,
            error_message, node_log
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            workflow_run_id,
            workflow_id,
            node_id,
            pipeline_id,
            "PENDING",
            execution_group,
            None,
            None,
            None,
            None,
            None,
        ),
    )
    run_node_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_node_id


def _update_workflow_run_node(run_node_id: int, **kwargs):
    allowed = {
        "pipeline_run_id",
        "status",
        "execution_group",
        "started_at",
        "ended_at",
        "duration_seconds",
        "error_message",
        "node_log",
    }
    fields = []
    values = []

    for key, value in kwargs.items():
        if key in allowed:
            fields.append(f"{key} = ?")
            values.append(value)

    if not fields:
        return

    values.append(run_node_id)
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE workflow_run_nodes
        SET {", ".join(fields)}
        WHERE id = ?
        """,
        values,
    )
    conn.commit()
    conn.close()


def _update_workflow_run(
    workflow_run_id: int,
    status: str,
    ended_at: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    success_nodes: Optional[int] = None,
    failed_nodes: Optional[int] = None,
    skipped_nodes: Optional[int] = None,
    error_message: Optional[str] = None,
    run_log: Optional[str] = None,
):
    fields = ["status = ?"]
    values: List[Any] = [status]

    if ended_at is not None:
        fields.append("ended_at = ?")
        values.append(ended_at)
    if duration_seconds is not None:
        fields.append("duration_seconds = ?")
        values.append(duration_seconds)
    if success_nodes is not None:
        fields.append("success_nodes = ?")
        values.append(success_nodes)
    if failed_nodes is not None:
        fields.append("failed_nodes = ?")
        values.append(failed_nodes)
    if skipped_nodes is not None:
        fields.append("skipped_nodes = ?")
        values.append(skipped_nodes)
    if error_message is not None:
        fields.append("error_message = ?")
        values.append(error_message)
    if run_log is not None:
        fields.append("run_log = ?")
        values.append(run_log)

    values.append(workflow_run_id)
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE workflow_runs
        SET {", ".join(fields)}
        WHERE id = ?
        """,
        values,
    )
    conn.commit()
    conn.close()


def _get_workflow_run_response(workflow_run_id: int):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT wr.*, w.name AS workflow_name
        FROM workflow_runs wr
        LEFT JOIN workflows w ON w.id = wr.workflow_id
        WHERE wr.id = ?
        """,
        (workflow_run_id,),
    )
    run_row = cur.fetchone()
    if not run_row:
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow run not found")

    cur.execute(
        """
        SELECT wrn.*, wn.node_name, p.name AS pipeline_name
        FROM workflow_run_nodes wrn
        LEFT JOIN workflow_nodes wn ON wn.id = wrn.node_id
        LEFT JOIN pipelines p ON p.id = wrn.pipeline_id
        WHERE wrn.workflow_run_id = ?
        ORDER BY wrn.execution_group ASC, wrn.id ASC
        """,
        (workflow_run_id,),
    )
    node_rows = cur.fetchall()
    conn.close()

    data = row_to_dict(run_row)
    data["nodes"] = [row_to_dict(row) for row in node_rows]
    return data


@router.get("/workflows")
def list_workflows(connection_id: Optional[int] = None, database_name: Optional[str] = None, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()

    query = "SELECT * FROM workflows WHERE 1=1"
    params: List[Any] = []

    if connection_id is not None:
        query += " AND connection_id = ?"
        params.append(connection_id)
    if database_name:
        query += " AND database_name = ?"
        params.append(database_name)

    query += " ORDER BY updated_at DESC, id DESC"
    cur.execute(query, params)
    workflow_rows = cur.fetchall()

    workflows = []
    for row in workflow_rows:
        workflow = row_to_dict(row)
        cur.execute("SELECT COUNT(*) AS total_nodes FROM workflow_nodes WHERE workflow_id = ?", (workflow["id"],))
        node_count_row = cur.fetchone()
        cur.execute("SELECT COUNT(*) AS total_edges FROM workflow_edges WHERE workflow_id = ?", (workflow["id"],))
        edge_count_row = cur.fetchone()
        workflow["node_count"] = int((node_count_row or {}).get("total_nodes") or 0)
        workflow["edge_count"] = int((edge_count_row or {}).get("total_edges") or 0)
        workflows.append(workflow)

    conn.close()
    return workflows


@router.post("/workflows")
def create_workflow(payload: WorkflowCreate, _u=Depends(_dev_or_admin)):
    execution_mode = _normalize_execution_mode(payload.execution_mode)
    now = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO workflows (
            name, description, connection_id, database_name,
            execution_mode, stop_on_error, is_active, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.name,
            payload.description,
            payload.connection_id,
            payload.database_name,
            execution_mode,
            payload.stop_on_error,
            payload.is_active,
            now,
            now,
        ),
    )
    workflow_id = cur.lastrowid
    conn.commit()
    conn.close()

    return _fetch_workflow_graph(workflow_id)


@router.get("/workflows/{workflow_id}")
def get_workflow(workflow_id: int, _u=Depends(get_current_user)):
    return _fetch_workflow_graph(workflow_id)


@router.put("/workflows/{workflow_id}")
def update_workflow(workflow_id: int, payload: WorkflowUpdate, _u=Depends(_dev_or_admin)):
    execution_mode = _normalize_execution_mode(payload.execution_mode)
    now = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute(
        """
        UPDATE workflows
        SET name = ?, description = ?, connection_id = ?, database_name = ?,
            execution_mode = ?, stop_on_error = ?, is_active = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            payload.name,
            payload.description,
            payload.connection_id,
            payload.database_name,
            execution_mode,
            payload.stop_on_error,
            payload.is_active,
            now,
            workflow_id,
        ),
    )
    conn.commit()
    conn.close()
    return _fetch_workflow_graph(workflow_id)


@router.delete("/workflows/{workflow_id}")
def delete_workflow(workflow_id: int, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute("DELETE FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    cur.execute("DELETE FROM workflow_run_nodes WHERE workflow_id = ?", (workflow_id,))
    cur.execute("DELETE FROM workflow_runs WHERE workflow_id = ?", (workflow_id,))
    cur.execute("DELETE FROM workflow_edges WHERE workflow_id = ?", (workflow_id,))
    cur.execute("DELETE FROM workflow_nodes WHERE workflow_id = ?", (workflow_id,))
    cur.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))
    conn.commit()
    conn.close()

    _remove_workflow_schedule_from_scheduler(workflow_id)
    return {"status": "success", "message": "Workflow deleted successfully"}


@router.put("/workflows/{workflow_id}/graph")
def update_workflow_graph(workflow_id: int, payload: WorkflowGraphUpdate, _u=Depends(_dev_or_admin)):
    now = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute("SELECT id, pipeline_id FROM workflow_nodes WHERE workflow_id = ?", (workflow_id,))
    existing_nodes = {int(row["id"]): int(row["pipeline_id"]) for row in cur.fetchall()}

    retained_node_ids = set()

    for node in payload.nodes:
        if node.id and int(node.id) in existing_nodes:
            node_id = int(node.id)
            cur.execute(
                """
                UPDATE workflow_nodes
                SET pipeline_id = ?, node_name = ?, pos_x = ?, pos_y = ?, updated_at = ?
                WHERE id = ? AND workflow_id = ?
                """,
                (
                    node.pipeline_id,
                    node.node_name,
                    node.pos_x,
                    node.pos_y,
                    now,
                    node_id,
                    workflow_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO workflow_nodes (workflow_id, pipeline_id, node_name, pos_x, pos_y, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workflow_id,
                    node.pipeline_id,
                    node.node_name,
                    node.pos_x,
                    node.pos_y,
                    now,
                    now,
                ),
            )
            node_id = int(cur.lastrowid)

        retained_node_ids.add(node_id)

    if retained_node_ids:
        placeholders = ",".join(["?"] * len(retained_node_ids))
        cur.execute(
            f"DELETE FROM workflow_nodes WHERE workflow_id = ? AND id NOT IN ({placeholders})",
            [workflow_id, *sorted(retained_node_ids)],
        )
    else:
        cur.execute("DELETE FROM workflow_nodes WHERE workflow_id = ?", (workflow_id,))

    cur.execute("DELETE FROM workflow_edges WHERE workflow_id = ?", (workflow_id,))

    edges_to_insert: List[Dict[str, Any]] = []
    for edge in payload.edges:
        if edge.from_node_id not in retained_node_ids or edge.to_node_id not in retained_node_ids:
            conn.close()
            raise HTTPException(status_code=400, detail="Edge references a missing node")
        edges_to_insert.append({"from_node_id": edge.from_node_id, "to_node_id": edge.to_node_id})

    _validate_workflow_graph(sorted(retained_node_ids), edges_to_insert)

    unique_pairs = set()
    for edge in edges_to_insert:
        pair = (int(edge["from_node_id"]), int(edge["to_node_id"]))
        if pair in unique_pairs:
            continue
        unique_pairs.add(pair)
        cur.execute(
            """
            INSERT INTO workflow_edges (workflow_id, from_node_id, to_node_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (workflow_id, pair[0], pair[1], now),
        )

    cur.execute("UPDATE workflows SET updated_at = ? WHERE id = ?", (now, workflow_id))
    conn.commit()
    conn.close()
    return _fetch_workflow_graph(workflow_id)


def _execute_workflow_node(
    workflow_run_id: int,
    run_node_id: int,
    node: Dict[str, Any],
    trigger_type: str,
    initiated_by: Optional[str],
    allow_ddl_execute: bool,
    execution_group: int,
):
    step_started = utc_now_str()
    _update_workflow_run_node(
        run_node_id,
        status="RUNNING",
        started_at=step_started,
        execution_group=execution_group,
        node_log=f"Running pipeline {node['pipeline_id']} from workflow run {workflow_run_id}",
    )

    try:
        run_response = _run_pipeline(
            int(node["pipeline_id"]),
            stop_on_error=True,
            trigger_type=trigger_type,
            initiated_by=initiated_by,
            allow_ddl_execute=allow_ddl_execute,
        )
        step_ended = utc_now_str()
        duration = compute_duration(step_started, step_ended)
        _update_workflow_run_node(
            run_node_id,
            status="SUCCESS",
            pipeline_run_id=run_response.get("id"),
            ended_at=step_ended,
            duration_seconds=duration,
            node_log=f"Pipeline run {run_response.get('id')} completed successfully",
        )
        return {
            "node_id": int(node["id"]),
            "status": "SUCCESS",
            "pipeline_run_id": run_response.get("id"),
            "error": None,
            "duration": duration,
        }
    except Exception as exc:
        err_text = safe_error_text(exc)
        step_ended = utc_now_str()
        duration = compute_duration(step_started, step_ended)
        _update_workflow_run_node(
            run_node_id,
            status="FAILED",
            ended_at=step_ended,
            duration_seconds=duration,
            error_message=err_text,
            node_log=f"Execution failed: {err_text}",
        )
        return {
            "node_id": int(node["id"]),
            "status": "FAILED",
            "pipeline_run_id": None,
            "error": err_text,
            "duration": duration,
        }


@router.post("/workflows/{workflow_id}/run")
def run_workflow(workflow_id: int, payload: RunWorkflowRequest, _u=Depends(_dev_or_admin)):
    workflow = _fetch_workflow_graph(workflow_id)
    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])

    if not nodes:
        raise HTTPException(status_code=400, detail="Workflow has no nodes to run")

    execution_mode = _normalize_execution_mode(payload.execution_mode or workflow.get("execution_mode") or "serial")
    stop_on_error = workflow.get("stop_on_error") if payload.stop_on_error is None else payload.stop_on_error
    max_parallel = max(1, min(int(payload.max_parallel_nodes or 4), 8))

    node_ids = [int(node["id"]) for node in nodes]
    _validate_workflow_graph(node_ids, edges)

    node_by_id = {int(node["id"]): node for node in nodes}
    adjacency: Dict[int, List[int]] = {node_id: [] for node_id in node_ids}
    indegree = {node_id: 0 for node_id in node_ids}
    predecessors: Dict[int, set] = {node_id: set() for node_id in node_ids}

    for edge in edges:
        from_node_id = int(edge["from_node_id"])
        to_node_id = int(edge["to_node_id"])
        adjacency[from_node_id].append(to_node_id)
        indegree[to_node_id] += 1
        predecessors[to_node_id].add(from_node_id)

    started_at = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO workflow_runs (
            workflow_id, status, trigger_type, started_at, ended_at,
            duration_seconds, total_nodes, success_nodes, failed_nodes, skipped_nodes,
            error_message, initiated_by, run_log
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            workflow_id,
            "RUNNING",
            payload.trigger_type or "MANUAL",
            started_at,
            None,
            None,
            len(node_ids),
            0,
            0,
            0,
            None,
            payload.initiated_by,
            None,
        ),
    )
    workflow_run_id = int(cur.lastrowid)
    conn.commit()
    conn.close()

    run_node_ids: Dict[int, int] = {}
    for node in nodes:
        run_node_ids[int(node["id"])] = _upsert_workflow_run_node(
            workflow_run_id,
            workflow_id,
            int(node["id"]),
            int(node["pipeline_id"]),
            execution_group=0,
        )

    success_nodes = set()
    failed_nodes = set()
    skipped_nodes = set()
    pending_nodes = set(node_ids)
    run_logs: List[str] = []
    first_error: Optional[str] = None
    execution_group = 0

    def choose_ready_nodes() -> List[int]:
        candidates = []
        for node_id in sorted(pending_nodes):
            if indegree[node_id] != 0:
                continue
            if any(pred in failed_nodes or pred in skipped_nodes for pred in predecessors[node_id]):
                continue
            candidates.append(node_id)
        return candidates

    while pending_nodes:
        ready_nodes = choose_ready_nodes()
        if not ready_nodes:
            for node_id in sorted(pending_nodes):
                skipped_nodes.add(node_id)
                run_node_id = run_node_ids[node_id]
                _update_workflow_run_node(
                    run_node_id,
                    status="SKIPPED",
                    ended_at=utc_now_str(),
                    error_message="Skipped because one or more upstream nodes failed",
                    node_log="Skipped due to failed dependency",
                )
                run_logs.append(f"Node {node_id}: SKIPPED (failed dependency)")
            pending_nodes.clear()
            break

        execution_group += 1
        current_batch = ready_nodes if execution_mode == "parallel" else [ready_nodes[0]]

        results: List[Dict[str, Any]] = []
        if execution_mode == "parallel" and len(current_batch) > 1:
            workers = min(max_parallel, len(current_batch))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {}
                for node_id in current_batch:
                    node = node_by_id[node_id]
                    run_node_id = run_node_ids[node_id]
                    future = pool.submit(
                        _execute_workflow_node,
                        workflow_run_id,
                        run_node_id,
                        node,
                        payload.trigger_type or "WORKFLOW",
                        payload.initiated_by,
                        payload.allow_ddl_execute,
                        execution_group,
                    )
                    future_map[future] = node_id
                for future in as_completed(future_map):
                    results.append(future.result())
        else:
            for node_id in current_batch:
                node = node_by_id[node_id]
                run_node_id = run_node_ids[node_id]
                results.append(
                    _execute_workflow_node(
                        workflow_run_id,
                        run_node_id,
                        node,
                        payload.trigger_type or "WORKFLOW",
                        payload.initiated_by,
                        payload.allow_ddl_execute,
                        execution_group,
                    )
                )

        current_failed = False
        for result in results:
            node_id = int(result["node_id"])
            pending_nodes.discard(node_id)

            if result["status"] == "SUCCESS":
                success_nodes.add(node_id)
                run_logs.append(
                    f"Node {node_id}: SUCCESS (pipeline_run_id={result.get('pipeline_run_id')}, duration={result.get('duration')}s)"
                )
                for downstream in adjacency[node_id]:
                    indegree[downstream] -= 1
            else:
                current_failed = True
                failed_nodes.add(node_id)
                err_text = result.get("error") or "Unknown error"
                run_logs.append(f"Node {node_id}: FAILED - {err_text}")
                if first_error is None:
                    first_error = err_text

        if current_failed and stop_on_error:
            for node_id in sorted(pending_nodes):
                skipped_nodes.add(node_id)
                _update_workflow_run_node(
                    run_node_ids[node_id],
                    status="SKIPPED",
                    ended_at=utc_now_str(),
                    error_message="Skipped because workflow is configured to stop on first error",
                    node_log="Skipped due to stop_on_error behavior",
                )
                run_logs.append(f"Node {node_id}: SKIPPED (stop_on_error)")
            pending_nodes.clear()
            break

    ended_at = utc_now_str()
    duration = compute_duration(started_at, ended_at)
    success_count = len(success_nodes)
    failed_count = len(failed_nodes)
    skipped_count = len(skipped_nodes)

    if failed_count == 0 and skipped_count == 0:
        final_status = "SUCCESS"
    elif success_count == 0:
        final_status = "FAILED"
    else:
        final_status = "PARTIAL_SUCCESS"

    _update_workflow_run(
        workflow_run_id,
        status=final_status,
        ended_at=ended_at,
        duration_seconds=duration,
        success_nodes=success_count,
        failed_nodes=failed_count,
        skipped_nodes=skipped_count,
        error_message=first_error,
        run_log="\n".join(run_logs),
    )

    _update_workflow_schedule_last_run(workflow_id)
    return _get_workflow_run_response(workflow_run_id)


@router.get("/workflows/{workflow_id}/runs")
def list_workflow_runs(workflow_id: int, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute(
        """
        SELECT *
        FROM workflow_runs
        WHERE workflow_id = ?
        ORDER BY id DESC
        """,
        (workflow_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [row_to_dict(row) for row in rows]


@router.get("/workflow-runs/{workflow_run_id}")
def get_workflow_run(workflow_run_id: int, _u=Depends(get_current_user)):
    return _get_workflow_run_response(workflow_run_id)


def _update_workflow_schedule_last_run(workflow_id: int):
    now = utc_now_str()
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE workflow_schedules SET last_run_at = ?, updated_at = ? WHERE workflow_id = ?",
        (now, now, workflow_id),
    )
    conn.commit()
    conn.close()


@router.post("/workflows/{workflow_id}/schedule")
def create_workflow_schedule(workflow_id: int, payload: WorkflowScheduleCreate, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")

    cur.execute("SELECT id FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail="Schedule already exists for this workflow. Use PUT to update.")

    if payload.schedule_type == "cron" and not payload.cron_expression:
        conn.close()
        raise HTTPException(status_code=400, detail="cron_expression is required for cron schedule type")
    if payload.schedule_type == "interval" and not payload.interval_minutes:
        conn.close()
        raise HTTPException(status_code=400, detail="interval_minutes is required for interval schedule type")

    now = utc_now_str()
    cur.execute(
        """
        INSERT INTO workflow_schedules (
            workflow_id, schedule_type, cron_expression, interval_minutes,
            is_active, last_run_at, next_run_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            workflow_id,
            payload.schedule_type,
            payload.cron_expression,
            payload.interval_minutes,
            payload.is_active,
            None,
            None,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()

    _sync_workflow_schedule_to_scheduler(workflow_id)
    return _get_workflow_schedule_response(workflow_id)


@router.get("/workflows/{workflow_id}/schedule")
def get_workflow_schedule(workflow_id: int, _u=Depends(get_current_user)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Workflow not found")
    conn.close()
    return _get_workflow_schedule_response(workflow_id)


@router.put("/workflows/{workflow_id}/schedule")
def update_workflow_schedule(workflow_id: int, payload: WorkflowScheduleUpdate, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="No schedule found for this workflow")

    fields = []
    values: List[Any] = []
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
        return _get_workflow_schedule_response(workflow_id)

    now = utc_now_str()
    fields.append("updated_at = ?")
    values.append(now)
    values.append(workflow_id)

    cur.execute(
        f"""
        UPDATE workflow_schedules
        SET {", ".join(fields)}
        WHERE workflow_id = ?
        """,
        values,
    )
    conn.commit()
    conn.close()

    _sync_workflow_schedule_to_scheduler(workflow_id)
    return _get_workflow_schedule_response(workflow_id)


@router.delete("/workflows/{workflow_id}/schedule")
def delete_workflow_schedule(workflow_id: int, _u=Depends(_dev_or_admin)):
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="No schedule found for this workflow")

    cur.execute("DELETE FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    conn.commit()
    conn.close()
    _remove_workflow_schedule_from_scheduler(workflow_id)

    return {"status": "success", "message": "Workflow schedule deleted"}


def _get_workflow_schedule_response(workflow_id: int) -> dict:
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ws.*, w.name AS workflow_name
        FROM workflow_schedules ws
        LEFT JOIN workflows w ON w.id = ws.workflow_id
        WHERE ws.workflow_id = ?
        """,
        (workflow_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"schedule": None, "workflow_id": workflow_id}

    data = row_to_dict(row)
    next_run = _get_workflow_next_run_time(workflow_id)
    if next_run:
        data["next_run_at"] = next_run
    return data


def _sync_workflow_schedule_to_scheduler(workflow_id: int):
    if _scheduler_ref is None:
        return

    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM workflow_schedules WHERE workflow_id = ?", (workflow_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        _remove_workflow_schedule_from_scheduler(workflow_id)
        return

    schedule = row_to_dict(row)
    job_id = f"workflow_schedule_{workflow_id}"

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
                _scheduled_workflow_run,
                trigger=trigger,
                args=[workflow_id],
                id=job_id,
                replace_existing=True,
                name=f"Workflow {workflow_id} (cron)",
            )
        elif schedule["schedule_type"] == "interval" and schedule.get("interval_minutes"):
            trigger = IntervalTrigger(minutes=schedule["interval_minutes"])
            _scheduler_ref.add_job(
                _scheduled_workflow_run,
                trigger=trigger,
                args=[workflow_id],
                id=job_id,
                replace_existing=True,
                name=f"Workflow {workflow_id} (interval {schedule['interval_minutes']}m)",
            )

        job = _scheduler_ref.get_job(job_id)
        if job and job.next_run_time:
            next_run_str = job.next_run_time.isoformat(timespec="seconds")
            now = utc_now_str()
            db_conn = get_sqlite_conn()
            db_cur = db_conn.cursor()
            db_cur.execute(
                "UPDATE workflow_schedules SET next_run_at = ?, updated_at = ? WHERE workflow_id = ?",
                (next_run_str, now, workflow_id),
            )
            db_conn.commit()
            db_conn.close()
    except Exception as exc:
        logger.error(f"Failed to sync schedule for workflow {workflow_id}: {exc}")


def _remove_workflow_schedule_from_scheduler(workflow_id: int):
    if _scheduler_ref is None:
        return
    job_id = f"workflow_schedule_{workflow_id}"
    try:
        _scheduler_ref.remove_job(job_id)
    except Exception:
        pass


def _get_workflow_next_run_time(workflow_id: int) -> Optional[str]:
    if _scheduler_ref is None:
        return None

    job_id = f"workflow_schedule_{workflow_id}"
    try:
        job = _scheduler_ref.get_job(job_id)
        if job and job.next_run_time:
            return job.next_run_time.isoformat(timespec="seconds")
    except Exception:
        pass
    return None


def _scheduled_workflow_run(workflow_id: int):
    logger.info(f"Scheduled run triggered for workflow {workflow_id}")
    try:
        run_workflow(
            workflow_id,
            RunWorkflowRequest(
                trigger_type="SCHEDULED",
                initiated_by="scheduler",
                allow_ddl_execute=False,
            ),
        )
    except Exception as exc:
        logger.error(f"Scheduled run failed for workflow {workflow_id}: {exc}")