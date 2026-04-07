from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field
import psycopg2
import os
import json
import re
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Any, Dict
from openai import OpenAI
from app.relationships import compute_pk_score, compute_fk_score
from app.pipeline_builder import router as pipeline_builder_router
from app.pipeline_builder import set_scheduler as set_pipeline_scheduler
from app.api.routes_auth import router as auth_router
from app.api.routes_admin import router as admin_router
from app.core.security import get_current_user, require_role


# =========================
# OpenAI config
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# =========================
# FastAPI app
# =========================
app = FastAPI(title="Data Lifecycle Copilot")
app.include_router(pipeline_builder_router)
app.include_router(auth_router, prefix="/api")
app.include_router(admin_router, prefix="/api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

saved_connections = {}
next_connection_id = 1


def _load_saved_connections():
    """Reload connections from SQLite into memory on startup."""
    global next_connection_id
    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM connections WHERE is_active = 1")
    rows = cur.fetchall()
    max_id = 0
    for row in rows:
        row_dict = dict(row)
        cid = row_dict["id"]
        saved_connections[cid] = row_dict
        if cid > max_id:
            max_id = cid
    next_connection_id = max_id + 1
    conn.close()
    print(f"Loaded {len(rows)} saved connection(s) from SQLite (next_id={next_connection_id}).")

# ── Auth shortcuts for inline routes ──
_any_authenticated = Depends(get_current_user)
_dev_or_admin = Depends(require_role("admin", "developer"))


# =========================
# Local app store (SQLite)
# =========================
APP_DB_PATH = Path(__file__).resolve().parent / "copilot_app.db"


def get_app_store_conn():
    conn = sqlite3.connect(APP_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_app_store():
    conn = get_app_store_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prompt_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id INTEGER,
            database_name TEXT,
            schema_name TEXT,
            object_name TEXT,
            user_prompt TEXT,
            statement_type TEXT,
            sql_text TEXT,
            created_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS connections (
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
            is_active INTEGER DEFAULT 1
        )
    """)

    conn.commit()
    conn.close()


@app.on_event("startup")
def on_startup():
    init_app_store()
    _load_saved_connections()
    _start_scheduler()
    # Create SQLAlchemy-managed tables (users, connections, module_access)
    from app.core.database import engine
    from app.models.base import Base
    import app.models  # noqa: ensure all models are imported
    Base.metadata.create_all(bind=engine)

    # Migrate existing users table — add columns introduced by auth feature
    from sqlalchemy import text, inspect as sa_inspect
    with engine.connect() as conn:
        inspector = sa_inspect(engine)
        existing = {c["name"] for c in inspector.get_columns("users")}
        migrations = [
            ("hashed_password", "VARCHAR(255) NOT NULL DEFAULT ''"),
            ("role", "VARCHAR(50) NOT NULL DEFAULT 'tester'"),
            ("created_at", "TIMESTAMP DEFAULT NOW()"),
        ]
        for col_name, col_def in migrations:
            if col_name not in existing:
                conn.execute(text(f'ALTER TABLE users ADD COLUMN {col_name} {col_def}'))
        # Drop legacy is_admin column if it exists
        if "is_admin" in existing:
            conn.execute(text('ALTER TABLE users DROP COLUMN is_admin'))
        conn.commit()


@app.on_event("shutdown")
def on_shutdown():
    _stop_scheduler()


_scheduler_instance = None


def _start_scheduler():
    global _scheduler_instance
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler_instance = BackgroundScheduler(timezone="UTC")
        _scheduler_instance.start()
        set_pipeline_scheduler(_scheduler_instance)
        print("APScheduler started successfully.")
    except ImportError:
        print("WARNING: apscheduler not installed. Scheduling features will be unavailable.")
    except Exception as e:
        print(f"WARNING: Failed to start scheduler: {e}")


def _stop_scheduler():
    global _scheduler_instance
    if _scheduler_instance:
        try:
            _scheduler_instance.shutdown(wait=False)
            print("APScheduler shut down.")
        except Exception:
            pass


# =========================
# Request models
# =========================
class ConnectionRequest(BaseModel):
    name: str
    db_type: str
    host: str
    port: str
    database_name: str | None = "postgres"
    schema_name: str | None = "public"
    username: str
    password: str | None = ""
    account: str | None = ""
    warehouse: str | None = ""
    role: str | None = ""


class QueryRequest(BaseModel):
    query: str
    limit: int = 25
    offset: int = 0


class SchemaAliasedRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class SqlGenerationRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: str = Field(alias="schema")
    object_name: str
    user_prompt: str
    selected_tables: List[str] = []


class JoinSuggestionRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: str = Field(alias="schema")
    object_name: str


class FixSqlRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: Optional[str] = Field(default="public", alias="schema")
    object_name: Optional[str] = None
    sql: str
    error: str


class InsightsAgenticRAGRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: Optional[str] = None
    schema_name: Optional[str] = Field(default="public", alias="schema")
    object_name: Optional[str] = None
    user_prompt: str
    conversation_history: List[Dict[str, str]] = Field(default_factory=list)
    selected_objects: List[str] = Field(default_factory=list)


class RecipeSaveRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: str = Field(alias="schema")
    object_name: str
    recipe_name: str
    user_prompt: str
    statement_type: str = "select"
    selected_tables: List[str] = []
    sql: str
    explanation: str = ""


class DataQualitySqlRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: str = Field(alias="schema")
    object_name: str


class ColumnInfo(BaseModel):
    name: str
    data_type: Optional[str] = None


class TransformationSuggestionRequest(BaseModel):
    object_name: str
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    columns: List[ColumnInfo]
    sample_rows: List[Dict[str, Any]] = []
    user_prompt: Optional[str] = ""
    dialect: Optional[str] = "postgres"


# =========================
# DB helpers
# =========================
def get_postgres_connection(conn_data: dict, database_name: str | None = None):
    dbname = database_name or conn_data.get("database_name") or "postgres"
    return psycopg2.connect(
        host=conn_data["host"],
        port=conn_data["port"],
        dbname=dbname,
        user=conn_data["username"],
        password=conn_data["password"],
    )


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def get_db_connection_by_id(conn_id: int, database_name: str | None = None):
    conn_data = saved_connections.get(conn_id)
    if not conn_data:
        raise HTTPException(status_code=404, detail="Connection not found")

    if conn_data["db_type"].lower() != "postgres":
        raise HTTPException(status_code=400, detail="Only PostgreSQL is supported for now")

    try:
        return get_postgres_connection(conn_data, database_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open connection: {str(e)}")


def run_query(connection, query: str, params=None):
    cur = connection.cursor()
    cur.execute(query, params or [])
    return cur


def normalize_object_type(table_type: str | None) -> str:
    if not table_type:
        return "OBJECT"
    upper = table_type.upper()
    if upper == "VIEW":
        return "VIEW"
    if upper == "BASE TABLE":
        return "TABLE"
    return upper


def get_column_stats(connection, schema: str, table_name: str):
    col_sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    cur = run_query(connection, col_sql, [schema, table_name])
    columns = cur.fetchall()
    cur.close()

    stats = {}
    q_schema = quote_ident(schema)
    q_table = quote_ident(table_name)

    for column_name, data_type in columns:
        q_col = quote_ident(column_name)
        stat_sql = f"""
            SELECT
                COUNT(*) AS total_count,
                COUNT(DISTINCT {q_col}) AS distinct_count,
                SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) AS null_count
            FROM {q_schema}.{q_table}
        """
        s_cur = run_query(connection, stat_sql)
        row = s_cur.fetchone()
        s_cur.close()

        total_count = row[0] or 0
        distinct_count = row[1] or 0
        null_count = row[2] or 0
        denom = total_count if total_count > 0 else 1

        stats[column_name] = {
            "data_type": data_type,
            "uniqueness": distinct_count / denom,
            "null_ratio": null_count / denom,
        }

    return stats


def get_object_columns(connection, schema: str, object_name: str):
    cur = run_query(
        connection,
        """
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        [schema, object_name],
    )
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "column_name": r[0],
            "data_type": r[1],
            "is_nullable": r[2],
            "ordinal_position": r[3],
        }
        for r in rows
    ]


def get_object_type(connection, schema: str, object_name: str):
    cur = run_query(
        connection,
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        [schema, object_name],
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return "VIEW" if row[0] == "VIEW" else "TABLE"


def get_schema_objects(connection, schema: str):
    cur = run_query(
        connection,
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        [schema],
    )
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "name": r[0],
            "type": "VIEW" if r[1] == "VIEW" else "TABLE"
        }
        for r in rows
    ]


def get_all_schema_objects(connection):
    """Get all user objects across all non-system schemas."""
    cur = run_query(
        connection,
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        """,
    )
    rows = cur.fetchall()
    cur.close()

    return [
        {
            "schema": r[0],
            "name": r[1],
            "type": "VIEW" if r[2] == "VIEW" else "TABLE",
        }
        for r in rows
    ]


def get_view_definition(connection, schema: str, object_name: str):
    cur = run_query(
        connection,
        """
        SELECT view_definition
        FROM information_schema.views
        WHERE table_schema = %s
          AND table_name = %s
        """,
        [schema, object_name],
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def get_object_metadata(connection, schema: str, object_name: str, object_type: str | None = None):
    resolved_type = object_type or get_object_type(connection, schema, object_name)
    if not resolved_type:
        return None

    columns = get_object_columns(connection, schema, object_name)
    view_definition = get_view_definition(connection, schema, object_name) if resolved_type == "VIEW" else None

    return {
        "schema_name": schema,
        "object_name": object_name,
        "object_type": resolved_type,
        "columns": [
            {
                "column_name": col["column_name"],
                "data_type": col["data_type"],
                "is_nullable": col["is_nullable"],
            }
            for col in columns[:150]
        ],
        "view_definition": view_definition,
    }


# =========================
# LLM helpers
# =========================
def extract_json_block(text: str):
    if not text:
        return None

    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

    fenced = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        block = fenced.group(1)
        try:
            return json.loads(block)
        except Exception:
            pass

    bracket_start = text.find("{")
    bracket_end = text.rfind("}")
    if bracket_start != -1 and bracket_end != -1 and bracket_end > bracket_start:
        candidate = text[bracket_start:bracket_end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None

    return None


def call_llm_json(system_prompt: str, user_prompt: str):
    if not OPENAI_API_KEY or client is None:
        return {
            "enabled": False,
            "reason": "OPENAI_API_KEY is not configured",
            "content": None
        }

    try:
        response = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {
                    "role": "system",
                    "content": [
                        {"type": "input_text", "text": system_prompt}
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": user_prompt}
                    ],
                },
            ],
        )

        text_output = getattr(response, "output_text", None)

        if not text_output:
            try:
                chunks = []
                for item in response.output:
                    for content_item in getattr(item, "content", []):
                        text_value = getattr(content_item, "text", None)
                        if text_value:
                            chunks.append(text_value)
                text_output = "\n".join(chunks).strip()
            except Exception:
                text_output = None

        parsed = extract_json_block(text_output or "")

        if parsed is None:
            return {
                "enabled": True,
                "reason": f"OpenAI response was not valid JSON. Raw output: {text_output or 'EMPTY'}",
                "content": None
            }

        return {
            "enabled": True,
            "reason": "",
            "content": parsed
        }

    except Exception as e:
        return {
            "enabled": True,
            "reason": f"OpenAI request error: {str(e)}",
            "content": None
        }


# =========================
# Relationship logic
# =========================
def build_relationship_summary(connection, schema: str, object_name: str):
    pk_query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
      AND tc.table_name = %s
    ORDER BY kcu.ordinal_position
    """
    cur = run_query(connection, pk_query, [schema, object_name])
    pk_cols = [r[0] for r in cur.fetchall()]
    cur.close()

    fk_query = """
    SELECT
        kcu.column_name,
        ccu.table_schema,
        ccu.table_name,
        ccu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = %s
      AND tc.table_name = %s
    """
    cur = run_query(connection, fk_query, [schema, object_name])
    fk_rows = cur.fetchall()
    cur.close()

    foreign_keys = [
        {
            "column": r[0],
            "ref_schema": r[1],
            "ref_table": r[2],
            "ref_column": r[3],
            "confidence": "verified"
        }
        for r in fk_rows
    ]

    referenced_by_query = """
    SELECT
        tc.table_schema,
        tc.table_name,
        kcu.column_name,
        ccu.column_name AS referenced_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND ccu.table_schema = %s
      AND ccu.table_name = %s
    """
    cur = run_query(connection, referenced_by_query, [schema, object_name])
    referenced_by_rows = cur.fetchall()
    cur.close()

    referenced_by = [
        {
            "schema": r[0],
            "table": r[1],
            "column": r[2],
            "referenced_column": r[3],
        }
        for r in referenced_by_rows
    ]

    stats = get_column_stats(connection, schema, object_name)

    pk_candidates = []
    for col in stats:
        score, reasons = compute_pk_score(col, stats[col])
        if score > 0.4:
            pk_candidates.append({
                "column": col,
                "score": round(score, 2),
                "reasons": reasons
            })

    pk_candidates.sort(key=lambda x: x["score"], reverse=True)

    tables_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s
    """
    cur = run_query(connection, tables_query, [schema])
    tables = [t[0] for t in cur.fetchall()]
    cur.close()

    suggestions = []

    for tgt in tables:
        if tgt == object_name:
            continue

        try:
            tgt_stats = get_column_stats(connection, schema, tgt)
        except Exception:
            continue

        for src_col in stats:
            for tgt_col in tgt_stats:
                score, reasons = compute_fk_score(
                    src_col,
                    tgt_col,
                    stats[src_col],
                    tgt_stats[tgt_col]
                )

                if score > 0.6:
                    suggestions.append({
                        "from_table": object_name,
                        "from_column": src_col,
                        "to_table": tgt,
                        "to_column": tgt_col,
                        "score": round(score, 2),
                        "reasons": reasons
                    })

    suggestions.sort(key=lambda x: x["score"], reverse=True)

    return {
        "object_name": object_name,
        "primary_keys": pk_cols,
        "foreign_keys": foreign_keys,
        "referenced_by": referenced_by,
        "pk_candidates": pk_candidates[:5],
        "suggested_relationships": suggestions[:10]
    }


# =========================
# SQL / prompt helpers
# =========================
def detect_requested_statement_type(prompt_text: str) -> str:
    text = (prompt_text or "").strip().lower()

    # Helper sets for compound intent detection
    create_signals = [
        "create table as", "ctas", "create table", "create a table",
        "creates a table", "create statement", "create the table",
        "build a table", "build table", "ddl", "define table",
        "table definition", "history table", "store history",
    ]
    merge_signals = [
        "merge into", " merge ", "upsert", "scd type 2", "scd2",
        "slowly changing", "merge statement",
    ]

    has_create = any(x in text for x in create_signals)
    has_merge = any(x in text for x in merge_signals)

    # If user asks to CREATE a table AND mentions SCD/merge concepts,
    # the intent is DDL (CREATE TABLE with SCD columns), not a MERGE DML.
    if has_create and has_merge:
        return "create_table"

    # Order matters: check most specific patterns first
    if has_merge:
        return "merge"
    if any(x in text for x in [
        "insert into", "insert statement", "load into", "staging load",
        "insert rows", "populate table",
    ]):
        return "insert"
    if any(x in text for x in [
        "create or replace view", "create view", "reporting view",
        "create a view", "creates a view", "build a view",
    ]):
        return "create_view"
    if has_create:
        return "create_table"
    if any(x in text for x in [
        "update statement", "update ", "update rows", "set column",
    ]):
        return "update"
    if any(x in text for x in [
        "delete statement", "delete from", "remove rows", "purge",
    ]):
        return "delete"
    if any(x in text for x in ["with query", "cte query"]):
        return "with_query"

    return "auto"


def statement_type_label(statement_type: str) -> str:
    mapping = {
        "select": "SELECT",
        "insert": "INSERT INTO ... SELECT",
        "create_view": "CREATE VIEW",
        "create_table": "CREATE TABLE (DDL or CTAS)",
        "update": "UPDATE",
        "delete": "DELETE",
        "merge": "MERGE",
        "with_query": "WITH ... SELECT/INSERT",
        "auto": "whatever the user prompt requests",
    }
    return mapping.get(statement_type, "SELECT")


def is_previewable_sql(sql: str) -> bool:
    text = (sql or "").strip().lower()
    return text.startswith("select") or text.startswith("with")


def record_prompt_history(
    connection_id: int,
    database_name: str,
    schema: str,
    object_name: str,
    user_prompt: str,
    statement_type: str,
    sql_text: str
):
    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO prompt_history (
            connection_id, database_name, schema_name, object_name,
            user_prompt, statement_type, sql_text, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        connection_id,
        database_name,
        schema,
        object_name,
        user_prompt,
        statement_type,
        sql_text,
        datetime.utcnow().isoformat()
    ))
    conn.commit()
    conn.close()


def extract_prompt_object_names(prompt_text: str, schema_objects: List[Dict[str, str]], base_object: str) -> List[str]:
    text = (prompt_text or "").lower()
    matches = {base_object}

    for item in sorted(schema_objects, key=lambda obj: len(obj["name"]), reverse=True):
        object_name = item["name"]
        pattern = rf'(?<![a-z0-9_]){re.escape(object_name.lower())}(?![a-z0-9_])'
        if re.search(pattern, text):
            matches.add(object_name)

    return sorted(matches)


def parse_object_reference(reference: str, default_schema: str) -> tuple[str, str]:
    token = (reference or "").strip().strip(",;")
    if not token:
        return default_schema, ""

    parts = [part.strip().strip('"') for part in token.split(".") if part.strip()]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return default_schema, parts[0]


def match_prompt_objects(prompt_text: str, all_objects: List[Dict[str, str]]) -> List[Dict[str, str]]:
    prompt_lower = (prompt_text or "").lower().strip()
    if not prompt_lower:
        return []

    stopwords = {
        "the", "and", "for", "with", "from", "into", "over", "under", "than", "then",
        "show", "what", "which", "where", "when", "how", "are", "was", "were", "have",
        "has", "had", "that", "this", "these", "those", "last", "next", "past", "days",
        "day", "week", "weeks", "month", "months", "year", "years", "please", "about",
        "compare", "analysis", "analyze", "trend", "trends", "rate", "rates", "high", "low",
        "more", "less", "than", "across", "between", "by", "of", "to", "in", "on", "at",
    }

    raw_tokens = re.findall(r"[a-z0-9_]+", prompt_lower)
    tokens: List[str] = []
    for token in raw_tokens:
        if len(token) < 3 or token in stopwords:
            continue
        tokens.append(token)
        if token.endswith("s") and len(token) > 4:
            tokens.append(token[:-1])

    tokens = list(dict.fromkeys(tokens))

    ranked: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for obj in all_objects:
        schema = obj["schema"]
        name = obj["name"]
        schema_lower = schema.lower()
        name_lower = name.lower()
        key = (schema_lower, name_lower)

        if key in seen:
            continue

        qualified_pattern = rf'(?<![a-z0-9_]){re.escape(schema_lower)}\.{re.escape(name_lower)}(?![a-z0-9_])'
        plain_pattern = rf'(?<![a-z0-9_]){re.escape(name_lower)}(?![a-z0-9_])'

        score = 0.0
        if re.search(qualified_pattern, prompt_lower):
            score += 120
        if re.search(plain_pattern, prompt_lower):
            score += 90

        name_parts = [part for part in re.split(r"[^a-z0-9]+", name_lower) if part]

        for token in tokens:
            if token in name_parts:
                score += 35
                continue

            if token in name_lower:
                score += 18
                continue

            if len(token) >= 5:
                for part in name_parts:
                    if len(part) < 4:
                        continue
                    if part.startswith(token[:4]) or token.startswith(part):
                        score += 8
                        break

            if token == schema_lower:
                score += 10

        if score <= 0:
            continue

        ranked.append(
            {
                "schema": schema,
                "name": name,
                "type": obj["type"],
                "score": score,
            }
        )
        seen.add(key)

    ranked.sort(key=lambda item: (-item["score"], len(item["name"]), item["schema"], item["name"]))
    return [{"schema": item["schema"], "name": item["name"], "type": item["type"]} for item in ranked[:24]]


def is_data_analytics_prompt(prompt_text: str, matched_objects: List[Dict[str, str]]) -> bool:
    normalized = re.sub(r"\s+", " ", (prompt_text or "").strip().lower())
    if not normalized:
        return False

    if matched_objects:
        return True

    sql_signals = [
        "select ", " from ", " join ", " where ", " group by ", " order by ",
        "having ", "count(", "sum(", "avg(", "min(", "max(",
    ]
    if any(signal in f" {normalized} " for signal in sql_signals):
        return True

    analytics_terms = [
        "analy", "analysis", "insight", "kpi", "metric", "trend", "compare", "breakdown",
        "summary", "distribution", "segment", "cohort", "forecast", "anomaly", "correlation",
        "top", "bottom", "highest", "lowest", "count", "total", "average", "avg", "sum",
        "min", "max", "median", "ratio", "rate", "growth", "decline", "last ", "over ",
        "between", "payment", "payments", "revenue", "sales", "orders", "customers", "inventory",
        "stock", "conversion", "churn", "retention", "failed", "success", "currency", "amount",
        "table", "view", "column", "dataset", "database", "schema",
    ]
    if any(term in normalized for term in analytics_terms):
        return True

    return False


def build_out_of_scope_insights_reply() -> str:
    return (
        "I am here to help with data analysis questions. "
        "Please ask about trends, comparisons, KPIs, anomalies, or strategy from your data. "
        "Try prompts like: Show payment trend over the last 30 days; "
        "Compare average payment amount by status; "
        "Which segments have high volume but low success rate?"
    )


def summarize_query_result(columns: List[str], rows: List[List[Any]]) -> Dict[str, Any]:
    if not columns or not rows:
        return {
            "stats": None,
            "chart": [],
            "chart_label": "",
            "recommendations": [],
        }

    numeric_indexes: List[int] = []
    for idx in range(len(columns)):
        has_numeric = False
        for row in rows:
            if idx >= len(row):
                continue
            try:
                float(row[idx])
                has_numeric = True
                break
            except Exception:
                continue
        if has_numeric:
            numeric_indexes.append(idx)

    metric_index = numeric_indexes[0] if numeric_indexes else -1
    dimension_index = -1
    for idx in range(len(columns)):
        if idx not in numeric_indexes:
            dimension_index = idx
            break

    stats = None
    chart: List[Dict[str, Any]] = []
    chart_label = ""

    if metric_index >= 0:
        values: List[float] = []
        for row in rows:
            if metric_index >= len(row):
                continue
            try:
                values.append(float(row[metric_index]))
            except Exception:
                continue

        if values:
            total = sum(values)
            stats = {
                "metric": columns[metric_index],
                "count": len(values),
                "sum": total,
                "avg": total / len(values),
                "min": min(values),
                "max": max(values),
            }

        for idx, row in enumerate(rows[:8]):
            if metric_index >= len(row):
                continue
            try:
                value = float(row[metric_index])
            except Exception:
                continue

            label = f"Row {idx + 1}"
            if dimension_index >= 0 and dimension_index < len(row):
                label = str(row[dimension_index] if row[dimension_index] is not None else f"Row {idx + 1}")

            chart.append({"label": label, "value": value})

        if chart:
            chart_label = (
                f"{columns[metric_index]} by {columns[dimension_index]}"
                if dimension_index >= 0
                else f"{columns[metric_index]} by row"
            )

    lowered_columns = [str(col).lower() for col in columns]
    has_sales = any("sales" in col or "revenue" in col for col in lowered_columns)
    has_inventory = any("stock" in col or "inventory" in col for col in lowered_columns)
    has_age = any("age" in col or "old" in col for col in lowered_columns)

    recommendations: List[str] = []
    if has_sales and has_inventory:
        recommendations.append(
            "Focus promotions on high-stock and low-sales products and monitor weekly lift."
        )
    if has_age:
        recommendations.append(
            "For older products, use phased markdowns and bundle with fast movers to improve turnover."
        )
    if stats and not recommendations:
        recommendations.append(
            "Use top and bottom segments from this result to run pricing and channel experiments."
        )

    return {
        "stats": stats,
        "chart": chart,
        "chart_label": chart_label,
        "recommendations": recommendations,
    }


def build_agentic_response_text(
    user_prompt: str,
    schema_name: str,
    object_name: str,
    generation_result: Dict[str, Any],
    query_result: Optional[Dict[str, Any]],
    insights: Dict[str, Any],
) -> str:
    lines: List[str] = [
        f'I analyzed your request "{user_prompt}" on {schema_name}.{object_name}.'
    ]

    explanation = (generation_result or {}).get("explanation")
    if explanation:
        lines.append(str(explanation))

    stats = (insights or {}).get("stats")
    if stats:
        lines.append(
            f"{stats['metric']} avg={stats['avg']:.2f}, min={stats['min']:.2f}, max={stats['max']:.2f}."
        )

    if query_result and isinstance(query_result.get("rows"), list):
        lines.append(f"Returned {len(query_result['rows'])} rows in preview.")

    recommendations = (insights or {}).get("recommendations") or []
    if recommendations:
        lines.append("I included actionable recommendations below.")

    return " ".join(lines)


def normalize_sql_identifier(token: str | None) -> str:
    if not token:
        return ""
    return token.strip().strip(",;")


def unquote_sql_identifier(token: str | None) -> str:
    normalized = normalize_sql_identifier(token)
    if normalized.startswith('"') and normalized.endswith('"') and len(normalized) >= 2:
        return normalized[1:-1].replace('""', '"')
    return normalized


def split_relation_name(token: str, default_schema: str) -> tuple[str, str] | None:
    normalized = normalize_sql_identifier(token)
    if not normalized or normalized.startswith("("):
        return None

    parts = [unquote_sql_identifier(part) for part in normalized.split(".")]
    if len(parts) == 1:
        return default_schema, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) >= 3:
        # Support fully qualified names like database.schema.table.
        return parts[-2], parts[-1]
    return None


def extract_cte_names(sql: str) -> set[str]:
    text = (sql or "").strip()
    cte_names: set[str] = set()
    if not text.lower().startswith("with"):
        return cte_names

    idx = 4
    length = len(text)
    if text[idx:].lstrip().lower().startswith("recursive"):
        idx = text.lower().find("recursive", idx) + len("recursive")

    name_pattern = re.compile(r'\s*(?:"([^"]+)"|([a-zA-Z_][\w$]*))(?:\s*\([^)]*\))?\s+as\s*\(', re.IGNORECASE)

    while idx < length:
        match = name_pattern.match(text, idx)
        if not match:
            break

        cte_name = match.group(1) or match.group(2)
        cte_names.add(cte_name)
        idx = match.end() - 1
        depth = 0

        while idx < length:
            char = text[idx]
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    idx += 1
                    break
            idx += 1

        while idx < length and text[idx].isspace():
            idx += 1

        if idx >= length or text[idx] != ',':
            break
        idx += 1

    return {name.lower() for name in cte_names}


def extract_sql_source_relations(sql: str, default_schema: str) -> tuple[list[dict], dict[str, dict]]:
    relation_pattern = re.compile(
        r'\b(from|join)\s+((?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\.(?:"[^"]+"|[a-zA-Z_][\w$]*)){0,2})(?:\s+(?:as\s+)?(?!on\b|where\b|group\b|order\b|limit\b|join\b|left\b|right\b|full\b|inner\b|cross\b|union\b|having\b|using\b)("[^"]+"|[a-zA-Z_][\w$]*))?',
        re.IGNORECASE,
    )

    # SQL data types and keywords that should never be treated as table references
    _SQL_NON_TABLE_NAMES = {
        # Data types
        "bigint", "int", "integer", "smallint", "tinyint", "serial", "bigserial",
        "numeric", "decimal", "real", "float", "double", "precision",
        "boolean", "bool", "bit",
        "char", "character", "varchar", "text", "nchar", "nvarchar", "clob",
        "date", "time", "timestamp", "timestamptz", "interval",
        "bytea", "blob", "binary", "varbinary",
        "json", "jsonb", "xml", "uuid", "inet", "cidr", "macaddr",
        "money", "point", "line", "lseg", "box", "path", "polygon", "circle",
        "array", "enum", "record", "void", "trigger", "regclass", "oid",
        # SQL keywords that could appear after FROM in subexpressions
        "select", "insert", "update", "delete", "values", "set", "default",
        "null", "true", "false", "current_timestamp", "current_date", "current_time",
        "lateral", "unnest", "generate_series", "row_number", "rank",
    }

    cte_names = extract_cte_names(sql)
    relations: list[dict] = []
    alias_map: dict[str, dict] = {}

    for match in relation_pattern.finditer(sql or ""):
        relation_name = split_relation_name(match.group(2), default_schema)
        if not relation_name:
            continue

        schema_name, object_name = relation_name

        # Skip SQL data types and keywords that regex falsely matched as tables
        if object_name.lower() in _SQL_NON_TABLE_NAMES:
            continue
        # Also skip qualified references where the object part is a data type
        # e.g. "src.bigint" from a CAST expression parsed incorrectly
        full_token = match.group(2).strip()
        if "." in full_token:
            parts = full_token.split(".")
            last_part = unquote_sql_identifier(parts[-1]).lower()
            if last_part in _SQL_NON_TABLE_NAMES:
                continue

        if object_name.lower() in cte_names:
            continue

        alias_token = unquote_sql_identifier(match.group(3)) or object_name
        relation = {
            "schema_name": schema_name,
            "object_name": object_name,
            "alias": alias_token,
        }
        relations.append(relation)
        alias_map[alias_token.lower()] = relation
        alias_map[object_name.lower()] = relation

    return relations, alias_map


def object_exists(connection, schema_name: str, object_name: str) -> bool:
    cur = run_query(
        connection,
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
        """,
        [schema_name, object_name],
    )
    row = cur.fetchone()
    cur.close()
    return bool(row)


def resolve_relation_any_schema(connection, schema_name: str | None, object_name: str | None):
    if not object_name:
        return None

    if schema_name:
        obj_type = get_object_type(connection, schema_name, object_name)
        if obj_type:
            return {
                "schema_name": schema_name,
                "object_name": object_name,
                "object_type": obj_type,
            }

    cur = run_query(
        connection,
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_name = %s
        ORDER BY
            CASE WHEN table_schema NOT IN ('information_schema', 'pg_catalog') THEN 0 ELSE 1 END,
            table_schema
        LIMIT 1
        """,
        [object_name],
    )
    row = cur.fetchone()
    cur.close()

    if not row:
        return None

    return {
        "schema_name": row[0],
        "object_name": row[1],
        "object_type": "VIEW" if row[2] == "VIEW" else "TABLE",
    }


def parse_target_relations_from_sql(sql: str, default_schema: str):
    text = (sql or "").strip()
    if not text:
        return []

    patterns = [
        re.compile(
            r'\binsert\s+into\s+((?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\.(?:"[^"]+"|[a-zA-Z_][\w$]*)){0,2})',
            re.IGNORECASE,
        ),
        re.compile(
            r'\bupdate\s+((?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\.(?:"[^"]+"|[a-zA-Z_][\w$]*)){0,2})',
            re.IGNORECASE,
        ),
        re.compile(
            r'\bcreate\s+(?:or\s+replace\s+)?table\s+((?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\.(?:"[^"]+"|[a-zA-Z_][\w$]*)){0,2})',
            re.IGNORECASE,
        ),
        re.compile(
            r'\bmerge\s+into\s+((?:"[^"]+"|[a-zA-Z_][\w$]*)(?:\.(?:"[^"]+"|[a-zA-Z_][\w$]*)){0,2})',
            re.IGNORECASE,
        ),
    ]

    targets = []
    seen = set()

    for pattern in patterns:
        for match in pattern.finditer(text):
            relation_name = split_relation_name(match.group(1), default_schema)
            if not relation_name:
                continue

            schema_name, object_name = relation_name
            key = (schema_name.lower(), object_name.lower())
            if key in seen:
                continue
            seen.add(key)

            targets.append({
                "schema_name": schema_name,
                "object_name": object_name,
            })

    return targets


def same_relation(a_schema: str, a_object: str, b_schema: str, b_object: str) -> bool:
    return (
        (a_schema or "").lower() == (b_schema or "").lower()
        and (a_object or "").lower() == (b_object or "").lower()
    )


def validate_generated_sql_against_metadata(
    connection,
    schema_name: str,
    sql: str,
    known_schema_objects: Dict[str, str],
) -> Dict[str, Any]:
    relations, alias_map = extract_sql_source_relations(sql, schema_name)
    errors: List[str] = []
    validated_objects: List[str] = []
    columns_cache: Dict[tuple[str, str], set[str]] = {}

    for relation in relations:
        rel_schema = relation["schema_name"]
        rel_object = relation["object_name"]
        lookup_key = rel_object.lower()

        if rel_schema == schema_name:
            if lookup_key not in known_schema_objects:
                errors.append(f"Unknown source object '{rel_schema}.{rel_object}' referenced in generated SQL.")
                continue
            rel_type = known_schema_objects[lookup_key]
        else:
            rel_type = get_object_type(connection, rel_schema, rel_object)
            if not rel_type:
                errors.append(f"Unknown source object '{rel_schema}.{rel_object}' referenced in generated SQL.")
                continue

        cache_key = (rel_schema, rel_object)
        if cache_key not in columns_cache:
            columns_cache[cache_key] = {
                col["column_name"].lower()
                for col in get_object_columns(connection, rel_schema, rel_object)
            }

        validated_objects.append(f"{rel_schema}.{rel_object}:{rel_type}")

    qualified_column_pattern = re.compile(
        r'(?<![a-zA-Z0-9_"])((?:"[^"]+"|[a-zA-Z_][\w$]*))\.((?:"[^"]+"|[a-zA-Z_][\w$]*))',
        re.IGNORECASE,
    )

    for match in qualified_column_pattern.finditer(sql or ""):
        alias_name = unquote_sql_identifier(match.group(1)).lower()
        column_name = unquote_sql_identifier(match.group(2)).lower()
        relation = alias_map.get(alias_name)
        if not relation:
            continue

        cache_key = (relation["schema_name"], relation["object_name"])
        valid_columns = columns_cache.get(cache_key)
        if valid_columns is None:
            continue
        if column_name not in valid_columns:
            errors.append(
                f"Unknown column '{match.group(0)}' for source object '{relation['schema_name']}.{relation['object_name']}'."
            )

    return {
        "valid": not errors,
        "errors": list(dict.fromkeys(errors)),
        "validated_objects": list(dict.fromkeys(validated_objects)),
    }


# =========================
# Transformation suggestion helpers
# =========================
def build_null_profile_sql(payload: TransformationSuggestionRequest):
    table_ref = f"{payload.schema_name}.{payload.object_name}" if payload.schema_name else payload.object_name
    pieces = []
    for col in payload.columns[:10]:
        pieces.append(f"SUM(CASE WHEN {col.name} IS NULL THEN 1 ELSE 0 END) AS {col.name}_nulls")
    return f"SELECT\n  " + ",\n  ".join(pieces) + f"\nFROM {table_ref};"


def heuristic_suggestions(payload: TransformationSuggestionRequest):
    suggestions = []
    col_names = [c.name.lower() for c in payload.columns]

    for c in payload.columns:
        cname = c.name.lower()
        dtype = (c.data_type or "").lower()

        if "name" in cname or "city" in cname or "state" in cname:
            suggestions.append({
                "title": f"Trim and normalize {c.name}",
                "category": "standardization",
                "confidence": 0.84,
                "reason": f"{c.name} looks like a text field that may benefit from whitespace cleanup and casing normalization.",
                "sql_expression": f"TRIM({c.name}) AS {c.name}_clean",
                "sql_snippet": f"SELECT TRIM({c.name}) AS {c.name}_clean FROM {payload.schema_name}.{payload.object_name};" if payload.schema_name else f"SELECT TRIM({c.name}) AS {c.name}_clean FROM {payload.object_name};",
                "impact": "Improves text consistency"
            })

        if "date" in cname or "dt" in cname or "time" in cname:
            suggestions.append({
                "title": f"Standardize date/time in {c.name}",
                "category": "type_cast",
                "confidence": 0.82,
                "reason": f"{c.name} appears to be a date/time field and may need explicit casting or parsing.",
                "sql_expression": f"CAST({c.name} AS TIMESTAMP) AS {c.name}_ts",
                "sql_snippet": f"SELECT CAST({c.name} AS TIMESTAMP) AS {c.name}_ts FROM {payload.schema_name}.{payload.object_name};" if payload.schema_name else f"SELECT CAST({c.name} AS TIMESTAMP) AS {c.name}_ts FROM {payload.object_name};",
                "impact": "Improves downstream temporal analytics"
            })

        if "id" in cname and dtype not in ("integer", "bigint", "numeric"):
            suggestions.append({
                "title": f"Cast {c.name} to numeric if needed",
                "category": "type_cast",
                "confidence": 0.76,
                "reason": f"{c.name} looks like an identifier field and may require numeric casting for joins or consistency.",
                "sql_expression": f"CAST({c.name} AS BIGINT) AS {c.name}_num",
                "sql_snippet": f"SELECT CAST({c.name} AS BIGINT) AS {c.name}_num FROM {payload.schema_name}.{payload.object_name};" if payload.schema_name else f"SELECT CAST({c.name} AS BIGINT) AS {c.name}_num FROM {payload.object_name};",
                "impact": "Improves join compatibility"
            })

        if "email" in cname:
            suggestions.append({
                "title": f"Lowercase {c.name}",
                "category": "standardization",
                "confidence": 0.91,
                "reason": f"{c.name} appears to be an email field and should usually be normalized to lowercase.",
                "sql_expression": f"LOWER(TRIM({c.name})) AS {c.name}_normalized",
                "sql_snippet": f"SELECT LOWER(TRIM({c.name})) AS {c.name}_normalized FROM {payload.schema_name}.{payload.object_name};" if payload.schema_name else f"SELECT LOWER(TRIM({c.name})) AS {c.name}_normalized FROM {payload.object_name};",
                "impact": "Improves matching and duplicate checks"
            })

    if "first_name" in col_names and "last_name" in col_names:
        suggestions.append({
            "title": "Create full_name column",
            "category": "derived_column",
            "confidence": 0.93,
            "reason": "Both first_name and last_name exist, so a full_name derived column may be useful.",
            "sql_expression": "CONCAT(first_name, ' ', last_name) AS full_name",
            "sql_snippet": f"SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM {payload.schema_name}.{payload.object_name};" if payload.schema_name else f"SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM {payload.object_name};",
            "impact": "Improves reporting usability"
        })

    suggestions.append({
        "title": "Null check quality scan",
        "category": "data_quality",
        "confidence": 0.88,
        "reason": "Every table should have a quick null-profile check for important columns.",
        "sql_expression": None,
        "sql_snippet": build_null_profile_sql(payload),
        "impact": "Improves data profiling"
    })

    return {
        "summary": f"Generated {len(suggestions)} transformation suggestions using fallback heuristics.",
        "suggestions": suggestions[:12]
    }


def call_llm_for_transformations(payload: TransformationSuggestionRequest):
    system_prompt = """
You are a senior PostgreSQL data engineering copilot.

Return STRICT JSON only in this format:
{
  "summary": "short summary",
  "suggestions": [
    {
      "title": "string",
      "category": "standardization | type_cast | derived_column | filtering | aggregation | join_hint | data_quality | deduplication",
      "confidence": 0.0,
      "reason": "string",
      "sql_expression": "string or null",
      "sql_snippet": "string",
      "impact": "string"
    }
  ]
}

Rules:
- Suggest practical transformations only.
- Ground all suggestions in the provided columns and sample rows.
- Use PostgreSQL syntax.
- Return 5 to 10 suggestions.
- Return JSON only.
"""

    user_prompt = json.dumps({
        "dialect": payload.dialect,
        "database_name": payload.database_name,
        "schema_name": payload.schema_name,
        "object_name": payload.object_name,
        "columns": [c.model_dump() for c in payload.columns],
        "sample_rows": payload.sample_rows[:10],
        "user_prompt": payload.user_prompt or "Suggest useful transformations for analytics, cleaning, joins, and data quality."
    }, indent=2, default=str)

    llm_res = call_llm_json(system_prompt, user_prompt)
    return llm_res.get("content") if llm_res else None


# =========================
# Basic routes
# =========================
@app.get("/")
def root():
    return {"message": "Backend is running"}


@app.post("/api/connections/test")
def test_connection(payload: ConnectionRequest, _u=_dev_or_admin):
    if payload.db_type.lower() != "postgres":
        raise HTTPException(status_code=400, detail="Only PostgreSQL is supported for now")

    try:
        conn = psycopg2.connect(
            host=payload.host,
            port=payload.port,
            dbname=payload.database_name or "postgres",
            user=payload.username,
            password=payload.password,
        )
        conn.close()
        return {"status": "success", "message": "PostgreSQL connection successful"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")


@app.get("/api/connections")
def list_saved_connections(_u=_any_authenticated):
    """Return all saved connections (shared team resource). Passwords are masked."""
    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM connections WHERE is_active = 1 ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    results = []
    for row in rows:
        d = dict(row)
        d.pop("password", None)
        d["is_active"] = bool(d.get("is_active", True))
        results.append(d)
    return results


@app.post("/api/connections/save")
def save_connection(payload: ConnectionRequest, current_user=Depends(require_role("admin", "developer"))):
    global next_connection_id

    connection_id = next_connection_id
    next_connection_id += 1

    conn_data = payload.model_dump()
    saved_connections[connection_id] = conn_data

    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO connections (
            id, name, db_type, host, port, database_name, schema_name,
            username, password, account, warehouse, role, is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        connection_id,
        payload.name,
        payload.db_type,
        payload.host,
        payload.port,
        payload.database_name or "postgres",
        payload.schema_name or "public",
        payload.username,
        payload.password or "",
        payload.account or "",
        payload.warehouse or "",
        payload.role or "",
        1,
    ))
    conn.commit()
    conn.close()

    return {
        "id": connection_id,
        "owner_user_id": current_user.id,
        "name": payload.name,
        "db_type": payload.db_type,
        "host": payload.host,
        "port": payload.port,
        "database_name": payload.database_name or "postgres",
        "schema_name": payload.schema_name or "public",
        "username": payload.username,
        "account": payload.account,
        "warehouse": payload.warehouse,
        "role": payload.role,
        "is_active": True,
    }

@app.get("/api/schema-explorer/databases")
def get_databases(connection_id: int, _u=_any_authenticated):
    conn = get_db_connection_by_id(connection_id, "postgres")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
        """)
        rows = cur.fetchall()
        cur.close()
        return {"databases": [row[0] for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch databases: {str(e)}")
    finally:
        conn.close()


@app.get("/api/schema-explorer/schemas")
def get_schemas(connection_id: int, database_name: str, _u=_any_authenticated):
    conn = get_db_connection_by_id(connection_id, database_name)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_toast')
            ORDER BY schema_name
        """)
        rows = cur.fetchall()
        cur.close()
        return {"schemas": [row[0] for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schemas: {str(e)}")
    finally:
        conn.close()


@app.get("/api/schema-explorer/objects")
def get_objects(connection_id: int, database_name: str, schema: str, _u=_any_authenticated):
    conn = get_db_connection_by_id(connection_id, database_name)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema,))
        rows = cur.fetchall()
        cur.close()

        return {
            "objects": [
                {
                    "name": row[0],
                    "type": "VIEW" if row[1] == "VIEW" else "TABLE"
                }
                for row in rows
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch objects: {str(e)}")
    finally:
        conn.close()


@app.get("/api/schema-explorer/object-details")
def get_object_details(connection_id: int, database_name: str, schema: str, object: str, _u=_any_authenticated):
    conn = get_db_connection_by_id(connection_id, database_name)
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (schema, object))
        object_row = cur.fetchone()

        if not object_row:
            raise HTTPException(status_code=404, detail="Object not found")

        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, object))
        columns = cur.fetchall()
        cur.close()

        return {
            "object_name": object_row[0],
            "object_type": "VIEW" if object_row[1] == "VIEW" else "TABLE",
            "columns": [
                {
                    "column_name": col[0],
                    "data_type": col[1],
                    "is_nullable": col[2],
                    "column_default": col[3],
                }
                for col in columns
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch object details: {str(e)}")
    finally:
        conn.close()


@app.get("/api/schema-explorer/sample-data")
def get_sample_data(
    connection_id: int,
    database_name: str,
    schema: str,
    object: str,
    limit: int = 25,
    _u=_any_authenticated,
):
    if limit not in [25, 50, 100]:
        raise HTTPException(status_code=400, detail="Limit must be 25, 50, or 100")

    conn = get_db_connection_by_id(connection_id, database_name)
    try:
        q_schema = quote_ident(schema)
        q_object = quote_ident(object)

        sql = f'SELECT * FROM {q_schema}.{q_object} LIMIT %s'
        cur = conn.cursor()
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()

        return {
            "columns": columns,
            "rows": rows,
            "limit": limit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch sample data: {str(e)}")
    finally:
        conn.close()


@app.post("/api/query/execute")
def execute_query(
    connection_id: int,
    payload: QueryRequest,
    database_name: str = Query(...),
    _u=_any_authenticated,
):
    query = payload.query.strip().rstrip(";")
    limit = payload.limit
    offset = payload.offset

    if not query.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

    if limit not in [25, 50, 100]:
        raise HTTPException(status_code=400, detail="Limit must be 25, 50, or 100")

    if offset < 0:
        raise HTTPException(status_code=400, detail="Offset cannot be negative")

    conn = get_db_connection_by_id(connection_id, database_name)
    try:
        cur = conn.cursor()

        count_query = f"""
            SELECT COUNT(*) FROM ({query}) AS subquery_count
        """
        cur.execute(count_query)
        total_count = cur.fetchone()[0]

        data_query = f"""
            SELECT * FROM ({query}) AS subquery_data
            LIMIT %s OFFSET %s
        """
        cur.execute(data_query, (limit, offset))

        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()

        return {
            "columns": columns,
            "rows": rows,
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/api/connections/{conn_id}/relationships")
def get_relationships(
    conn_id: int,
    schema: str = Query(...),
    object: str = Query(...),
    database_name: str = Query(...),
    _u=_any_authenticated,
):
    conn = get_db_connection_by_id(conn_id, database_name)

    try:
        return build_relationship_summary(conn, schema, object)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch relationships: {str(e)}")
    finally:
        conn.close()


@app.get("/api/connections/{conn_id}/definition")
def get_definition(
    conn_id: int,
    schema: str = Query(...),
    object: str = Query(...),
    database_name: str = Query(...),
    _u=_any_authenticated,
):
    conn = get_db_connection_by_id(conn_id, database_name)

    try:
        definition = get_view_definition(conn, schema, object)

        return {
            "object_name": object,
            "definition": definition or "Not a view or no definition found"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch definition: {str(e)}")
    finally:
        conn.close()


@app.get("/api/lineage")
def get_lineage(
    connection_id: int = Query(...),
    database_name: str = Query(...),
    schema: str = Query(...),
    object: str = Query(...),
    _u=_any_authenticated,
):
    conn = get_db_connection_by_id(connection_id, database_name)

    try:
        resolved_current = resolve_relation_any_schema(conn, schema, object)
        if not resolved_current:
            raise HTTPException(status_code=404, detail="Object not found")

        current_schema = resolved_current["schema_name"]
        current_object = resolved_current["object_name"]
        current_type = resolved_current["object_type"]

        upstream = []
        downstream = []

        def add_upstream(schema_name: str, object_name: str, object_type: str, relation_kind: str):
            if same_relation(schema_name, object_name, current_schema, current_object):
                return
            upstream.append({
                "schema_name": schema_name,
                "object_name": object_name,
                "object_type": object_type or "OBJECT",
                "relation_kind": relation_kind,
            })

        def add_downstream(schema_name: str, object_name: str, object_type: str, relation_kind: str):
            if same_relation(schema_name, object_name, current_schema, current_object):
                return
            downstream.append({
                "schema_name": schema_name,
                "object_name": object_name,
                "object_type": object_type or "OBJECT",
                "relation_kind": relation_kind,
            })

        # -------------------------------------------------
        # 1) View-based lineage
        # -------------------------------------------------
        if current_type == "VIEW":
            cur = run_query(
                conn,
                """
                SELECT DISTINCT
                    vtu.table_schema AS schema_name,
                    vtu.table_name AS object_name
                FROM information_schema.view_table_usage vtu
                WHERE vtu.view_schema = %s
                  AND vtu.view_name = %s
                """,
                [current_schema, current_object],
            )
            rows = cur.fetchall()
            cur.close()

            for row in rows:
                rel = resolve_relation_any_schema(conn, row[0], row[1])
                if rel:
                    add_upstream(
                        rel["schema_name"],
                        rel["object_name"],
                        rel["object_type"],
                        "VIEW_SOURCE",
                    )

        if current_type == "TABLE":
            cur = run_query(
                conn,
                """
                SELECT DISTINCT
                    vtu.view_schema AS schema_name,
                    vtu.view_name AS object_name
                FROM information_schema.view_table_usage vtu
                WHERE vtu.table_schema = %s
                  AND vtu.table_name = %s
                """,
                [current_schema, current_object],
            )
            rows = cur.fetchall()
            cur.close()

            for row in rows:
                rel = resolve_relation_any_schema(conn, row[0], row[1])
                if rel:
                    add_downstream(
                        rel["schema_name"],
                        rel["object_name"],
                        rel["object_type"],
                        "VIEW_TARGET",
                    )

        # -------------------------------------------------
        # 2) FK lineage
        # -------------------------------------------------
        cur = run_query(
            conn,
            """
            SELECT
                kcu.column_name,
                ccu.table_schema,
                ccu.table_name,
                ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            """,
            [current_schema, current_object],
        )
        fk_rows = cur.fetchall()
        cur.close()

        for row in fk_rows:
            rel = resolve_relation_any_schema(conn, row[1], row[2])
            if rel:
                add_upstream(
                    rel["schema_name"],
                    rel["object_name"],
                    rel["object_type"],
                    f"FK:{row[0]}->{row[3]}",
                )

        cur = run_query(
            conn,
            """
            SELECT DISTINCT
                tc.table_schema AS schema_name,
                tc.table_name AS object_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_schema = %s
              AND ccu.table_name = %s
            """,
            [current_schema, current_object],
        )
        ref_rows = cur.fetchall()
        cur.close()

        for row in ref_rows:
            rel = resolve_relation_any_schema(conn, row[0], row[1])
            if rel:
                add_downstream(
                    rel["schema_name"],
                    rel["object_name"],
                    rel["object_type"],
                    "FK_TARGET",
                )

        # -------------------------------------------------
        # 3) Pipeline lineage from pipeline headers + step SQL
        # -------------------------------------------------
        app_store_conn = get_app_store_conn()
        app_store_cur = app_store_conn.cursor()

        pipeline_rows = []
        step_rows = []

        try:
            app_store_cur.execute("""
                SELECT
                    p.id,
                    p.name,
                    p.schema_name,
                    p.source_object,
                    p.target_object,
                    MAX(
                        CASE
                            WHEN LOWER(COALESCE(pr.status, '')) IN ('success', 'partial_success') THEN 1
                            ELSE 0
                        END
                    ) AS has_successful_run
                FROM pipelines p
                LEFT JOIN pipeline_runs pr
                  ON pr.pipeline_id = p.id
                                WHERE LOWER(COALESCE(p.database_name, '')) = LOWER(?)
                GROUP BY p.id, p.name, p.schema_name, p.source_object, p.target_object
                        """, (database_name,))
            pipeline_rows = app_store_cur.fetchall()

            app_store_cur.execute("""
                SELECT
                    ps.pipeline_id,
                    ps.step_name,
                    ps.sql_text,
                    p.name
                FROM pipeline_steps ps
                JOIN pipelines p
                  ON p.id = ps.pipeline_id
                WHERE LOWER(COALESCE(p.database_name, '')) = LOWER(?)
                  AND COALESCE(ps.is_active, 1) = 1
                ORDER BY ps.pipeline_id, ps.step_order, ps.id
            """, (database_name,))
            step_rows = app_store_cur.fetchall()
        except sqlite3.OperationalError:
            pipeline_rows = []
            step_rows = []
        finally:
            app_store_conn.close()

        def resolve_pipeline_relation(token: str, pipeline_schema: str):
            normalized = (token or "").strip()
            if not normalized:
                return None

            default_schema = pipeline_schema or current_schema
            relation_name = split_relation_name(normalized, default_schema)
            if relation_name:
                rel = resolve_relation_any_schema(conn, relation_name[0], relation_name[1])
                if rel:
                    return rel

            return resolve_relation_any_schema(conn, pipeline_schema or None, normalized)

        # Header-based fallback
        for row in pipeline_rows:
            pipeline_id = row[0]
            pipeline_name = row[1] or f"Pipeline {pipeline_id}"
            pipeline_schema = (row[2] or "").strip()
            source_object = (row[3] or "").strip()
            target_object = (row[4] or "").strip()
            has_successful_run = int(row[5] or 0) == 1

            relation_kind = (
                f"PIPELINE_RUN:{pipeline_name}"
                if has_successful_run
                else f"PIPELINE_DEFINED:{pipeline_name}"
            )

            source_rel = resolve_pipeline_relation(source_object, pipeline_schema)
            target_rel = resolve_pipeline_relation(target_object, pipeline_schema)

            if source_rel and target_rel and same_relation(
                source_rel["schema_name"],
                source_rel["object_name"],
                current_schema,
                current_object,
            ):
                add_downstream(
                    target_rel["schema_name"],
                    target_rel["object_name"],
                    target_rel["object_type"],
                    relation_kind,
                )

            if source_rel and target_rel and same_relation(
                target_rel["schema_name"],
                target_rel["object_name"],
                current_schema,
                current_object,
            ):
                add_upstream(
                    source_rel["schema_name"],
                    source_rel["object_name"],
                    source_rel["object_type"],
                    relation_kind,
                )

        # Step-SQL-based lineage (important fix)
        for row in step_rows:
            pipeline_id = row[0]
            step_name = row[1] or ""
            sql_text = row[2] or ""
            pipeline_name = row[3] or f"Pipeline {pipeline_id}"

            relation_kind = f"PIPELINE_STEP:{pipeline_name}:{step_name}"

            sources, _ = extract_sql_source_relations(sql_text, current_schema)
            targets = parse_target_relations_from_sql(sql_text, current_schema)

            resolved_sources = []
            for src in sources:
                rel = resolve_relation_any_schema(
                    conn,
                    src.get("schema_name"),
                    src.get("object_name"),
                )
                if rel:
                    resolved_sources.append(rel)

            resolved_targets = []
            for tgt in targets:
                rel = resolve_relation_any_schema(
                    conn,
                    tgt.get("schema_name"),
                    tgt.get("object_name"),
                )
                if rel:
                    resolved_targets.append(rel)

            current_is_source = any(
                same_relation(
                    rel["schema_name"], rel["object_name"],
                    current_schema, current_object
                )
                for rel in resolved_sources
            )
            current_is_target = any(
                same_relation(
                    rel["schema_name"], rel["object_name"],
                    current_schema, current_object
                )
                for rel in resolved_targets
            )

            if current_is_source:
                for rel in resolved_targets:
                    add_downstream(
                        rel["schema_name"],
                        rel["object_name"],
                        rel["object_type"],
                        relation_kind,
                    )

            if current_is_target:
                for rel in resolved_sources:
                    add_upstream(
                        rel["schema_name"],
                        rel["object_name"],
                        rel["object_type"],
                        relation_kind,
                    )

        def dedupe(items):
            seen = set()
            result = []
            for item in items:
                key = (
                    (item["schema_name"] or "").lower(),
                    (item["object_name"] or "").lower(),
                    (item["object_type"] or "").upper(),
                    item["relation_kind"],
                )
                if key not in seen:
                    seen.add(key)
                    result.append(item)
            return result

        return {
            "object_name": current_object,
            "schema_name": current_schema,
            "database_name": database_name,
            "object_type": current_type,
            "upstream": dedupe(upstream),
            "downstream": dedupe(downstream),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch lineage: {str(e)}")
    finally:
        conn.close()
        
@app.post("/api/ai/join-suggestions")
def ai_join_suggestions(payload: JoinSuggestionRequest, _u=_dev_or_admin):
    conn = get_db_connection_by_id(payload.connection_id, payload.database_name)

    try:
        object_type = get_object_type(conn, payload.schema_name, payload.object_name)
        if not object_type:
            raise HTTPException(status_code=404, detail="Object not found")

        base_columns = get_object_columns(conn, payload.schema_name, payload.object_name)
        relationships = build_relationship_summary(conn, payload.schema_name, payload.object_name)
        schema_objects = get_schema_objects(conn, payload.schema_name)

        candidate_tables = []
        for obj in schema_objects:
            if obj["name"] == payload.object_name:
                continue

            try:
                cols = get_object_columns(conn, payload.schema_name, obj["name"])
                candidate_tables.append({
                    "name": obj["name"],
                    "type": obj["type"],
                    "columns": [c["column_name"] for c in cols[:50]],
                })
            except Exception:
                continue

        heuristic_suggestions = relationships.get("suggested_relationships", [])

        system_prompt = """
You are a senior data engineer.
You analyze schema metadata and propose likely SQL joins.

Return STRICT JSON only with this shape:
{
  "summary": "short summary",
  "recommended_joins": [
    {
      "join_type": "LEFT JOIN",
      "left_table": "base_table",
      "left_column": "column_name",
      "right_table": "other_table",
      "right_column": "column_name",
      "confidence": 0.0,
      "reason": "why this join is likely"
    }
  ],
  "notes": ["note1", "note2"]
}

Rules:
- Prefer exact PK/FK evidence first.
- Then use naming similarity and ID-like patterns.
- Be conservative.
- Do not invent columns not present in metadata.
- Return JSON only.
"""

        user_prompt = json.dumps({
            "schema": payload.schema_name,
            "base_object": payload.object_name,
            "base_object_type": object_type,
            "base_columns": [c["column_name"] for c in base_columns],
            "verified_relationships": {
                "primary_keys": relationships.get("primary_keys", []),
                "foreign_keys": relationships.get("foreign_keys", []),
                "referenced_by": relationships.get("referenced_by", []),
                "heuristic_relationships": heuristic_suggestions,
            },
            "candidate_tables": candidate_tables[:20],
        }, indent=2)

        llm_res = call_llm_json(system_prompt, user_prompt)
        llm_content = llm_res.get("content") if llm_res else None

        return {
            "object_name": payload.object_name,
            "schema": payload.schema_name,
            "llm_enabled": llm_res.get("enabled", False),
            "llm_reason": llm_res.get("reason", ""),
            "heuristic_relationships": heuristic_suggestions,
            "ai_summary": (llm_content or {}).get("summary", ""),
            "recommended_joins": (llm_content or {}).get("recommended_joins", []),
            "notes": (llm_content or {}).get("notes", []),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate join suggestions: {str(e)}")
    finally:
        conn.close()


@app.post("/api/ai/transformation-suggestions")
def get_ai_transformation_suggestions(payload: TransformationSuggestionRequest, _u=_dev_or_admin):
    try:
        try:
            result = call_llm_for_transformations(payload)
            if not isinstance(result, dict) or "suggestions" not in result:
                raise ValueError("Invalid LLM response format")
            return result
        except Exception:
            return heuristic_suggestions(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate transformation suggestions: {str(e)}")


@app.post("/api/fix-sql")
def fix_sql(request: FixSqlRequest, _u=_dev_or_admin):
    conn = None
    try:
        default_schema = (request.schema_name or "public").strip() or "public"
        conn = get_db_connection_by_id(request.connection_id, request.database_name)
        agent_log: List[Dict[str, Any]] = []

        def _table_exists(schema_name: str, object_name: str, all_map: Dict[tuple[str, str], str]) -> bool:
            return (schema_name.lower(), object_name.lower()) in all_map

        def _name_variants(name: str) -> set[str]:
            value = (name or "").lower()
            variants = {value}
            if value.endswith("ies"):
                variants.add(value[:-3] + "y")
            if value.endswith("s"):
                variants.add(value[:-1])
            else:
                variants.add(value + "s")
            if value.endswith("es"):
                variants.add(value[:-2])
            else:
                variants.add(value + "es")
            return {v for v in variants if v}

        def _best_object_match(
            source_schema: str,
            source_object: str,
            all_objects: List[Dict[str, str]],
        ) -> Optional[Dict[str, Any]]:
            source_schema_l = (source_schema or "").lower()
            source_object_l = (source_object or "").lower()
            source_variants = _name_variants(source_object_l)

            best: Optional[Dict[str, Any]] = None
            for obj in all_objects:
                candidate_schema = obj["schema"]
                candidate_name = obj["name"]
                candidate_l = candidate_name.lower()

                score = SequenceMatcher(None, source_object_l, candidate_l).ratio()
                reason = "fuzzy-name-similarity"

                if candidate_l in source_variants or source_object_l in _name_variants(candidate_l):
                    score = max(score, 0.98)
                    reason = "singular/plural-match"
                elif candidate_l.startswith(source_object_l) or source_object_l.startswith(candidate_l):
                    score = max(score, 0.9)
                    reason = "prefix-match"

                if candidate_schema.lower() == source_schema_l:
                    score += 0.03

                if best is None or score > best["score"]:
                    best = {
                        "schema": candidate_schema,
                        "name": candidate_name,
                        "type": obj["type"],
                        "score": round(score, 4),
                        "reason": reason,
                    }

            if best and best["score"] >= 0.78:
                return best
            return None

        def _format_cols_block(schema_name: str, object_name: str, columns: List[Dict[str, Any]]) -> str:
            lines = [f'  Table: "{schema_name}"."{object_name}"']
            for c in columns:
                nullable = "NULL" if c.get("is_nullable") == "YES" else "NOT NULL"
                lines.append(
                    f'    - {c["column_name"]} ({c.get("data_type", "unknown")}, {nullable})'
                )
            return "\n".join(lines)

        def _explain_check_sql(connection, sql_text: str) -> Dict[str, Any]:
            statements = [s.strip() for s in (sql_text or "").split(";") if s.strip()]
            if not statements:
                return {"ok": False, "checked": 0, "error": "No SQL statement found in AI output."}

            explainable_prefixes = {"select", "with", "insert", "update", "delete", "merge"}
            checked = 0
            try:
                # Clear any previous failed transaction state before running EXPLAIN.
                connection.rollback()
            except Exception:
                pass
            cur = connection.cursor()
            try:
                for stmt in statements:
                    head = (stmt.split(None, 1)[0].lower() if stmt.split(None, 1) else "")
                    if head in explainable_prefixes:
                        cur.execute(f"EXPLAIN {stmt}")
                        cur.fetchall()
                        checked += 1
                return {"ok": True, "checked": checked, "error": ""}
            except Exception as ex:
                try:
                    connection.rollback()
                except Exception:
                    pass
                return {"ok": False, "checked": checked, "error": str(ex)[:500]}
            finally:
                cur.close()

        try:
            schema_objects = get_schema_objects(conn, default_schema)
        except Exception:
            schema_objects = []
        known_schema_objects = {obj["name"].lower(): obj["type"] for obj in schema_objects}

        all_db_objects = get_all_schema_objects(conn)
        all_objects_map = {
            (obj["schema"].lower(), obj["name"].lower()): obj["type"]
            for obj in all_db_objects
        }

        source_relations, _ = extract_sql_source_relations(request.sql, default_schema)
        target_relations = parse_target_relations_from_sql(request.sql, default_schema)

        touched_relations = []
        seen_relation_keys = set()
        for rel in source_relations:
            key = (rel["schema_name"].lower(), rel["object_name"].lower())
            if key not in seen_relation_keys:
                touched_relations.append({
                    "schema_name": rel["schema_name"],
                    "object_name": rel["object_name"],
                    "role": "source",
                })
                seen_relation_keys.add(key)
        for rel in target_relations:
            key = (rel["schema_name"].lower(), rel["object_name"].lower())
            if key not in seen_relation_keys:
                touched_relations.append({
                    "schema_name": rel["schema_name"],
                    "object_name": rel["object_name"],
                    "role": "target",
                })
                seen_relation_keys.add(key)

        relation_repairs = []
        for rel in touched_relations:
            rel_schema = rel["schema_name"]
            rel_object = rel["object_name"]
            if _table_exists(rel_schema, rel_object, all_objects_map):
                relation_repairs.append({
                    "original": f"{rel_schema}.{rel_object}",
                    "resolved": f"{rel_schema}.{rel_object}",
                    "changed": False,
                    "reason": "exact-match",
                    "role": rel["role"],
                })
                continue

            match = _best_object_match(rel_schema, rel_object, all_db_objects)
            if match:
                relation_repairs.append({
                    "original": f"{rel_schema}.{rel_object}",
                    "resolved": f'{match["schema"]}.{match["name"]}',
                    "changed": True,
                    "reason": f'{match["reason"]} (score={match["score"]})',
                    "role": rel["role"],
                })
            else:
                relation_repairs.append({
                    "original": f"{rel_schema}.{rel_object}",
                    "resolved": f"{rel_schema}.{rel_object}",
                    "changed": False,
                    "reason": "no-confident-match-found",
                    "role": rel["role"],
                })

        agent_log.append({
            "phase": "METADATA_DISCOVERY",
            "status": "ok",
            "details": (
                f"Parsed {len(touched_relations)} relation(s), "
                f"catalogued {len(all_db_objects)} objects across schemas."
            ),
        })

        metadata_blocks = []
        metadata_tables = []
        metadata_seen = set()

        for rr in relation_repairs:
            resolved = rr["resolved"]
            parts = resolved.split(".", 1)
            if len(parts) != 2:
                continue
            rel_schema, rel_object = parts[0], parts[1]
            cache_key = (rel_schema.lower(), rel_object.lower())
            if cache_key in metadata_seen:
                continue
            try:
                cols = get_object_columns(conn, rel_schema, rel_object)
                if cols:
                    metadata_blocks.append(_format_cols_block(rel_schema, rel_object, cols))
                    metadata_tables.append(f"{rel_schema}.{rel_object}")
                    metadata_seen.add(cache_key)
            except Exception:
                continue

        if request.object_name and request.object_name.lower() in known_schema_objects:
            cache_key = (default_schema.lower(), request.object_name.lower())
            if cache_key not in metadata_seen:
                try:
                    cols = get_object_columns(conn, default_schema, request.object_name)
                    if cols:
                        metadata_blocks.append(_format_cols_block(default_schema, request.object_name, cols))
                        metadata_tables.append(f"{default_schema}.{request.object_name}")
                        metadata_seen.add(cache_key)
                except Exception:
                    pass

        metadata_section = "\n\n".join(metadata_blocks) if metadata_blocks else "(No table metadata could be loaded from parsed relations.)"
        relation_resolution_text = "\n".join(
            f'  - {rr["role"]}: {rr["original"]} -> {rr["resolved"]} ({rr["reason"]})'
            for rr in relation_repairs
        ) or "  - No relations parsed"

        system_prompt = f"""You are a senior PostgreSQL SQL debugging assistant running in an agentic workflow.

You must fix SQL with this process:
1) Respect relation resolution hints.
2) Use only real columns from provided metadata.
3) Produce executable SQL that can be copied and run with zero edits.

=== RELATION RESOLUTION HINTS (from metadata discovery agent) ===
{relation_resolution_text}
=== END RELATION RESOLUTION ===

=== REAL TABLE METADATA (EXACT COLUMNS + TYPES) ===
{metadata_section}
=== END METADATA ===

Return STRICT JSON only with this shape:
{{
  "sql": "fixed SQL",
  "explanation": "what was fixed",
  "assumptions": ["assumption1"],
  "warnings": ["warning1"]
}}

Rules:
- Keep PostgreSQL syntax.
- Prefer minimal changes.
- If the error references a missing relation, correct it to an existing resolved relation from hints.
- Never invent columns not listed in metadata.
- If a type mismatch is possible, add explicit CAST() based on real metadata types.
- Output JSON only.
"""

        user_prompt = json.dumps({
            "database_name": request.database_name,
            "schema": default_schema,
            "object_name": request.object_name,
            "original_sql": request.sql,
            "error": request.error,
            "relation_repairs": relation_repairs,
        }, indent=2)

        llm_res = call_llm_json(system_prompt, user_prompt)
        llm_content = llm_res.get("content") if llm_res else None

        if not llm_content:
            return {
                "sql": request.sql,
                "explanation": "AI fix was unavailable, so the original SQL was returned unchanged.",
                "assumptions": ["No AI output was available."],
                "warnings": [llm_res.get("reason", "LLM unavailable") if llm_res else "LLM unavailable"],
                "llm_enabled": llm_res.get("enabled", False) if llm_res else False,
                "llm_reason": llm_res.get("reason", "") if llm_res else "",
                "agent_log": agent_log,
                "metadata_tables": metadata_tables,
            }

        candidate_sql = (llm_content.get("sql") or request.sql or "").strip()
        explanation = llm_content.get("explanation", "")
        assumptions = list(llm_content.get("assumptions", []))
        warnings = list(llm_content.get("warnings", []))

        final_validation = {"valid": True, "errors": [], "validated_objects": []}
        explain_status = {"ok": True, "checked": 0, "error": ""}
        max_attempts = 3
        used_attempts = 0

        for attempt in range(max_attempts):
            used_attempts = attempt + 1
            final_validation = validate_generated_sql_against_metadata(
                conn,
                default_schema,
                candidate_sql,
                known_schema_objects,
            )
            explain_status = _explain_check_sql(conn, candidate_sql)

            agent_log.append({
                "phase": "VALIDATION",
                "status": "ok" if final_validation["valid"] and explain_status["ok"] else "partial",
                "details": (
                    f"attempt={attempt + 1}, metadata_valid={final_validation['valid']}, "
                    f"explain_ok={explain_status['ok']}, explain_checked={explain_status['checked']}"
                ),
            })

            if final_validation["valid"] and explain_status["ok"]:
                break

            if attempt == max_attempts - 1:
                break

            retry_payload = json.dumps({
                "retry": True,
                "attempt": attempt + 2,
                "previous_sql": candidate_sql,
                "metadata_validation_errors": final_validation["errors"],
                "explain_error": explain_status["error"],
                "instruction": (
                    "Fix the SQL so all relations/columns exist in metadata and EXPLAIN succeeds. "
                    "Use relation resolution hints and exact metadata names only."
                ),
            }, indent=2)
            retry_res = call_llm_json(system_prompt, retry_payload)
            retry_content = retry_res.get("content") if retry_res else None
            if retry_content and retry_content.get("sql"):
                candidate_sql = retry_content["sql"].strip()
                explanation = retry_content.get("explanation", explanation)
                assumptions = list(retry_content.get("assumptions", assumptions))
                warnings = list(retry_content.get("warnings", warnings))

        if not final_validation["valid"]:
            warnings.extend(final_validation["errors"])
            warnings.append("SQL still has metadata validation issues after agent retries.")

        if not explain_status["ok"]:
            warnings.append(f"EXPLAIN check failed: {explain_status['error']}")
        elif explain_status["checked"] == 0:
            assumptions.append("EXPLAIN runtime check was skipped because statement type is not EXPLAIN-able.")
        else:
            assumptions.append(f"Verified by EXPLAIN on {explain_status['checked']} statement(s).")

        return {
            "sql": candidate_sql,
            "explanation": explanation,
            "assumptions": assumptions,
            "warnings": list(dict.fromkeys(warnings)),
            "llm_enabled": llm_res.get("enabled", False),
            "llm_reason": llm_res.get("reason", ""),
            "agent_log": agent_log,
            "relation_repairs": relation_repairs,
            "metadata_tables": metadata_tables,
            "attempts": used_attempts,
            "validated": final_validation["valid"] and explain_status["ok"],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fix SQL: {str(e)}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


class ResolveObjectsRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_name: str = Field(alias="schema")
    user_prompt: str


@app.post("/api/ai/resolve-objects")
def resolve_objects(payload: ResolveObjectsRequest, _u=_any_authenticated):
    """Parse a user prompt for table/view references and return matched objects with their columns."""
    conn = get_db_connection_by_id(payload.connection_id, payload.database_name)
    try:
        schema_objects = get_schema_objects(conn, payload.schema_name)
        all_db_objects = get_all_schema_objects(conn)

        prompt_lower = (payload.user_prompt or "").lower()
        matched = []

        # Match objects in the current schema
        for obj in sorted(schema_objects, key=lambda o: len(o["name"]), reverse=True):
            pattern = rf'(?<![a-z0-9_]){re.escape(obj["name"].lower())}(?![a-z0-9_])'
            if re.search(pattern, prompt_lower):
                try:
                    cols = get_object_columns(conn, payload.schema_name, obj["name"])
                except Exception:
                    cols = []
                matched.append({
                    "schema": payload.schema_name,
                    "name": obj["name"],
                    "type": obj["type"],
                    "columns": [
                        {"column_name": c["column_name"], "data_type": c.get("data_type", "unknown"),
                         "is_nullable": c.get("is_nullable", "YES")}
                        for c in cols
                    ],
                })

        # Match cross-schema objects
        for obj in all_db_objects:
            if obj["schema"].lower() == payload.schema_name.lower():
                continue
            pattern = rf'(?<![a-z0-9_]){re.escape(obj["name"].lower())}(?![a-z0-9_])'
            if re.search(pattern, prompt_lower):
                try:
                    cols = get_object_columns(conn, obj["schema"], obj["name"])
                except Exception:
                    cols = []
                matched.append({
                    "schema": obj["schema"],
                    "name": obj["name"],
                    "type": obj["type"],
                    "columns": [
                        {"column_name": c["column_name"], "data_type": c.get("data_type", "unknown"),
                         "is_nullable": c.get("is_nullable", "YES")}
                        for c in cols
                    ],
                })

        # If nothing matched, return all objects in schema (so user can pick)
        if not matched:
            for obj in schema_objects[:50]:
                matched.append({
                    "schema": payload.schema_name,
                    "name": obj["name"],
                    "type": obj["type"],
                    "columns": [],  # lightweight — no columns for unmatched
                })

        return {"matched_objects": matched, "prompt": payload.user_prompt}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resolve objects: {str(e)}")
    finally:
        conn.close()


@app.post("/api/insights/agentic-rag")
def insights_agentic_rag(payload: InsightsAgenticRAGRequest, _u=_dev_or_admin):
    """Unified agentic-RAG flow for insights tab: retrieval, disambiguation, SQL generation, and evidence."""
    prompt_text = (payload.user_prompt or "").strip()
    if not prompt_text:
        raise HTTPException(status_code=400, detail="Prompt cannot be empty")

    active_connection = saved_connections.get(payload.connection_id) or {}
    effective_database = (
        (payload.database_name or "").strip()
        or (active_connection.get("database_name") or "").strip()
        or None
    )
    default_schema = (
        (payload.schema_name or "").strip()
        or (active_connection.get("schema_name") or "").strip()
        or "public"
    )
    conn = get_db_connection_by_id(payload.connection_id, effective_database)
    try:
        resolved_database_name = (
            effective_database
            or (active_connection.get("database_name") or "").strip()
            or "postgres"
        )

        all_db_objects = get_all_schema_objects(conn)

        object_lookup: Dict[tuple[str, str], Dict[str, str]] = {
            (obj["schema"].lower(), obj["name"].lower()): obj for obj in all_db_objects
        }

        prompt_matches = match_prompt_objects(prompt_text, all_db_objects)

        has_explicit_context = bool((payload.object_name or "").strip()) or bool(payload.selected_objects)
        if not has_explicit_context and not is_data_analytics_prompt(prompt_text, prompt_matches):
            return {
                "mode": "out_of_scope",
                "assistant_response": build_out_of_scope_insights_reply(),
            }

        selected_from_request: List[Dict[str, str]] = []
        for ref in payload.selected_objects:
            schema_name, object_name = parse_object_reference(ref, default_schema)
            key = (schema_name.lower(), object_name.lower())
            if key in object_lookup:
                selected_from_request.append(object_lookup[key])

        resolved_schema = default_schema
        resolved_object = (payload.object_name or "").strip()

        if not resolved_object and selected_from_request:
            resolved_schema = selected_from_request[0]["schema"]
            resolved_object = selected_from_request[0]["name"]

        if not resolved_object and prompt_matches:
            resolved_schema = prompt_matches[0]["schema"]
            resolved_object = prompt_matches[0]["name"]

        # If prompt references multiple objects and caller has not selected any object yet,
        # request an explicit disambiguation step in the UI.
        if not payload.selected_objects and len(prompt_matches) > 1 and not payload.object_name:
            candidates = [
                {
                    "schema": item["schema"],
                    "name": item["name"],
                    "type": item["type"],
                }
                for item in prompt_matches[:12]
            ]
            return {
                "mode": "needs_disambiguation",
                "message": "Multiple objects were detected in your question. Please select the relevant objects.",
                "disambiguation": {
                    "candidates": candidates,
                    "prompt": prompt_text,
                },
            }

        if not resolved_object:
            ranked_all = match_prompt_objects(prompt_text, all_db_objects)

            if ranked_all:
                fallback_candidates = [
                    {
                        "schema": item["schema"],
                        "name": item["name"],
                        "type": item["type"],
                    }
                    for item in ranked_all[:12]
                ]
            else:
                public_like = {"public", "information_schema", "pg_catalog"}
                sorted_objects = sorted(
                    all_db_objects,
                    key=lambda item: (
                        item["schema"].lower() in public_like,
                        item["schema"].lower(),
                        item["name"].lower(),
                    ),
                )
                fallback_candidates = [
                    {
                        "schema": item["schema"],
                        "name": item["name"],
                        "type": item["type"],
                    }
                    for item in sorted_objects[:12]
                ]

            return {
                "mode": "needs_disambiguation",
                "message": "I could not infer a specific object yet. Select relevant objects, or mention table/view names in your question.",
                "disambiguation": {
                    "candidates": fallback_candidates,
                    "prompt": prompt_text,
                },
            }

        resolved_type = get_object_type(conn, resolved_schema, resolved_object)
        if not resolved_type:
            raise HTTPException(status_code=404, detail="Target object not found")

        selected_context_objects = selected_from_request.copy()
        if not selected_context_objects and prompt_matches:
            selected_context_objects = prompt_matches[:4]

        # Build retrieval context from metadata + relationships (RAG context payload).
        retrieval_objects = []
        seen_retrieval_keys: set[tuple[str, str]] = set()

        base_key = (resolved_schema.lower(), resolved_object.lower())
        if base_key in object_lookup:
            selected_context_objects.insert(0, object_lookup[base_key])

        for obj in selected_context_objects:
            key = (obj["schema"].lower(), obj["name"].lower())
            if key in seen_retrieval_keys:
                continue
            seen_retrieval_keys.add(key)

            metadata = get_object_metadata(conn, obj["schema"], obj["name"], obj.get("type"))
            if metadata:
                retrieval_objects.append(
                    {
                        "schema": metadata["schema_name"],
                        "name": metadata["object_name"],
                        "type": metadata["object_type"],
                        "columns": [col["column_name"] for col in metadata["columns"][:30]],
                    }
                )

        relationship_summary = build_relationship_summary(conn, resolved_schema, resolved_object)

        history_lines: List[str] = []
        for turn in (payload.conversation_history or [])[-8:]:
            role = str(turn.get("role") or "user").strip().lower()
            role_label = "assistant" if role == "assistant" else "user"
            text = str(turn.get("text") or turn.get("content") or "").strip()
            if text:
                history_lines.append(f"{role_label}: {text[:300]}")

        selected_tables_for_sql = []
        seen_sql_tables: set[str] = set()
        for obj in selected_context_objects:
            if (
                obj["schema"].lower() == resolved_schema.lower()
                and obj["name"].lower() == resolved_object.lower()
            ):
                continue

            qualified_ref = f"{obj['schema']}.{obj['name']}"
            if qualified_ref.lower() in seen_sql_tables:
                continue
            seen_sql_tables.add(qualified_ref.lower())
            selected_tables_for_sql.append(qualified_ref)

        selected_tables_for_sql = selected_tables_for_sql[:12]

        retrieval_context_payload = {
            "database": resolved_database_name,
            "schema": resolved_schema,
            "base_object": resolved_object,
            "retrieved_objects": retrieval_objects[:6],
            "relationship_hints": relationship_summary.get("suggested_relationships", [])[:6],
            "history": history_lines,
        }

        augmented_prompt = "\n\n".join(
            [
                f"Business question: {prompt_text}",
                "Use this retrieved metadata context to answer accurately:",
                json.dumps(retrieval_context_payload),
                "Generate SQL that directly supports business insights and keep it preview-safe where possible.",
            ]
        )

        sql_payload = SqlGenerationRequest(
            connection_id=payload.connection_id,
            database_name=resolved_database_name,
            schema=resolved_schema,
            object_name=resolved_object,
            user_prompt=augmented_prompt,
            selected_tables=selected_tables_for_sql,
        )

        generation_result = ai_generate_sql(sql_payload, _u)
        sql_text = (generation_result.get("sql") or "").strip().rstrip(";")

        query_result = None
        warnings: List[str] = list(generation_result.get("warnings") or [])

        if sql_text and is_previewable_sql(sql_text):
            try:
                cur = conn.cursor()

                count_query = f"SELECT COUNT(*) FROM ({sql_text}) AS subquery_count"
                cur.execute(count_query)
                total_count = cur.fetchone()[0]

                data_query = f"SELECT * FROM ({sql_text}) AS subquery_data LIMIT %s OFFSET %s"
                cur.execute(data_query, (100, 0))

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                cur.close()

                query_result = {
                    "columns": columns,
                    "rows": rows,
                    "total_count": total_count,
                    "limit": 100,
                    "offset": 0,
                }
            except Exception as e:
                warnings.append(f"Failed to execute preview SQL: {str(e)}")

        insights = summarize_query_result(
            (query_result or {}).get("columns") or [],
            (query_result or {}).get("rows") or [],
        )

        assistant_text = build_agentic_response_text(
            prompt_text,
            resolved_schema,
            resolved_object,
            generation_result,
            query_result,
            insights,
        )

        return {
            "mode": "completed",
            "assistant_response": assistant_text,
            "context": {
                "database": resolved_database_name,
                "schema": resolved_schema,
                "object": resolved_object,
                "selected_objects": [
                    f"{obj['schema']}.{obj['name']}" for obj in selected_context_objects[:8]
                ],
            },
            "generation_meta": {
                "title": generation_result.get("title"),
                "statement_type": generation_result.get("statement_type"),
                "previewable": generation_result.get("previewable"),
                "explanation": generation_result.get("explanation"),
                "assumptions": generation_result.get("assumptions", []),
                "warnings": warnings,
                "llm_enabled": generation_result.get("llm_enabled"),
                "llm_reason": generation_result.get("llm_reason"),
            },
            "sql": sql_text,
            "query_result": query_result,
            "insights": insights,
            "retrieval_context": retrieval_context_payload,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run agentic insights flow: {str(e)}")
    finally:
        conn.close()


@app.post("/api/ai/generate-sql")
def ai_generate_sql(payload: SqlGenerationRequest, _u=_dev_or_admin):
    conn = get_db_connection_by_id(payload.connection_id, payload.database_name)

    try:
        object_type = get_object_type(conn, payload.schema_name, payload.object_name)
        if not object_type:
            raise HTTPException(status_code=404, detail="Object not found")

        base_columns = get_object_columns(conn, payload.schema_name, payload.object_name)
        relationships = build_relationship_summary(conn, payload.schema_name, payload.object_name)
        view_definition = get_view_definition(conn, payload.schema_name, payload.object_name)
        schema_objects = get_schema_objects(conn, payload.schema_name)
        known_schema_objects = {obj["name"].lower(): obj["type"] for obj in schema_objects}

        # Also get all objects across all schemas for cross-schema resolution
        all_db_objects = get_all_schema_objects(conn)
        all_objects_lookup = {}
        for obj in all_db_objects:
            all_objects_lookup[(obj["schema"].lower(), obj["name"].lower())] = obj["type"]
            # Also index by name-only for unqualified references
            if obj["name"].lower() not in all_objects_lookup:
                all_objects_lookup[obj["name"].lower()] = (obj["schema"], obj["name"], obj["type"])

        effective_user_prompt = (payload.user_prompt or "").strip()
        if not effective_user_prompt:
            effective_user_prompt = (
                f"Generate a useful PostgreSQL SELECT query using {payload.object_name} "
                f"as the base table. Add joins only when strongly supported by metadata."
            )

        # Detect referenced objects from the prompt — search across ALL schemas
        prompt_referenced_objects = extract_prompt_object_names(
            effective_user_prompt,
            schema_objects,
            payload.object_name,
        )

        # Also detect cross-schema references from the prompt
        cross_schema_refs = []
        prompt_lower = effective_user_prompt.lower()
        for obj in all_db_objects:
            if obj["schema"].lower() == payload.schema_name.lower():
                continue  # Already handled by prompt_referenced_objects
            obj_name_lower = obj["name"].lower()
            pattern = rf'(?<![a-z0-9_]){re.escape(obj_name_lower)}(?![a-z0-9_])'
            if re.search(pattern, prompt_lower):
                cross_schema_refs.append({
                    "schema": obj["schema"],
                    "name": obj["name"],
                    "type": obj["type"],
                })

        related_objects_context: List[Dict[str, Any]] = []
        related_object_refs: List[Dict[str, str]] = []
        seen_related_keys: set[tuple[str, str]] = set()

        # Include explicitly selected tables (supports both "table" and "schema.table" references).
        for selected_ref in payload.selected_tables:
            sel_schema, sel_name = parse_object_reference(selected_ref, payload.schema_name)
            if not sel_name:
                continue

            key = (sel_schema.lower(), sel_name.lower())
            if key in seen_related_keys:
                continue
            if key == (payload.schema_name.lower(), payload.object_name.lower()):
                continue

            object_type_name = all_objects_lookup.get(key)
            if not object_type_name and sel_schema.lower() == payload.schema_name.lower():
                object_type_name = known_schema_objects.get(sel_name.lower())
            if not object_type_name:
                continue

            related_object_refs.append({"schema": sel_schema, "name": sel_name, "type": object_type_name})
            seen_related_keys.add(key)

        # Include prompt-detected same-schema objects.
        for table_name in prompt_referenced_objects:
            if not table_name:
                continue
            key = (payload.schema_name.lower(), table_name.lower())
            if key in seen_related_keys:
                continue
            if table_name.lower() == payload.object_name.lower():
                continue

            object_type_name = known_schema_objects.get(table_name.lower())
            if not object_type_name:
                continue

            related_object_refs.append({"schema": payload.schema_name, "name": table_name, "type": object_type_name})
            seen_related_keys.add(key)

        # Include prompt-detected cross-schema objects.
        for ref in cross_schema_refs:
            key = (ref["schema"].lower(), ref["name"].lower())
            if key in seen_related_keys:
                continue
            related_object_refs.append({"schema": ref["schema"], "name": ref["name"], "type": ref["type"]})
            seen_related_keys.add(key)

        for ref in related_object_refs[:16]:
            try:
                metadata = get_object_metadata(conn, ref["schema"], ref["name"], ref["type"])
                if metadata:
                    related_objects_context.append(metadata)
            except Exception:
                continue

        requested_statement = detect_requested_statement_type(effective_user_prompt)
        stmt_hint = statement_type_label(requested_statement)

        # Build a human-readable column specification block for the system prompt
        def format_columns_block(schema_name: str, obj_name: str, columns: list) -> str:
            lines = [f'  Table: "{schema_name}"."{obj_name}"']
            for c in columns:
                nullable = "NULL" if c.get("is_nullable") == "YES" else "NOT NULL"
                lines.append(f'    - {c["column_name"]} ({c.get("data_type", "unknown")}, {nullable})')
            return "\n".join(lines)

        base_columns_block = format_columns_block(payload.schema_name, payload.object_name, base_columns)

        related_columns_blocks = []
        for ro in related_objects_context:
            block = format_columns_block(
                ro["schema_name"], ro["object_name"], ro.get("columns", [])
            )
            related_columns_blocks.append(block)

        all_columns_text = base_columns_block
        if related_columns_blocks:
            all_columns_text += "\n\n" + "\n\n".join(related_columns_blocks)

        system_prompt = f"""You are a senior PostgreSQL analytics and data engineering assistant.

=== AVAILABLE TABLE METADATA (EXACT COLUMNS) ===
{all_columns_text}
=== END OF METADATA ===

ABSOLUTE RULE: The columns listed above are the ONLY columns that exist. You must ONLY
use column names from the metadata above. If a column name is not listed above, it does
NOT exist and you must NOT use it. Do NOT guess or invent column names.

Return STRICT JSON only with this shape:
{{
  "title": "short title",
  "sql": "complete SQL statement(s)",
  "statement_type": "<detected type e.g. select, insert, create_table, create_view, merge, update, delete, with_query>",
  "explanation": "brief explanation",
  "assumptions": ["assumption1"],
  "warnings": ["warning1"],
  "target_table_hint": "optional target table name or empty string"
}}

Rules:
- Generate PostgreSQL-compatible SQL.
- CRITICAL: You MUST use ONLY the exact column names shown in the AVAILABLE TABLE METADATA
  section above. Cross-check every column you reference against the metadata. If it is not
  listed, do NOT use it. This is the most important rule.
- When selecting columns, use actual column names from the metadata above.
  When casting or transforming, respect the data_type specified for each column.
- Read the user's natural-language prompt carefully. The SQL statement type you generate
  MUST match what the user is actually asking for.
- The auto-detected statement type hint is: "{requested_statement}" ({stmt_hint}).
  Use this as a hint but ALWAYS defer to the user's actual words if they conflict.
- Prefer fully qualified names using "schema"."table".
- Use explicit JOIN clauses where needed.
- If the user did not specify an exact target table for INSERT/CREATE/UPDATE/DELETE/MERGE,
  make a reasonable best-effort name and mention it in assumptions/warnings.
- For CREATE TABLE requests, generate a full DDL CREATE TABLE statement (with columns,
  data types, constraints). For CTAS, use CREATE TABLE ... AS SELECT.
- For SCD Type 2 / slowly changing dimension requests, include appropriate surrogate key,
  effective dates (valid_from, valid_to), and is_current flag columns.
- If a table is not present in the metadata section above, do not reference it as a source.
- For deduplication, use ROW_NUMBER() OVER (PARTITION BY <key_columns> ORDER BY <timestamp> DESC).
- For staging-to-core loads, apply COALESCE for null handling, TRIM on text columns,
  and appropriate CAST operations based on actual column data types from the metadata.
- Return JSON only. No markdown, no explanation outside the JSON.
"""

        # Build available schema objects with cross-schema info
        available_objects_list = [
            {"schema": payload.schema_name, "name": obj["name"], "type": obj["type"]}
            for obj in schema_objects[:200]
        ]
        for ref in cross_schema_refs[:20]:
            available_objects_list.append(ref)

        user_prompt = json.dumps({
            "database_name": payload.database_name,
            "schema": payload.schema_name,
            "base_object": payload.object_name,
            "base_object_type": object_type,
            "base_columns": [
                {
                    "column_name": c["column_name"],
                    "data_type": c["data_type"],
                    "is_nullable": c["is_nullable"],
                }
                for c in base_columns
            ],
            "verified_relationships": {
                "primary_keys": relationships.get("primary_keys", []),
                "foreign_keys": relationships.get("foreign_keys", []),
                "referenced_by": relationships.get("referenced_by", []),
                "heuristic_relationships": relationships.get("suggested_relationships", []),
            },
            "available_schema_objects": available_objects_list,
            "prompt_referenced_objects": prompt_referenced_objects,
            "cross_schema_referenced_objects": [
                f'{ref["schema"]}.{ref["name"]}' for ref in cross_schema_refs
            ],
            "selected_join_tables": payload.selected_tables,
            "retrieved_related_objects": related_objects_context,
            "view_definition": view_definition,
            "requested_statement_type": requested_statement,
            "user_prompt": effective_user_prompt,
        }, indent=2)

        llm_res = call_llm_json(system_prompt, user_prompt)
        llm_content = llm_res.get("content") if llm_res else None

        # Build metadata debug info so frontend can show what was sent
        metadata_sent = {
            "base_object": f"{payload.schema_name}.{payload.object_name}",
            "base_columns": [c["column_name"] for c in base_columns],
            "related_objects": [
                f'{ro["schema_name"]}.{ro["object_name"]} ({len(ro.get("columns", []))} cols)'
                for ro in related_objects_context
            ],
            "cross_schema_refs": [f'{ref["schema"]}.{ref["name"]}' for ref in cross_schema_refs],
        }

        if not llm_content:
            fallback_cols = ", ".join(c["column_name"] for c in base_columns) if base_columns else "*"
            fallback_sql = f'SELECT {fallback_cols} FROM {quote_ident(payload.schema_name)}.{quote_ident(payload.object_name)} LIMIT 100'
            record_prompt_history(
                payload.connection_id,
                payload.database_name,
                payload.schema_name,
                payload.object_name,
                effective_user_prompt,
                "select",
                fallback_sql
            )
            return {
                "title": "Fallback SQL",
                "sql": fallback_sql,
                "statement_type": "select",
                "previewable": True,
                "explanation": "LLM was not available, so a safe fallback SELECT was generated using actual metadata columns.",
                "assumptions": ["Generated without AI assistance."],
                "warnings": [llm_res.get("reason", "LLM unavailable")],
                "llm_enabled": llm_res.get("enabled", False),
                "llm_reason": llm_res.get("reason", ""),
                "metadata_sent": metadata_sent,
            }

        sql = (llm_content.get("sql") or "").strip()
        returned_statement_type = (llm_content.get("statement_type") or requested_statement or "select").strip().lower()
        if returned_statement_type == "auto":
            returned_statement_type = "select"

        if not sql:
            fallback_cols = ", ".join(c["column_name"] for c in base_columns) if base_columns else "*"
            sql = f'SELECT {fallback_cols} FROM {quote_ident(payload.schema_name)}.{quote_ident(payload.object_name)} LIMIT 100'
            returned_statement_type = "select"

        previewable = is_previewable_sql(sql)
        warnings = list(llm_content.get("warnings", []))
        assumptions = list(llm_content.get("assumptions", []))

        validation = validate_generated_sql_against_metadata(
            conn,
            payload.schema_name,
            sql,
            known_schema_objects,
        )

        if validation["validated_objects"]:
            assumptions.append(
                "Validated source metadata against: " + ", ".join(validation["validated_objects"])
            )

        if not validation["valid"]:
            # Auto-retry: feed validation errors back to the LLM for self-correction
            retry_errors = " | ".join(validation["errors"])
            retry_user_prompt = json.dumps({
                "RETRY": True,
                "previous_sql": sql,
                "validation_errors": validation["errors"],
                "instruction": (
                    f"Your previous SQL had these metadata errors: {retry_errors}. "
                    "Re-generate the SQL using ONLY the exact column names from the "
                    "AVAILABLE TABLE METADATA in the system prompt. Do NOT invent columns."
                ),
                "original_request": effective_user_prompt,
            }, indent=2)

            retry_res = call_llm_json(system_prompt, retry_user_prompt)
            retry_content = retry_res.get("content") if retry_res else None

            if retry_content and retry_content.get("sql"):
                retry_sql = retry_content["sql"].strip()
                retry_validation = validate_generated_sql_against_metadata(
                    conn, payload.schema_name, retry_sql, known_schema_objects,
                )
                if retry_validation["valid"]:
                    sql = retry_sql
                    warnings = list(retry_content.get("warnings", []))
                    assumptions = list(retry_content.get("assumptions", []))
                    assumptions.append("Auto-corrected after initial validation failure.")
                    if retry_validation["validated_objects"]:
                        assumptions.append(
                            "Validated source metadata against: "
                            + ", ".join(retry_validation["validated_objects"])
                        )
                    returned_statement_type = (
                        retry_content.get("statement_type") or requested_statement or "select"
                    ).strip().lower()
                    llm_content = retry_content
                    previewable = is_previewable_sql(sql)
                else:
                    # Both attempts failed — return with warnings instead of hard error
                    warnings.extend(validation["errors"])
                    warnings.append("Auto-retry also failed validation. Please review column names.")
            else:
                warnings.extend(validation["errors"])
                warnings.append("Auto-retry was unavailable. Please review column names.")

        if requested_statement != "auto" and returned_statement_type != requested_statement:
            warnings.append(
                f"Requested statement type was '{requested_statement}', but the model labeled the response as '{returned_statement_type}'."
            )

        record_prompt_history(
            payload.connection_id,
            payload.database_name,
            payload.schema_name,
            payload.object_name,
            effective_user_prompt,
            returned_statement_type,
            sql
        )

        return {
            "title": llm_content.get("title", "Generated SQL"),
            "sql": sql,
            "statement_type": returned_statement_type,
            "previewable": previewable,
            "explanation": llm_content.get("explanation", ""),
            "assumptions": assumptions,
            "warnings": warnings,
            "target_table_hint": llm_content.get("target_table_hint", ""),
            "llm_enabled": llm_res.get("enabled", False),
            "llm_reason": llm_res.get("reason", ""),
            "metadata_sent": metadata_sent,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL: {str(e)}")
    finally:
        conn.close()


@app.post("/api/recipes")
def create_recipe(payload: RecipeSaveRequest, _u=_dev_or_admin):
    conn = get_app_store_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO recipes (
            connection_id, database_name, schema_name, object_name,
            recipe_name, user_prompt, statement_type, selected_tables_json,
            sql_text, explanation, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload.connection_id,
        payload.database_name,
        payload.schema_name,
        payload.object_name,
        payload.recipe_name,
        payload.user_prompt,
        payload.statement_type,
        json.dumps(payload.selected_tables),
        payload.sql,
        payload.explanation,
        datetime.utcnow().isoformat(),
    ))

    recipe_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {"status": "success", "id": recipe_id}


@app.get("/api/recipes")
def list_recipes(
    connection_id: int,
    database_name: str,
    schema: str,
    object_name: str,
    _u=_any_authenticated,
):
    conn = get_app_store_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM recipes
        WHERE connection_id = ?
          AND database_name = ?
          AND schema_name = ?
          AND object_name = ?
        ORDER BY id DESC
    """, (connection_id, database_name, schema, object_name))

    rows = cur.fetchall()
    conn.close()

    recipes = []
    for row in rows:
        recipes.append({
            "id": row["id"],
            "connection_id": row["connection_id"],
            "database_name": row["database_name"],
            "schema_name": row["schema_name"],
            "object_name": row["object_name"],
            "recipe_name": row["recipe_name"],
            "user_prompt": row["user_prompt"],
            "statement_type": row["statement_type"],
            "selected_tables": json.loads(row["selected_tables_json"] or "[]"),
            "sql": row["sql_text"],
            "explanation": row["explanation"],
            "created_at": row["created_at"],
        })

    return {"recipes": recipes}


@app.delete("/api/recipes/{recipe_id}")
def remove_recipe(recipe_id: int, _u=_dev_or_admin):
    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM recipes WHERE id = ?", (recipe_id,))
    conn.commit()
    conn.close()
    return {"status": "success"}


@app.get("/api/prompt-history")
def list_prompt_history(
    connection_id: int,
    database_name: str,
    schema: str,
    object_name: str,
    limit: int = 10,
    _u=_any_authenticated,
):
    conn = get_app_store_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM prompt_history
        WHERE connection_id = ?
          AND database_name = ?
          AND schema_name = ?
          AND object_name = ?
        ORDER BY id DESC
        LIMIT ?
    """, (connection_id, database_name, schema, object_name, limit))

    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "user_prompt": row["user_prompt"],
            "statement_type": row["statement_type"],
            "sql": row["sql_text"],
            "created_at": row["created_at"],
        })

    return {"items": items}


@app.post("/api/ai/data-quality-sql")
def ai_data_quality_sql(payload: DataQualitySqlRequest, _u=_dev_or_admin):
    conn = get_db_connection_by_id(payload.connection_id, payload.database_name)

    try:
        columns = get_object_columns(conn, payload.schema_name, payload.object_name)
        if not columns:
            raise HTTPException(status_code=404, detail="Object not found or no columns found")

        q_schema = quote_ident(payload.schema_name)
        q_table = quote_ident(payload.object_name)

        checks = []

        null_sql_parts = []
        for col in columns[:12]:
            q_col = quote_ident(col["column_name"])
            alias = f'{col["column_name"]}_nulls'
            null_sql_parts.append(f"SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) AS {quote_ident(alias)}")

        checks.append({
            "title": "Null Profile",
            "description": "Counts null values for key columns.",
            "sql": f"SELECT\n  " + ",\n  ".join(null_sql_parts) + f"\nFROM {q_schema}.{q_table};"
        })

        col_names = [c["column_name"] for c in columns]
        candidate_id = next((c for c in col_names if c.lower() == "id" or c.lower().endswith("_id")), None)
        if candidate_id:
            q_id = quote_ident(candidate_id)
            checks.append({
                "title": "Duplicate Key Check",
                "description": f"Finds duplicate values in {candidate_id}.",
                "sql": f"""
SELECT {q_id}, COUNT(*) AS duplicate_count
FROM {q_schema}.{q_table}
GROUP BY {q_id}
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
""".strip()
            })

        checks.append({
            "title": "Row Count",
            "description": "Basic row count for the object.",
            "sql": f"SELECT COUNT(*) AS total_rows FROM {q_schema}.{q_table};"
        })

        return {
            "object_name": payload.object_name,
            "schema": payload.schema_name,
            "checks": checks
        }

    finally:
        conn.close()


# =========================
# Agentic Pipeline Step Generator
# =========================

class AgenticPipelineStepsRequest(BaseModel):
    user_requirement: str
    selected_tables: Optional[List[str]] = []


@app.post("/api/pipelines/{pipeline_id}/ai-generate-steps")
def ai_generate_pipeline_steps(
    pipeline_id: int,
    payload: AgenticPipelineStepsRequest,
    _u=_dev_or_admin,
):
    """
    Multi-phase agentic AI pipeline step generator.
    Phase 1 — PLAN: Decompose requirements into ordered SQL steps.
    Phase 2 — GENERATE: Per-step SQL generation with full metadata injection.
    Phase 3 — VALIDATE & HEAL: Validate each step's SQL and self-correct on failure (up to 2 retries).
    Returns copy-paste-ready, validated SQL steps.
    """
    if not OPENAI_API_KEY or client is None:
        raise HTTPException(
            status_code=503,
            detail="OPENAI_API_KEY is not configured. Agentic AI requires a valid OpenAI key.",
        )

    # ── Load pipeline context from SQLite ──────────────────────────────────
    app_conn = get_app_store_conn()
    try:
        cur = app_conn.cursor()
        cur.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,))
        pipeline_row = cur.fetchone()
    finally:
        app_conn.close()

    if not pipeline_row:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = dict(pipeline_row)
    connection_id = pipeline.get("connection_id")
    schema_name = (pipeline.get("schema_name") or "public").strip()
    database_name = (pipeline.get("database_name") or "").strip() or None
    source_object = (pipeline.get("source_object") or "").strip()
    target_object = (pipeline.get("target_object") or "").strip()
    pipeline_name = pipeline.get("name", "")

    # ── Connect to target database ────────────────────────────────────────
    db_conn = get_db_connection_by_id(connection_id, database_name)
    agent_log: List[Dict[str, Any]] = []

    try:
        # ── METADATA FETCH ─────────────────────────────────────────────────
        schema_objects = get_schema_objects(db_conn, schema_name)
        known_schema_objects = {obj["name"].lower(): obj["type"] for obj in schema_objects}
        all_db_objects = get_all_schema_objects(db_conn)

        def _col_block(schema: str, name: str, columns: list) -> str:
            lines = [f'Table: "{schema}"."{name}"']
            for c in columns:
                nullable = "NULL" if c.get("is_nullable") == "YES" else "NOT NULL"
                lines.append(
                    f'  - {c["column_name"]} ({c.get("data_type", "unknown")}, {nullable})'
                )
            return "\n".join(lines)

        def _load_meta(schema: str, name: str) -> Optional[str]:
            """Return a column-block string for one table, or None on failure."""
            try:
                cols = get_object_columns(db_conn, schema, name)
                if cols:
                    return _col_block(schema, name, cols)
            except Exception:
                pass
            return None

        # Pre-load metadata for source, target, and explicitly selected tables
        preloaded_blocks: List[str] = []
        preloaded_names: set = set()

        for tbl_schema, tbl_name in [
            (schema_name, source_object),
            (schema_name, target_object),
        ]:
            if tbl_name and tbl_name.lower() not in preloaded_names and tbl_name.lower() in known_schema_objects:
                blk = _load_meta(tbl_schema, tbl_name)
                if blk:
                    preloaded_blocks.append(blk)
                    preloaded_names.add(tbl_name.lower())

        for tbl in (payload.selected_tables or []):
            tbl_lower = tbl.lower()
            if tbl_lower not in preloaded_names and tbl_lower in known_schema_objects:
                blk = _load_meta(schema_name, tbl)
                if blk:
                    preloaded_blocks.append(blk)
                    preloaded_names.add(tbl_lower)

        base_metadata_text = "\n\n".join(preloaded_blocks) if preloaded_blocks else "(no pre-loaded metadata — tables will be resolved per step)"

        available_objects_summary = "\n".join(
            f'  "{schema_name}"."{o["name"]}" ({o["type"]})'
            for o in schema_objects[:120]
        )

        agent_log.append({
            "phase": "METADATA_FETCH",
            "status": "ok",
            "details": f"Pre-loaded {len(preloaded_blocks)} table(s). Schema has {len(schema_objects)} object(s).",
        })

        # ── PHASE 1: PLAN ─────────────────────────────────────────────────
        plan_system = f"""You are an expert PostgreSQL data pipeline architect.

=== PRE-LOADED TABLE METADATA ===
{base_metadata_text}
=== END METADATA ===

=== ALL OBJECTS IN SCHEMA "{schema_name}" ===
{available_objects_summary}
=== END OBJECTS ===

Your task: Read the user's pipeline requirements and produce an ordered execution plan of SQL steps.
Each step must be one atomic SQL operation (CREATE TABLE, INSERT INTO, UPDATE, MERGE, DROP, TRUNCATE, etc.).

Return STRICT JSON only — no markdown, no extra text:
{{
  "plan_summary": "one sentence describing the full pipeline",
  "steps": [
    {{
      "order": 1,
      "step_name": "short descriptive name",
      "step_type": "sql",
      "description": "detailed description of what this step does and why",
      "sql_intent": "exact SQL operation pattern, e.g. CREATE TABLE AS SELECT with deduplication",
      "source_tables": ["schema.table1", "schema.table2"],
      "target_table": "schema.output_table"
    }}
  ]
}}

Rules:
- Only reference tables that EXIST in the metadata or object list above.
- Order steps so dependencies are created before they are used.
- Typical ETL pattern: (1) drop/truncate staging, (2) create/populate staging, (3) transform, (4) merge/upsert final, (5) cleanup.
- Maximum 8 steps. Be precise — one SQL statement per step.
- Use fully-qualified names: "schema"."table"."""

        plan_user = json.dumps({
            "pipeline_name": pipeline_name,
            "source_object": f"{schema_name}.{source_object}" if source_object else "not specified",
            "target_object": f"{schema_name}.{target_object}" if target_object else "not specified",
            "schema": schema_name,
            "database": database_name or "default",
            "user_requirement": payload.user_requirement,
            "selected_tables": payload.selected_tables or [],
        }, indent=2)

        plan_res = call_llm_json(plan_system, plan_user)
        plan_content = plan_res.get("content") if plan_res else None

        if not plan_content or not isinstance(plan_content.get("steps"), list) or not plan_content["steps"]:
            raise HTTPException(
                status_code=500,
                detail=f"Agent planning phase failed: {plan_res.get('reason', 'LLM returned no structured plan')}",
            )

        planned_steps = plan_content["steps"]
        plan_summary = plan_content.get("plan_summary", "")
        agent_log.append({
            "phase": "PLAN",
            "status": "ok",
            "details": f"Planned {len(planned_steps)} steps. Summary: {plan_summary}",
        })

        # ── PHASE 2 (GENERATE) + PHASE 3 (VALIDATE & HEAL) per step ──────
        generated_steps: List[Dict[str, Any]] = []

        for step_plan in planned_steps:
            step_name = step_plan.get("step_name", f"Step {step_plan.get('order', '?')}")
            description = step_plan.get("description", "")
            sql_intent = step_plan.get("sql_intent", "")
            source_tables_raw = step_plan.get("source_tables", [])
            target_table_raw = step_plan.get("target_table", "")

            # Resolve additional tables this step needs and load their metadata
            step_blocks = list(preloaded_blocks)
            step_loaded = set(preloaded_names)

            def _resolve_table(full_name: str):
                """Parse 'schema.table' or 'table' and load if not yet loaded."""
                parts = full_name.strip('"').split(".")
                tbl_s = parts[-2].strip('"') if len(parts) >= 2 else schema_name
                tbl_n = parts[-1].strip('"')
                key = f"{tbl_s}.{tbl_n}".lower()
                if key in step_loaded:
                    return
                if tbl_n.lower() in known_schema_objects:
                    blk = _load_meta(tbl_s, tbl_n)
                    if blk:
                        step_blocks.append(blk)
                        step_loaded.add(key)
                        step_loaded.add(tbl_n.lower())
                else:
                    # Try all schemas
                    for obj in all_db_objects:
                        if obj["name"].lower() == tbl_n.lower():
                            blk = _load_meta(obj["schema"], obj["name"])
                            if blk:
                                step_blocks.append(blk)
                                step_loaded.add(key)
                                step_loaded.add(tbl_n.lower())
                            break

            for raw_tbl in source_tables_raw[:6]:
                _resolve_table(raw_tbl)
            if target_table_raw:
                _resolve_table(target_table_raw)

            step_metadata_text = "\n\n".join(step_blocks) if step_blocks else base_metadata_text

            gen_system = f"""You are a senior PostgreSQL data engineer. Generate production-quality SQL for a single pipeline step.

=== EXACT TABLE METADATA — ONLY THESE COLUMNS EXIST ===
{step_metadata_text}
=== END METADATA ===

ABSOLUTE RULES (violation = broken pipeline):
1. Use ONLY column names that are explicitly listed in the metadata above. Not a single invented column.
2. Use schema-qualified table names everywhere: "schema_name"."table_name"
3. Every SQL statement must be complete and executable with zero edits — no placeholders, no comments like --TODO
4. Match PostgreSQL data types exactly. Use explicit CAST() when combining columns of different types.
5. For CREATE TABLE steps: include DROP TABLE IF EXISTS before the CREATE.
6. For INSERT INTO steps: the column list must exactly match the target table's columns from metadata.
7. For MERGE/UPSERT: both WHEN MATCHED and WHEN NOT MATCHED must use only real column names.
8. Apply COALESCE for NULL-able columns, TRIM() for text/varchar columns in transformations.
9. For deduplication: ROW_NUMBER() OVER (PARTITION BY <pk_cols> ORDER BY <updated_at_col> DESC).
10. The SQL must work the first time it runs — assume a fresh database with the schema from metadata.

Return STRICT JSON only — no markdown:
{{
  "sql": "complete executable PostgreSQL SQL",
  "explanation": "one precise sentence: what this step does",
  "columns_used": ["exact_col1", "exact_col2"],
  "assumptions": [],
  "warnings": []
}}"""

            gen_user = json.dumps({
                "step_name": step_name,
                "step_description": description,
                "sql_intent": sql_intent,
                "source_tables": source_tables_raw,
                "target_table": target_table_raw,
                "pipeline_source_object": f"{schema_name}.{source_object}" if source_object else "",
                "pipeline_target_object": f"{schema_name}.{target_object}" if target_object else "",
                "schema": schema_name,
                "user_requirement": payload.user_requirement,
            }, indent=2)

            gen_res = call_llm_json(gen_system, gen_user)
            gen_content = gen_res.get("content") if gen_res else None

            if not gen_content or not gen_content.get("sql"):
                generated_steps.append({
                    "step_name": step_name,
                    "step_type": "sql",
                    "sql_text": f"-- Could not generate SQL for: {description}",
                    "explanation": description,
                    "validated": False,
                    "warnings": [f"AI generation failed: {gen_res.get('reason', 'no output')}"],
                    "assumptions": [],
                })
                agent_log.append({
                    "phase": "GENERATE",
                    "step": step_name,
                    "status": "failed",
                    "details": gen_res.get("reason", "no SQL returned"),
                })
                continue

            sql = gen_content["sql"].strip()
            warnings = list(gen_content.get("warnings", []))
            assumptions = list(gen_content.get("assumptions", []))

            # ── Validate + self-heal loop ──────────────────────────────────
            validation = validate_generated_sql_against_metadata(
                db_conn, schema_name, sql, known_schema_objects
            )
            heal_attempts = 0

            while not validation["valid"] and heal_attempts < 2:
                heal_attempts += 1
                retry_errors = " | ".join(validation["errors"])
                heal_user = json.dumps({
                    "SELF_HEALING_ATTEMPT": heal_attempts,
                    "previous_sql": sql,
                    "validation_errors": validation["errors"],
                    "instruction": (
                        f"Your SQL had these metadata errors: {retry_errors}. "
                        "Fix them. You MUST use only the EXACT column names listed in the "
                        "EXACT TABLE METADATA in the system prompt. Return corrected JSON."
                    ),
                    "step_name": step_name,
                    "sql_intent": sql_intent,
                }, indent=2)

                heal_res = call_llm_json(gen_system, heal_user)
                heal_content = heal_res.get("content") if heal_res else None

                if heal_content and heal_content.get("sql"):
                    sql = heal_content["sql"].strip()
                    warnings = list(heal_content.get("warnings", []))
                    assumptions = list(heal_content.get("assumptions", []))
                    validation = validate_generated_sql_against_metadata(
                        db_conn, schema_name, sql, known_schema_objects
                    )
                    if validation["valid"]:
                        assumptions.append(f"Auto-healed after {heal_attempts} correction attempt(s).")
                else:
                    break

            if not validation["valid"]:
                warnings.extend(validation["errors"])
                warnings.append(
                    "Could not fully validate all table references after self-healing. "
                    "Review column names before running."
                )

            agent_log.append({
                "phase": "GENERATE",
                "step": step_name,
                "status": "ok" if validation["valid"] else "partial",
                "details": (
                    f"validated={validation['valid']}, "
                    f"tables_checked={validation.get('validated_objects', [])}, "
                    f"heal_attempts={heal_attempts}"
                ),
            })

            generated_steps.append({
                "step_name": step_name,
                "step_type": "sql",
                "sql_text": sql,
                "explanation": gen_content.get("explanation", description),
                "validated": validation["valid"],
                "warnings": warnings,
                "assumptions": assumptions,
            })

        return {
            "plan_summary": plan_summary,
            "steps": generated_steps,
            "agent_log": agent_log,
            "llm_enabled": True,
            "total_steps": len(generated_steps),
            "validated_steps": sum(1 for s in generated_steps if s.get("validated")),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Agentic pipeline generation failed: {str(e)}",
        )
    finally:
        try:
            db_conn.close()
        except Exception:
            pass