import io
import json
import os
import re
import time
import zipfile
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from openai import OpenAI
from pydantic import BaseModel, Field

from app.core.app_store import get_app_store_conn
from app.core.security import get_current_user

router = APIRouter(tags=["Env Comparator & Optimizer"], dependencies=[Depends(get_current_user)])

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


DIFF_CLASSIFY_PROMPT_TEMPLATE = """
You are a PostgreSQL SQL reviewer.

Treat these as NOT logic differences:
- DB/Schema qualifiers
- CREATE OR REPLACE header differences
- Whitespace/casing

Logic/security differences:
- joins/filters/select-list/grouping/window changes

Return ONLY valid JSON:
{
  "statusType": "In Sync" | "Column Difference" | "Logic Difference" | "Other Difference",
  "impact": "Negligible" | "Medium" | "High",
  "categories": ["columns","joins","filters","select_list","group_by","window","other"],
  "comments": ["short bullet 1", "short bullet 2"]
}
""".strip()


class SchemaAliasedRequest(BaseModel):
    model_config = {"populate_by_name": True}


class DiffAnalyzeItem(BaseModel):
    key: str
    object: str
    type: str
    srcCols: List[str] = []
    tgtCols: List[str] = []
    srcDDL: str = ""
    tgtDDL: str = ""


class DiffAnalyzeRequest(BaseModel):
    items: List[DiffAnalyzeItem]


class QueryColumnsRequest(SchemaAliasedRequest):
    connection_id: int
    database_name: str
    schema_: str = Field(default="src", alias="schema")
    sql: str


class OptimizeQueryRequest(QueryColumnsRequest):
    pass


class InferPkRequest(QueryColumnsRequest):
    pass


class DedupAdvancedRequest(QueryColumnsRequest):
    pkCols: List[str] = []
    strategy: str = "LATEST_PER_PK"
    hashExcludeCols: List[str] = []
    exposeHash: bool = False
    hashColName: str = "ROW_HASH"
    target_schema: Optional[str] = Field(default=None, alias="targetSchema")
    target_table: Optional[str] = Field(default=None, alias="targetTable")


class BenchmarkRequest(QueryColumnsRequest):
    originalSql: str
    optimizedSql: str


class SyncItem(BaseModel):
    key: Optional[str] = None
    object: str
    type: str
    statusType: Optional[str] = None
    srcDDL: Optional[str] = ""
    tgtDDL: Optional[str] = ""


class SyncSchemaRequest(BaseModel):
    source_db: str
    source_schema: str
    target_db: str
    target_schema: str
    items: List[SyncItem] = []


class SyncObjectsRequest(BaseModel):
    source_db: str
    source_schema: str
    target_db: str
    target_schema: str
    object: str
    type: str
    tgtDDL: str


def _normalize_ident(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).replace('"', "").strip()


def _strip_trailing_semicolons(sql: str) -> str:
    return re.sub(r";+\s*$", "", (sql or "").strip())


def _is_select_sql(sql: str) -> bool:
    return bool(re.match(r"^\s*(with|select)\b", sql or "", re.IGNORECASE))


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _safe_filename(name: str) -> str:
    return re.sub(r"[^\w\-.]+", "_", name or "object")


def _normalize_for_compare(sql: str) -> str:
    if not sql:
        return ""
    text = str(sql)
    text = re.sub(r"^\s*CREATE\s+OR\s+REPLACE\s+[\s\S]*?\bAS\b", "AS", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def _compute_column_diff(src_cols: List[str], tgt_cols: List[str]) -> Dict[str, Any]:
    src_cols = src_cols or []
    tgt_cols = tgt_cols or []
    src_set = set(src_cols)
    tgt_set = set(tgt_cols)
    missing_in_target = [c for c in src_cols if c not in tgt_set]
    missing_in_source = [c for c in tgt_cols if c not in src_set]
    return {
        "columnDiff": bool(missing_in_target or missing_in_source),
        "missingInTarget": missing_in_target,
        "missingInSource": missing_in_source,
    }


def _extract_ai_text(response: Any) -> str:
    text_output = getattr(response, "output_text", None)
    if text_output:
        return str(text_output).strip()

    chunks: List[str] = []
    for item in getattr(response, "output", []) or []:
        for content_item in getattr(item, "content", []) or []:
            text_value = getattr(content_item, "text", None)
            if text_value:
                chunks.append(str(text_value))

    return "\n".join(chunks).strip()


def _safe_json_extract(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    t = text.strip()
    t = re.sub(r"```(?:json)?", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"```", "", t).strip()
    try:
        obj = json.loads(t)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", t)
    if not match:
        return None
    try:
        obj = json.loads(match.group(0))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _call_ai(prompt: str, max_tokens: int = 1400) -> str:
    if not openai_client:
        raise RuntimeError("OPENAI_API_KEY not configured")

    response = openai_client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }
        ],
        max_output_tokens=max_tokens,
    )
    text = _extract_ai_text(response)
    if not text:
        raise RuntimeError("OpenAI returned empty output")
    return text


def _get_saved_connection(connection_id: int) -> Dict[str, Any]:
    conn = get_app_store_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM connections WHERE id = ? AND is_active = TRUE", (connection_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise RuntimeError("Connection not found")

    data = dict(row)
    db_type = str(data.get("db_type") or "").lower()
    if db_type != "postgres":
        raise RuntimeError("Only PostgreSQL connections are supported for this module")

    return data


def _open_postgres_connection(connection_id: int, database_name: str):
    record = _get_saved_connection(connection_id)
    db_name = _normalize_ident(database_name) or _normalize_ident(record.get("database_name")) or "postgres"

    return psycopg2.connect(
        host=record["host"],
        port=record["port"],
        dbname=db_name,
        user=record["username"],
        password=record.get("password") or "",
    )


def _set_search_path(cur, schema_name: str) -> None:
    schema = _normalize_ident(schema_name)
    if not schema:
        schema = "public"
    cur.execute(f"SET search_path TO {_quote_ident(schema)}, public")


def _query_output_columns(connection_id: int, database_name: str, schema_name: str, sql: str) -> List[str]:
    body = _strip_trailing_semicolons(sql)
    if not _is_select_sql(body):
        raise RuntimeError("Only SELECT/WITH queries are allowed")

    conn = _open_postgres_connection(connection_id, database_name)
    try:
        cur = conn.cursor()
        _set_search_path(cur, schema_name)
        cur.execute(f"SELECT * FROM ({body}) AS q LIMIT 0")
        cols = [d[0] for d in (cur.description or [])]
        cur.close()
        return cols
    finally:
        conn.close()


def _run_scalar(connection_id: int, database_name: str, schema_name: str, sql: str) -> int:
    conn = _open_postgres_connection(connection_id, database_name)
    try:
        cur = conn.cursor()
        _set_search_path(cur, schema_name)
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _run_compile_check(connection_id: int, database_name: str, schema_name: str, sql: str) -> None:
    conn = _open_postgres_connection(connection_id, database_name)
    try:
        cur = conn.cursor()
        _set_search_path(cur, schema_name)
        cur.execute(f"SELECT * FROM ({_strip_trailing_semicolons(sql)}) AS q LIMIT 0")
        cur.close()
    finally:
        conn.close()


def _compare_query_results(
    connection_id: int,
    database_name: str,
    schema_name: str,
    left_sql: str,
    right_sql: str,
) -> Dict[str, Any]:
    left = _strip_trailing_semicolons(left_sql)
    right = _strip_trailing_semicolons(right_sql)

    left_count = _run_scalar(
        connection_id,
        database_name,
        schema_name,
        f"SELECT COUNT(*) FROM ({left}) AS a",
    )
    right_count = _run_scalar(
        connection_id,
        database_name,
        schema_name,
        f"SELECT COUNT(*) FROM ({right}) AS b",
    )

    diff_left_vs_right = _run_scalar(
        connection_id,
        database_name,
        schema_name,
        (
            "SELECT COUNT(*) FROM ("
            f"SELECT * FROM ({left}) AS a "
            f"EXCEPT ALL SELECT * FROM ({right}) AS b"
            ") AS d"
        ),
    )

    diff_right_vs_left = _run_scalar(
        connection_id,
        database_name,
        schema_name,
        (
            "SELECT COUNT(*) FROM ("
            f"SELECT * FROM ({right}) AS b "
            f"EXCEPT ALL SELECT * FROM ({left}) AS a"
            ") AS d"
        ),
    )

    return {
        "leftRowCount": left_count,
        "rightRowCount": right_count,
        "leftMinusRight": diff_left_vs_right,
        "rightMinusLeft": diff_right_vs_left,
        "isSameData": left_count == right_count and diff_left_vs_right == 0 and diff_right_vs_left == 0,
    }


def _heuristic_pk_candidates(cols: List[str]) -> List[Dict[str, Any]]:
    if not cols:
        return []

    upper = [c.upper() for c in cols]

    def pick(predicate):
        return [cols[i] for i, value in enumerate(upper) if predicate(value)]

    id_cols = pick(lambda v: v == "ID" or v.endswith("_ID"))
    key_cols = pick(lambda v: v.endswith("_KEY") or v.endswith("_PK"))
    code_cols = pick(lambda v: v.endswith("_CODE") or v.endswith("_NUMBER"))

    candidates: List[Dict[str, Any]] = []
    if id_cols:
        candidates.append({"columns": [id_cols[0]], "confidence": 0.86, "reason": "ID-like column"})
    if key_cols:
        candidates.append({"columns": [key_cols[0]], "confidence": 0.78, "reason": "Key-like column"})
    if code_cols:
        candidates.append({"columns": [code_cols[0]], "confidence": 0.62, "reason": "Business-code column"})
    if len(id_cols) >= 2:
        candidates.append({"columns": id_cols[:2], "confidence": 0.54, "reason": "Composite ID pair"})

    candidates.append({"columns": [cols[0]], "confidence": 0.25, "reason": "Fallback first column"})

    dedup = []
    seen = set()
    for candidate in candidates:
        key = tuple(col.upper() for col in candidate["columns"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(candidate)

    return dedup[:5]


def _build_hash_expr(cols: List[str]) -> str:
    if not cols:
        return "md5('<EMPTY>')"
    parts = [f"COALESCE(CAST({_quote_ident(c)} AS text), '<NULL>')" for c in cols]
    return "md5(concat_ws('¦', " + ", ".join(parts) + "))"


def _resolve_pk_columns(output_cols: List[str], pk_cols: List[str]) -> List[str]:
    output_map = {c.upper(): c for c in output_cols}
    resolved_pk: List[str] = []
    for col in pk_cols:
        match = output_map.get(str(col).upper())
        if not match:
            raise RuntimeError(f"PK column '{col}' not found in output")
        resolved_pk.append(match)

    if not resolved_pk:
        raise RuntimeError("Select at least one PK column")

    return resolved_pk


def _build_order_terms(output_cols: List[str]) -> List[str]:
    col_upper = [c.upper() for c in output_cols]

    def has_col(name: str) -> bool:
        return name.upper() in col_upper

    order_terms: List[str] = []
    if has_col("updated_at"):
        order_terms.append(f"{_quote_ident('updated_at')} DESC")
    if has_col("created_at"):
        order_terms.append(f"{_quote_ident('created_at')} DESC")
    if has_col("effective_from_dtm"):
        order_terms.append(f"{_quote_ident('effective_from_dtm')} DESC")
    if has_col("load_timestamp"):
        order_terms.append(f"{_quote_ident('load_timestamp')} DESC")
    if has_col("id"):
        order_terms.append(f"{_quote_ident('id')} DESC")

    # Deterministic fallback for queries without clear recency columns.
    order_terms.append(f"{_build_hash_expr(output_cols)} DESC")
    return order_terms


def _build_dedup_sql(
    sql: str,
    output_cols: List[str],
    pk_cols: List[str],
    strategy: str,
    hash_exclude_cols: List[str],
    expose_hash: bool,
    hash_col_name: str,
) -> str:
    body = _strip_trailing_semicolons(sql)
    if not _is_select_sql(body):
        raise RuntimeError("Only SELECT/WITH queries are allowed")

    resolved_pk = _resolve_pk_columns(output_cols, pk_cols)

    strategy_upper = (strategy or "LATEST_PER_PK").upper()
    hash_exclude_set = {str(c).upper() for c in (hash_exclude_cols or [])}

    if strategy_upper == "LATEST_PER_PK_WITH_HASH_ALL":
        hash_cols = list(output_cols)
    elif strategy_upper == "LATEST_PER_PK_WITH_HASH_EXCLUDE":
        hash_cols = [c for c in output_cols if c.upper() not in hash_exclude_set]
    else:
        hash_cols = []

    order_terms = _build_order_terms(output_cols)

    select_list = ",\n  ".join(_quote_ident(c) for c in output_cols)
    extra_select = ""
    if expose_hash and hash_cols:
        hash_alias = _quote_ident(hash_col_name or "row_hash")
        extra_select = f",\n  {_build_hash_expr(hash_cols)} AS {hash_alias}"

    part_list = ", ".join(_quote_ident(c) for c in resolved_pk)
    order_list = ", ".join(order_terms)

    return (
        "WITH src AS (\n"
        f"{body}\n"
        "),\n"
        "ranked AS (\n"
        "  SELECT\n"
        f"    {select_list}{extra_select},\n"
        "    ROW_NUMBER() OVER (\n"
        f"      PARTITION BY {part_list}\n"
        f"      ORDER BY {order_list}\n"
        "    ) AS __rn\n"
        "  FROM src\n"
        ")\n"
        "SELECT\n"
        f"  {select_list}{extra_select}\n"
        "FROM ranked\n"
        "WHERE __rn = 1"
    )


def _build_pk_duplicate_groups_sql(select_sql: str, pk_cols: List[str], cte_name: str = "src") -> str:
    pk_list = ", ".join(_quote_ident(c) for c in pk_cols)
    return (
        f"WITH {cte_name} AS (\n"
        f"{select_sql}\n"
        ")\n"
        "SELECT COUNT(*)\n"
        "FROM (\n"
        f"  SELECT {pk_list}\n"
        f"  FROM {cte_name}\n"
        f"  GROUP BY {pk_list}\n"
        "  HAVING COUNT(*) > 1\n"
        ") AS dup"
    )


def _build_row_count_sql(select_sql: str, cte_name: str = "src") -> str:
    return (
        f"WITH {cte_name} AS (\n"
        f"{select_sql}\n"
        ")\n"
        f"SELECT COUNT(*) FROM {cte_name}"
    )


def _build_idempotent_insert_sql(
    dedup_select_sql: str,
    output_cols: List[str],
    pk_cols: List[str],
    target_schema: str,
    target_table: str,
) -> str:
    target_ref = f"{_quote_ident(target_schema)}.{_quote_ident(target_table)}"
    insert_cols = ", ".join(_quote_ident(c) for c in output_cols)
    select_cols = ", ".join(f"d.{_quote_ident(c)}" for c in output_cols)
    pk_match = " AND ".join(
        f"t.{_quote_ident(c)} IS NOT DISTINCT FROM d.{_quote_ident(c)}" for c in pk_cols
    )

    return (
        "WITH deduped AS (\n"
        f"{dedup_select_sql}\n"
        ")\n"
        f"INSERT INTO {target_ref} ({insert_cols})\n"
        f"SELECT {select_cols}\n"
        "FROM deduped d\n"
        "WHERE NOT EXISTS (\n"
        f"  SELECT 1 FROM {target_ref} t\n"
        f"  WHERE {pk_match}\n"
        ")"
    )


def _build_target_overlap_count_sql(
    dedup_select_sql: str,
    pk_cols: List[str],
    target_schema: str,
    target_table: str,
) -> str:
    target_ref = f"{_quote_ident(target_schema)}.{_quote_ident(target_table)}"
    pk_match = " AND ".join(
        f"t.{_quote_ident(c)} IS NOT DISTINCT FROM d.{_quote_ident(c)}" for c in pk_cols
    )
    return (
        "WITH deduped AS (\n"
        f"{dedup_select_sql}\n"
        ")\n"
        "SELECT COUNT(*)\n"
        "FROM deduped d\n"
        "WHERE EXISTS (\n"
        f"  SELECT 1 FROM {target_ref} t\n"
        f"  WHERE {pk_match}\n"
        ")"
    )


def _run_explain_check(connection_id: int, database_name: str, schema_name: str, sql: str) -> None:
    conn = _open_postgres_connection(connection_id, database_name)
    try:
        cur = conn.cursor()
        _set_search_path(cur, schema_name)
        cur.execute(f"EXPLAIN {_strip_trailing_semicolons(sql)}")
        cur.close()
    finally:
        conn.close()


def _rewrite_schema_refs(sql: str, source_db: str, source_schema: str, target_db: str, target_schema: str) -> str:
    text = sql or ""
    src_schema_q = _quote_ident(_normalize_ident(source_schema))
    tgt_schema_q = _quote_ident(_normalize_ident(target_schema))

    src_db = _normalize_ident(source_db)
    tgt_db = _normalize_ident(target_db)

    if tgt_db and src_db and tgt_db.lower() != src_db.lower():
        text = re.sub(
            rf'"{re.escape(tgt_db)}"\s*\.\s*"{re.escape(_normalize_ident(target_schema))}"',
            f'{_quote_ident(src_db)}.{src_schema_q}',
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            rf'\b{re.escape(tgt_db)}\s*\.\s*{re.escape(_normalize_ident(target_schema))}\b',
            f'{src_db}.{_normalize_ident(source_schema)}',
            text,
            flags=re.IGNORECASE,
        )

    text = re.sub(
        rf'"{re.escape(_normalize_ident(target_schema))}"\s*\.',
        f"{src_schema_q}.",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        rf'\b{re.escape(_normalize_ident(target_schema))}\s*\.',
        f"{_normalize_ident(source_schema)}.",
        text,
        flags=re.IGNORECASE,
    )
    return text


def _build_sync_sql(
    source_db: str,
    source_schema: str,
    target_db: str,
    target_schema: str,
    obj_name: str,
    obj_type: str,
    target_ddl: str,
) -> str:
    cleaned_obj = _normalize_ident(obj_name)
    cleaned_type = str(obj_type or "OBJECT").upper()
    ddl = _strip_trailing_semicolons(target_ddl or "")

    if not ddl:
        if cleaned_type == "VIEW":
            return (
                f"CREATE OR REPLACE VIEW {_quote_ident(source_schema)}.{_quote_ident(cleaned_obj)} AS\n"
                "SELECT * FROM "
                f"{_quote_ident(target_schema)}.{_quote_ident(cleaned_obj)};\n"
            )
        return (
            f"-- Missing target DDL for {cleaned_obj}\n"
            f"-- Add CREATE TABLE statement for {_quote_ident(source_schema)}.{_quote_ident(cleaned_obj)}\n"
        )

    rewritten = _rewrite_schema_refs(ddl, source_db, source_schema, target_db, target_schema)

    if cleaned_type == "VIEW" and not re.match(r"^\s*create\s+(or\s+replace\s+)?view\b", rewritten, re.IGNORECASE):
        body = rewritten
        body = re.sub(r"^\s*AS\s+", "", body, flags=re.IGNORECASE)
        return (
            f"CREATE OR REPLACE VIEW {_quote_ident(source_schema)}.{_quote_ident(cleaned_obj)} AS\n"
            f"{body};\n"
        )

    if cleaned_type == "TABLE" and not re.match(r"^\s*create\s+table\b", rewritten, re.IGNORECASE):
        return (
            f"CREATE TABLE IF NOT EXISTS {_quote_ident(source_schema)}.{_quote_ident(cleaned_obj)} AS\n"
            f"{rewritten};\n"
        )

    return _strip_trailing_semicolons(rewritten) + ";\n"


@router.get("/api/prompts")
async def prompts():
    return {"success": True, "prompts": {"diff_classify": DIFF_CLASSIFY_PROMPT_TEMPLATE}}


@router.post("/api/diff/analyze")
async def diff_analyze(payload: DiffAnalyzeRequest):
    try:
        output: Dict[str, Any] = {}

        for item in payload.items[:500]:
            col_diff = _compute_column_diff(item.srcCols or [], item.tgtCols or [])
            ddl_diff = _normalize_for_compare(item.srcDDL or "") != _normalize_for_compare(item.tgtDDL or "")

            if not col_diff["columnDiff"] and not ddl_diff:
                status = "In Sync"
                impact = "Negligible"
                comments = ["No meaningful differences found."]
                categories: List[str] = []
            elif col_diff["columnDiff"] and not ddl_diff:
                status = "Column Difference"
                impact = "Medium"
                comments = [
                    "Column list mismatch detected.",
                    f"Missing in Target: {', '.join(col_diff['missingInTarget']) or 'None'}",
                    f"Missing in Source: {', '.join(col_diff['missingInSource']) or 'None'}",
                ]
                categories = ["columns"]
            else:
                status = "Logic Difference"
                impact = "High"
                comments = ["Definition text differs after normalization."]
                categories = ["other"]

            output[item.key] = {
                "statusType": status,
                "impact": impact,
                "categories": categories,
                "comments": comments,
                "missingInTarget": col_diff["missingInTarget"],
                "missingInSource": col_diff["missingInSource"],
            }

        return {"success": True, "results": output}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/query-columns")
async def query_columns(payload: QueryColumnsRequest):
    try:
        sql = _strip_trailing_semicolons(payload.sql or "")
        if not sql:
            return {"success": False, "error": "Query is empty."}
        if not _is_select_sql(sql):
            return {"success": False, "error": "Only SELECT/WITH queries are allowed."}

        cols = _query_output_columns(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            sql,
        )
        return {"success": True, "columns": cols}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/optimize-query")
async def optimize_query(payload: OptimizeQueryRequest):
    try:
        original = _strip_trailing_semicolons(payload.sql or "")
        if not original:
            return {"success": False, "error": "Query is empty."}
        if not _is_select_sql(original):
            return {"success": False, "error": "Only SELECT/WITH queries are allowed."}

        expected_cols = _query_output_columns(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            original,
        )

        optimized = original
        changes = ["Preserved original query as safe baseline."]
        notes = ["PostgreSQL-first mode keeps output compatibility strict."]
        validation = {
            "columnParity": True,
            "isSameData": True,
            "originalRowCount": None,
            "optimizedRowCount": None,
            "originalMinusOptimized": 0,
            "optimizedMinusOriginal": 0,
        }

        if openai_client:
            prompt = f"""
You are a PostgreSQL SQL optimization assistant.

Rewrite the SQL to improve readability/performance while preserving identical output columns and order.
Rules:
- Return ONLY SQL.
- Must remain a single SELECT or WITH query.
- No DDL or DML.

Original SQL:
{original}
""".strip()
            try:
                candidate = _strip_trailing_semicolons(_call_ai(prompt, max_tokens=1400))
                if _is_select_sql(candidate):
                    _run_compile_check(payload.connection_id, payload.database_name, payload.schema_, candidate)
                    got_cols = _query_output_columns(
                        payload.connection_id,
                        payload.database_name,
                        payload.schema_,
                        candidate,
                    )
                    col_parity = got_cols == expected_cols
                    validation["columnParity"] = col_parity

                    if col_parity:
                        diff = _compare_query_results(
                            payload.connection_id,
                            payload.database_name,
                            payload.schema_,
                            original,
                            candidate,
                        )
                        validation = {
                            "columnParity": True,
                            "isSameData": bool(diff["isSameData"]),
                            "originalRowCount": int(diff["leftRowCount"]),
                            "optimizedRowCount": int(diff["rightRowCount"]),
                            "originalMinusOptimized": int(diff["leftMinusRight"]),
                            "optimizedMinusOriginal": int(diff["rightMinusLeft"]),
                        }

                        if diff["isSameData"]:
                            optimized = candidate
                            changes = ["Applied AI rewrite with exact-result equivalence validation."]
                            notes = [
                                "Validated against PostgreSQL parser, output columns, and EXCEPT ALL diff checks.",
                            ]
                        else:
                            changes = ["Rejected AI rewrite due data mismatch."]
                            notes = [
                                "Returned original query because optimized candidate changed result set.",
                            ]
                    else:
                        changes = ["Rejected AI rewrite due column mismatch."]
                        notes = [
                            "Returned original query because optimized candidate changed output columns.",
                        ]
            except Exception:
                changes = ["Kept original query after optimizer validation failure."]
                notes = [
                    "Candidate failed compile/equivalence checks, so original SQL was returned unchanged.",
                ]

        effect = "Negligible" if optimized == original else "Small"
        return {
            "success": True,
            "optimized": optimized,
            "explain": {
                "estimated_perf_effect": effect,
                "changes": changes,
                "notes": notes,
                "validation": validation,
            },
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/infer-primary-keys")
async def infer_primary_keys(payload: InferPkRequest):
    try:
        sql = _strip_trailing_semicolons(payload.sql or "")
        if not sql:
            return {"success": False, "error": "Query is empty."}
        if not _is_select_sql(sql):
            return {"success": False, "error": "Only SELECT/WITH queries are allowed."}

        cols = _query_output_columns(payload.connection_id, payload.database_name, payload.schema_, sql)
        candidates = _heuristic_pk_candidates(cols)
        return {"success": True, "columns": cols, "candidates": candidates}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/dedup-advanced")
async def dedup_advanced(payload: DedupAdvancedRequest):
    try:
        sql = _strip_trailing_semicolons(payload.sql or "")
        if not sql:
            return {"success": False, "error": "Query is empty."}
        if not _is_select_sql(sql):
            return {"success": False, "error": "Only SELECT/WITH queries are allowed."}

        output_cols = _query_output_columns(payload.connection_id, payload.database_name, payload.schema_, sql)
        resolved_pk = _resolve_pk_columns(output_cols, payload.pkCols or [])

        deduped = _build_dedup_sql(
            sql=sql,
            output_cols=output_cols,
            pk_cols=resolved_pk,
            strategy=payload.strategy or "LATEST_PER_PK",
            hash_exclude_cols=payload.hashExcludeCols or [],
            expose_hash=bool(payload.exposeHash),
            hash_col_name=payload.hashColName or "ROW_HASH",
        )
        _run_compile_check(payload.connection_id, payload.database_name, payload.schema_, deduped)

        source_dup_groups_sql = _build_pk_duplicate_groups_sql(sql, resolved_pk, cte_name="src")
        source_row_count_sql = _build_row_count_sql(sql, cte_name="src")
        dedup_dup_groups_sql = _build_pk_duplicate_groups_sql(deduped, resolved_pk, cte_name="deduped")
        dedup_row_count_sql = _build_row_count_sql(deduped, cte_name="deduped")

        source_dup_groups = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            source_dup_groups_sql,
        )
        source_row_count = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            source_row_count_sql,
        )
        dedup_dup_groups = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            dedup_dup_groups_sql,
        )
        dedup_row_count = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            dedup_row_count_sql,
        )

        target_schema = _normalize_ident(payload.target_schema or "")
        target_table = _normalize_ident(payload.target_table or "")

        insert_sql = ""
        target_overlap_count = None
        would_insert_count = None

        if target_schema and target_table:
            insert_sql = _build_idempotent_insert_sql(
                dedup_select_sql=deduped,
                output_cols=output_cols,
                pk_cols=resolved_pk,
                target_schema=target_schema,
                target_table=target_table,
            )
            _run_explain_check(payload.connection_id, payload.database_name, payload.schema_, insert_sql)

            target_overlap_count = _run_scalar(
                payload.connection_id,
                payload.database_name,
                payload.schema_,
                _build_target_overlap_count_sql(
                    dedup_select_sql=deduped,
                    pk_cols=resolved_pk,
                    target_schema=target_schema,
                    target_table=target_table,
                ),
            )
            would_insert_count = max(dedup_row_count - int(target_overlap_count), 0)

        return {
            "success": True,
            "deduped": deduped,
            "insertSql": insert_sql,
            "checks": {
                "pkColumns": resolved_pk,
                "sourceRowCount": source_row_count,
                "sourceDuplicateGroups": source_dup_groups,
                "dedupRowCount": dedup_row_count,
                "dedupDuplicateGroups": dedup_dup_groups,
                "isDedupValid": dedup_dup_groups == 0,
                "sourceDuplicateCheckSql": source_dup_groups_sql,
                "dedupDuplicateCheckSql": dedup_dup_groups_sql,
                "targetOverlapCount": target_overlap_count,
                "wouldInsertCount": would_insert_count,
            },
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/benchmark-view")
async def benchmark_view(payload: BenchmarkRequest):
    try:
        original = _strip_trailing_semicolons(payload.originalSql or "")
        optimized = _strip_trailing_semicolons(payload.optimizedSql or "")

        if not _is_select_sql(original) or not _is_select_sql(optimized):
            return {"success": False, "error": "Both queries must be SELECT/WITH statements."}

        t0 = time.perf_counter()
        original_count = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            f"SELECT COUNT(*) FROM ({original}) AS q",
        )
        t1 = time.perf_counter()

        t2 = time.perf_counter()
        optimized_count = _run_scalar(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            f"SELECT COUNT(*) FROM ({optimized}) AS q",
        )
        t3 = time.perf_counter()

        diff = _compare_query_results(
            payload.connection_id,
            payload.database_name,
            payload.schema_,
            original,
            optimized,
        )

        return {
            "success": True,
            "original": {"timeMs": int((t1 - t0) * 1000), "rowCount": original_count},
            "optimized": {"timeMs": int((t3 - t2) * 1000), "rowCount": optimized_count},
            "diff": {
                "minusOrigVsOpt": int(diff["leftMinusRight"]),
                "minusOptVsOrig": int(diff["rightMinusLeft"]),
                "isSameData": bool(diff["isSameData"]),
            },
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@router.post("/api/sync/schema")
async def sync_schema(payload: SyncSchemaRequest):
    try:
        source_db = _normalize_ident(payload.source_db)
        source_schema = _normalize_ident(payload.source_schema)
        target_db = _normalize_ident(payload.target_db)
        target_schema = _normalize_ident(payload.target_schema)

        if not source_schema or not target_schema:
            return JSONResponse({"success": False, "error": "source_schema and target_schema are required"}, status_code=400)

        if not payload.items:
            return JSONResponse({"success": False, "error": "items is empty"}, status_code=400)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for item in payload.items:
                obj = _normalize_ident(item.object)
                if not obj:
                    continue

                obj_type = str(item.type or "OBJECT").upper()
                sql_text = _build_sync_sql(
                    source_db=source_db,
                    source_schema=source_schema,
                    target_db=target_db,
                    target_schema=target_schema,
                    obj_name=obj,
                    obj_type=obj_type,
                    target_ddl=item.tgtDDL or "",
                )

                filename = f"{obj_type.lower()}__{_safe_filename(obj)}.sql"
                zip_file.writestr(filename, sql_text)

        buf.seek(0)
        filename = f"sync_schema_{source_schema}__from__{target_schema}.zip"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(buf, media_type="application/zip", headers=headers)
    except Exception as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)


@router.post("/api/sync/objects")
async def sync_objects(payload: SyncObjectsRequest):
    try:
        item = SyncItem(
            object=payload.object,
            type=payload.type,
            tgtDDL=payload.tgtDDL,
        )

        schema_payload = SyncSchemaRequest(
            source_db=payload.source_db,
            source_schema=payload.source_schema,
            target_db=payload.target_db,
            target_schema=payload.target_schema,
            items=[item],
        )
        return await sync_schema(schema_payload)
    except Exception as exc:
        return JSONResponse({"success": False, "error": str(exc)}, status_code=500)
