from __future__ import annotations

import json
import re
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.app_store import AppStoreOperationalError, get_app_store_conn
from app.models.data_model import DataModel
from app.models.connection import Connection
from app.models.model_template import ModelTemplate
from app.schemas.data_model import ModelCreate, ModelUpdate
from app.services.connection_service import ConnectionService
from app.services.adapter_factory import get_adapter


def _escape_comment(value: str) -> str:
    return str(value).replace("'", "''")


def _normalize_data_type(data_type: str, db_type: str) -> str:
    dt = str(data_type or "VARCHAR(255)").strip()
    if not dt:
        dt = "VARCHAR(255)"

    upper = dt.upper()
    db_type = (db_type or "").lower()

    if db_type == "postgres":
        mapping = {
            "TIMESTAMP_NTZ": "TIMESTAMP",
            "TIMESTAMP_TZ": "TIMESTAMPTZ",
            "VARIANT": "JSONB",
            "OBJECT": "JSONB",
            "ARRAY": "JSONB",
            "BINARY": "BYTEA",
        }
        return mapping.get(upper, dt)

    if db_type == "mysql":
        mapping = {
            "TIMESTAMP_NTZ": "DATETIME",
            "TIMESTAMP_TZ": "TIMESTAMP",
            "VARIANT": "JSON",
            "OBJECT": "JSON",
            "ARRAY": "JSON",
            "BINARY": "BLOB",
        }
        return mapping.get(upper, dt)

    return dt


def _quote_ident(name: str, db_type: str) -> str:
    db_type = (db_type or "").lower()
    n = str(name or "").strip()
    if db_type == "mysql":
        return "`" + n.replace("`", "``") + "`"
    return '"' + n.replace('"', '""') + '"'


def _qualified_name(schema: str, object_name: str, db_type: str) -> str:
    q_name = _quote_ident(object_name, db_type)
    if not schema:
        return q_name
    return f"{_quote_ident(schema, db_type)}.{q_name}"


def _format_source_relation(source_ref: str, source_schema: str, db_type: str) -> str:
    """Qualify simple table names with source schema for view FROM clauses."""
    ref = str(source_ref or "").strip()
    if not ref:
        return ""

    # Keep complex expressions untouched (joins, aliases, subqueries, functions).
    if re.search(r"\s|\(|\)|;", ref):
        return ref

    # Already qualified (schema.table, db.schema.table, quoted chains, etc.).
    if "." in ref:
        return ref

    return _qualified_name(source_schema or "", ref, db_type)


def _load_saved_connection_config(connection_id: int) -> dict:
    conn = get_app_store_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT db_type, host, port, database_name, schema_name,
                   username, password, account, warehouse, role
            FROM connections
            WHERE id = %s AND is_active = TRUE
            """,
            (connection_id,),
        )
        row = cur.fetchone()
    except AppStoreOperationalError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load target connection: {exc}")
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")

    return {
        "db_type": row.get("db_type"),
        "host": row.get("host"),
        "port": row.get("port"),
        "database_name": row.get("database_name"),
        "schema_name": row.get("schema_name"),
        "username": row.get("username"),
        "password": row.get("password") or "",
        "account": row.get("account"),
        "warehouse": row.get("warehouse"),
        "role": row.get("role"),
    }


# ── DDL generator ──────────────────────────────────────────────────────────────

def _generate_ddl(model: DataModel, db_type: str = "") -> str:
    """Build a DDL SQL string from the model definition."""
    object_name = _qualified_name(model.target_schema or "", model.name, db_type)
    cols = model.columns_def or []

    if model.object_type == "view":
        select_cols = ", ".join(_quote_ident(c["name"], db_type) for c in cols) if cols else "*"
        transformations = model.transformations if isinstance(model.transformations, dict) else {}
        custom_from = str(transformations.get("view_from_clause", "") or "").strip()
        if custom_from:
            from_clause = re.sub(r";+\s*$", "", custom_from)
        else:
            src_tables = model.source_tables or []
            formatted_sources = [
                _format_source_relation(t, model.source_schema or "", db_type)
                for t in src_tables
                if str(t or "").strip()
            ]
            from_clause = ", ".join(formatted_sources) if formatted_sources else "/* source_table */"
        return (
            f"CREATE OR REPLACE VIEW {object_name} AS\n"
            f"SELECT {select_cols}\n"
            f"FROM {from_clause};"
        )

    # Table variants
    prefix_map = {
        "iceberg": "CREATE ICEBERG TABLE",
        "transient": "CREATE TRANSIENT TABLE",
        "temporary": "CREATE TEMPORARY TABLE",
    }
    # PostgreSQL does not support CREATE OR REPLACE TABLE.
    ddl_prefix = prefix_map.get(model.table_type or "regular", "CREATE TABLE IF NOT EXISTS")

    col_lines: list[str] = []
    pk_cols: list[str] = []
    for col in cols:
        parts = [
            _quote_ident(col.get("name", "col"), db_type),
            _normalize_data_type(col.get("data_type", "VARCHAR(255)"), db_type),
        ]
        if not col.get("nullable", True):
            parts.append("NOT NULL")
        if col.get("unique"):
            parts.append("UNIQUE")
        if col.get("default_value"):
            parts.append(f"DEFAULT {col['default_value']}")
        if col.get("comment") and (db_type or "").lower() == "mysql":
            parts.append(f"COMMENT '{_escape_comment(col['comment'])}'")
        col_lines.append("    " + " ".join(parts))
        if col.get("primary_key"):
            pk_cols.append(_quote_ident(col.get("name", ""), db_type))

    if pk_cols:
        col_lines.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

    cols_sql = ",\n".join(col_lines) if col_lines else "    id INTEGER"

    return f"{ddl_prefix} {object_name} (\n{cols_sql}\n);"


# ── Service ────────────────────────────────────────────────────────────────────

class ModelService:

    @staticmethod
    def list_templates(db: Session) -> list[ModelTemplate]:
        return db.query(ModelTemplate).order_by(ModelTemplate.created_at.desc()).all()

    @staticmethod
    def create_template(db: Session, payload, user_id: int) -> ModelTemplate:
        name = (payload.name or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Template name is required")

        existing = db.query(ModelTemplate).filter(ModelTemplate.name == name).first()
        if existing:
            raise HTTPException(status_code=400, detail="Template name already exists")

        tmpl = ModelTemplate(
            name=name,
            description=payload.description,
            source_schema=payload.source_schema,
            target_schema_default=payload.target_schema_default,
            object_type=payload.object_type or "table",
            table_type=payload.table_type,
            columns_def=payload.columns_def,
            transformations=payload.transformations,
            business_rules=payload.business_rules,
            created_by_user_id=user_id,
        )
        db.add(tmpl)
        db.commit()
        db.refresh(tmpl)
        return tmpl

    @staticmethod
    def create_models_from_schema(db: Session, payload, user_id: int):
        source_schema = (payload.source_schema or "").strip()
        target_schema = (payload.target_schema or "").strip()
        if not source_schema or not target_schema:
            raise HTTPException(status_code=400, detail="Source and target schemas are required")

        include_objects: list[str] = []
        for obj_name in (payload.include_objects or []):
            name = str(obj_name or "").strip()
            if name and name not in include_objects:
                include_objects.append(name)

        try:
            src_conn = ConnectionService.get_connection_or_404(db, payload.source_connection_id)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to load source connection: {exc}")

        # Validate target connection early so client errors are explicit.
        try:
            ConnectionService.get_connection_or_404(db, payload.target_connection_id)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to load target connection: {exc}")

        # DataModel has foreign keys to SQLAlchemy `connections` table.
        # Some legacy/fallback lookups can resolve from app_store only; block those early.
        src_conn_row = db.query(Connection.id).filter(Connection.id == payload.source_connection_id, Connection.is_active == True).first()
        tgt_conn_row = db.query(Connection.id).filter(Connection.id == payload.target_connection_id, Connection.is_active == True).first()
        if not src_conn_row or not tgt_conn_row:
            raise HTTPException(
                status_code=400,
                detail="Selected source/target connection is inactive in model store. Re-save the connection and retry.",
            )

        template = None
        if payload.template_id:
            template = db.query(ModelTemplate).filter(ModelTemplate.id == payload.template_id).first()
            if not template:
                raise HTTPException(status_code=404, detail="Template not found")

        # Reference model: use an existing DataModel as a structural template.
        # Takes priority over a ModelTemplate when both are supplied.
        ref_model = None
        if payload.reference_model_id:
            ref_model = db.query(DataModel).filter(DataModel.id == payload.reference_model_id).first()
            if not ref_model:
                raise HTTPException(status_code=404, detail="Reference model not found")

        src_adapter = None
        # Fast path for reference mode: when objects are explicitly provided,
        # we do not need to query source metadata or read source columns.
        if ref_model and include_objects:
            table_names = include_objects
        else:
            try:
                src_cfg = ConnectionService.build_runtime_config(src_conn)
                if not src_cfg.get("password") and getattr(payload, "source_password", None):
                    src_cfg["password"] = payload.source_password
                src_adapter = get_adapter(src_conn.db_type, src_cfg)
                all_objects = src_adapter.list_objects(source_schema)
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to read source schema '{source_schema}': {exc}",
                )

            table_names = [
                obj.get("name")
                for obj in all_objects
                if obj.get("name") and str(obj.get("type", "")).upper() in {"BASE TABLE", "TABLE"}
            ]
            if include_objects:
                include_set = set(include_objects)
                table_names = [t for t in table_names if t in include_set]

        name_prefix = payload.name_prefix or ""
        name_suffix = payload.name_suffix or ""

        created_ids: list[int] = []
        skipped_objects: list[str] = []

        for source_table in table_names:
            model_name = f"{name_prefix}{source_table}{name_suffix}".strip()
            if not model_name:
                skipped_objects.append(source_table)
                continue

            exists = db.query(DataModel).filter(DataModel.name == model_name, DataModel.target_schema == target_schema).first()
            if exists:
                skipped_objects.append(source_table)
                continue

            # When a reference model is selected, use its column structure directly —
            # do NOT read columns from the source schema for this table.
            # The source schema is only used to enumerate which table names to create.
            if ref_model:
                import copy
                columns_def = copy.deepcopy(ref_model.columns_def or [])
                object_type = ref_model.object_type or "table"
                table_type = ref_model.table_type or "regular"
                business_rules = ref_model.business_rules
                transformations = ref_model.transformations
                description = ref_model.description
            else:
                try:
                    src_cols = src_adapter.list_columns(source_schema, source_table)
                except Exception as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Failed to read columns for {source_schema}.{source_table}: {exc}",
                    )
                columns_def = [
                    {
                        "name": c.get("name"),
                        "data_type": c.get("data_type") or "VARCHAR(255)",
                        "nullable": bool(c.get("nullable", True)),
                        "primary_key": False,
                        "unique": False,
                        "default_value": None,
                        "comment": None,
                    }
                    for c in src_cols
                    if c.get("name")
                ]

                object_type = template.object_type if template else "table"
                table_type = template.table_type if template else "regular"
                business_rules = template.business_rules if template else None
                transformations = template.transformations if template else None

                if template and template.columns_def:
                    templated_by_name = {
                        str(col.get("name", "")).lower(): col
                        for col in (template.columns_def or [])
                        if col.get("name")
                    }
                    for col in columns_def:
                        tcol = templated_by_name.get(str(col.get("name", "")).lower())
                        if not tcol:
                            continue
                        col["data_type"] = tcol.get("data_type") or col["data_type"]
                        col["nullable"] = bool(tcol.get("nullable", col["nullable"]))
                        col["primary_key"] = bool(tcol.get("primary_key", False))
                        col["unique"] = bool(tcol.get("unique", False))
                        col["default_value"] = tcol.get("default_value")
                        col["comment"] = tcol.get("comment")

                description = template.description if template else None

            model = DataModel(
                owner_user_id=user_id,
                name=model_name,
                description=description,
                source_connection_id=payload.source_connection_id,
                source_schema=source_schema,
                source_tables=[source_table],
                target_connection_id=payload.target_connection_id,
                target_schema=target_schema,
                object_type=object_type,
                table_type=table_type,
                columns_def=columns_def,
                transformations=transformations,
                business_rules=business_rules,
            )
            model.generated_sql = _generate_ddl(model)
            db.add(model)
            db.flush()
            created_ids.append(model.id)

        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise HTTPException(
                status_code=400,
                detail="Bulk create failed due to an invalid source/target connection reference. Re-select active connections and retry.",
            ) from exc
        return {
            "created_count": len(created_ids),
            "skipped_count": len(skipped_objects),
            "created_model_ids": created_ids,
            "skipped_objects": skipped_objects,
        }

    @staticmethod
    def create_model(db: Session, payload: ModelCreate, user_id: int) -> DataModel:
        model = DataModel(
            owner_user_id=user_id,
            **payload.model_dump(),
        )
        model.generated_sql = _generate_ddl(model)
        db.add(model)
        db.commit()
        db.refresh(model)
        return model

    @staticmethod
    def list_models(db: Session, user_id: int = None) -> list[DataModel]:
        """Return all models visible to the team (not filtered by owner)."""
        return (
            db.query(DataModel)
            .order_by(DataModel.created_at.desc())
            .all()
        )

    @staticmethod
    def get_model(db: Session, model_id: int, user_id: int = None) -> DataModel:
        """Fetch a model by id. Any authenticated user may view any model."""
        model = db.query(DataModel).filter(DataModel.id == model_id).first()
        if not model:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @staticmethod
    def update_model(
        db: Session, model_id: int, payload: ModelUpdate, user_id: int, user_role: str = ""
    ) -> DataModel:
        model = ModelService.get_model(db, model_id)
        if model.owner_user_id != user_id and user_role not in ("admin", "architect"):
            raise HTTPException(status_code=403, detail="Only the model owner, admin, or architect can edit this model")
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(model, field, value)
        model.version = (model.version or 1) + 1
        model.generated_sql = _generate_ddl(model)
        db.commit()
        db.refresh(model)
        return model

    @staticmethod
    def delete_model(db: Session, model_id: int, user_id: int, user_role: str = "") -> None:
        model = ModelService.get_model(db, model_id)
        if model.owner_user_id != user_id and user_role not in ("admin", "architect"):
            raise HTTPException(status_code=403, detail="Only the model owner, admin, or architect can delete this model")
        db.delete(model)
        db.commit()

    @staticmethod
    def validate_model(
        db: Session,
        model_id: int,
        business_rules: Optional[str],
        user_id: int,
        openai_client,
    ) -> DataModel:
        model = ModelService.get_model(db, model_id, user_id)

        if business_rules:
            model.business_rules = business_rules
        # Always regenerate DDL before validation
        model.generated_sql = _generate_ddl(model)

        if not openai_client:
            model.acceptance_score = None
            model.validation_notes = "AI validation unavailable: OpenAI API key not configured."
            model.status = "validated"
            db.commit()
            db.refresh(model)
            return model

        prompt = f"""You are a senior data engineering expert. Evaluate this data model against the stated business requirements.

Business Requirements:
{model.business_rules or 'No specific requirements provided.'}

Model Definition:
- Name        : {model.name}
- Type        : {model.object_type} ({model.table_type or 'regular'})
- Description : {model.description or 'N/A'}
- Source tables: {', '.join(model.source_tables or []) or 'N/A'}
- Columns     : {json.dumps(model.columns_def or [], indent=2)}

Generated DDL:
{model.generated_sql}

Score from 0 to 100 across:
1. Completeness  – all business-critical columns present?
2. Data types    – appropriate types and sizes?
3. Business alignment – matches stated requirements?
4. Best practices – naming, PKs, nullability, comments?

Return ONLY a JSON object like:
{{"score": <integer 0-100>, "notes": "<concise explanation, max 3 sentences>"}}"""

        try:
            response = openai_client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.15,
            )
            raw = response.choices[0].message.content.strip()
            json_match = re.search(r"\{.*\}", raw, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                model.acceptance_score = float(result.get("score", 0)) / 100.0
                model.validation_notes = result.get("notes", "")
        except Exception as exc:
            model.acceptance_score = None
            model.validation_notes = f"Validation error: {exc}"

        model.status = "validated"
        db.commit()
        db.refresh(model)
        return model

    @staticmethod
    def deploy_model(
        db: Session,
        model_id: int,
        target_connection_id: int,
        target_schema: str,
        user_id: int,
    ) -> DataModel:
        model = ModelService.get_model(db, model_id, user_id)
        schema = (target_schema or "").strip()
        if not schema:
            raise HTTPException(status_code=400, detail="Target schema is required")

        config = _load_saved_connection_config(target_connection_id)
        db_type = str(config.get("db_type") or "").lower()

        model.target_connection_id = target_connection_id
        model.target_schema = schema
        model.generated_sql = _generate_ddl(model, db_type)

        try:
            table_type = str(model.table_type or "regular").lower()

            if model.object_type == "table" and db_type in {"postgres", "mysql"} and table_type in {"iceberg", "transient"}:
                raise HTTPException(
                    status_code=400,
                    detail=f"Table type '{table_type}' is not supported for {db_type}. Use 'regular' or 'temporary'.",
                )

            adapter = get_adapter(config.get("db_type", ""), config)
            adapter.execute_sql(model.generated_sql)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Deployment failed: {exc}")

        model.status = "deployed"
        db.commit()
        db.refresh(model)
        return model
