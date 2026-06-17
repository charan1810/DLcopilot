from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.services.connection_service import ConnectionService
from app.services.adapter_factory import get_adapter


def _pg_quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class MetadataService:
    @staticmethod
    def list_schemas(db: Session, connection_id: int):
        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        adapter = get_adapter(conn.db_type, config)
        return adapter.list_schemas()

    @staticmethod
    def list_objects(db: Session, connection_id: int, schema_name: str):
        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        adapter = get_adapter(conn.db_type, config)
        return adapter.list_objects(schema_name)

    @staticmethod
    def list_columns(db: Session, connection_id: int, schema_name: str, object_name: str):
        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        adapter = get_adapter(conn.db_type, config)
        return adapter.list_columns(schema_name, object_name)

    @staticmethod
    def get_ddl(db: Session, connection_id: int, schema_name: str, object_name: str):
        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        adapter = get_adapter(conn.db_type, config)
        return adapter.get_ddl(schema_name, object_name)

    @staticmethod
    def create_schema(
        db: Session,
        connection_id: int,
        schema_name: str,
        database_name: str = "",
        password: str = "",
    ):
        schema_name = (schema_name or "").strip()
        if not schema_name:
            raise HTTPException(status_code=400, detail="Schema name is required")

        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        if not config.get("password") and password:
            config["password"] = password
        db_type = (conn.db_type or "").lower()

        adapter = get_adapter(conn.db_type, config)

        if db_type == "postgres":
            sql = f"CREATE SCHEMA IF NOT EXISTS {_pg_quote(schema_name)};"
        elif db_type == "mysql":
            sql = f"CREATE DATABASE IF NOT EXISTS `{schema_name.replace('`', '``')}`;"
        else:
            raise HTTPException(status_code=400, detail=f"Schema creation not supported for {db_type} via this API. Use Snowflake's native UI.")

        try:
            adapter.execute_sql(sql)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Schema creation failed: {exc}")

        return {"status": "created", "schema_name": schema_name, "db_type": db_type}