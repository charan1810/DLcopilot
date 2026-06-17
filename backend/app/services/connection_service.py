from types import SimpleNamespace

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.core.app_store import get_app_store_conn
from app.models.connection import Connection
from app.schemas.connection import ConnectionCreate, ConnectionTest
from app.core.security import encrypt_value, decrypt_value
from app.services.adapter_factory import get_adapter


class ConnectionService:
    @staticmethod
    def test_connection(payload: ConnectionTest):
        config = payload.model_dump()
        adapter = get_adapter(payload.db_type, config)
        return adapter.test_connection()

    @staticmethod
    def create_connection(db: Session, payload: ConnectionCreate):
        conn = Connection(
            owner_user_id=payload.owner_user_id,
            name=payload.name,
            db_type=payload.db_type,
            host=payload.host,
            port=payload.port,
            database_name=payload.database_name,
            schema_name=payload.schema_name,
            username=payload.username,
            password_encrypted=encrypt_value(payload.password),
            account=payload.account,
            warehouse=payload.warehouse,
            role=payload.role,
            is_active=True,
        )
        db.add(conn)
        db.commit()
        db.refresh(conn)
        return conn

    @staticmethod
    def list_connections(db: Session, current_user):
        base = db.query(Connection).filter(Connection.is_active == True)
        if getattr(current_user, "role", "") == "admin":
            return base.all()

        assigned_id = getattr(current_user, "connection_id", None)
        if not assigned_id:
            return []

        return base.filter(Connection.id == assigned_id).all()

    @staticmethod
    def get_connection_or_404(db: Session, connection_id: int):
        conn = db.query(Connection).filter(Connection.id == connection_id, Connection.is_active == True).first()
        if conn:
            return conn

        app_store_conn = get_app_store_conn()
        try:
            cur = app_store_conn.cursor()
            cur.execute("SELECT * FROM connections WHERE id = ? AND is_active = TRUE", (connection_id,))
            row = cur.fetchone()
        finally:
            app_store_conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Connection not found")

        data = dict(row)
        return SimpleNamespace(
            id=data.get("id"),
            owner_user_id=None,
            name=data.get("name"),
            db_type=data.get("db_type"),
            host=data.get("host"),
            port=data.get("port"),
            database_name=data.get("database_name"),
            schema_name=data.get("schema_name"),
            username=data.get("username"),
            password_encrypted=None,
            password=data.get("password"),
            account=data.get("account"),
            warehouse=data.get("warehouse"),
            role=data.get("role"),
            is_active=bool(data.get("is_active", True)),
        )

    @staticmethod
    def build_runtime_config(conn: Connection) -> dict:
        password = getattr(conn, "password", None)
        if password is None and getattr(conn, "password_encrypted", None):
            password = decrypt_value(conn.password_encrypted)

        return {
            "db_type": conn.db_type,
            "host": conn.host,
            "port": conn.port,
            "database_name": conn.database_name,
            "schema_name": conn.schema_name,
            "username": conn.username,
            "password": password,
            "account": conn.account,
            "warehouse": conn.warehouse,
            "role": conn.role,
        }