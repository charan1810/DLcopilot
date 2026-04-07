from sqlalchemy.orm import Session
from fastapi import HTTPException
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
    def list_connections(db: Session):
        return db.query(Connection).filter(Connection.is_active == True).all()

    @staticmethod
    def get_connection_or_404(db: Session, connection_id: int):
        conn = db.query(Connection).filter(Connection.id == connection_id, Connection.is_active == True).first()
        if not conn:
            raise HTTPException(status_code=404, detail="Connection not found")
        return conn

    @staticmethod
    def build_runtime_config(conn: Connection) -> dict:
        return {
            "db_type": conn.db_type,
            "host": conn.host,
            "port": conn.port,
            "database_name": conn.database_name,
            "schema_name": conn.schema_name,
            "username": conn.username,
            "password": decrypt_value(conn.password_encrypted),
            "account": conn.account,
            "warehouse": conn.warehouse,
            "role": conn.role,
        }