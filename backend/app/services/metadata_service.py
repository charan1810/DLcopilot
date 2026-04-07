from sqlalchemy.orm import Session

from app.services.connection_service import ConnectionService
from app.services.adapter_factory import get_adapter


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