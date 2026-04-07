from sqlalchemy.orm import Session
from app.services.connection_service import ConnectionService
from app.services.adapter_factory import get_adapter
from app.utils.sql_safety import validate_read_only_sql


class QueryService:
    @staticmethod
    def run_query(db: Session, connection_id: int, sql: str, limit: int):
        validate_read_only_sql(sql)

        conn = ConnectionService.get_connection_or_404(db, connection_id)
        config = ConnectionService.build_runtime_config(conn)
        adapter = get_adapter(conn.db_type, config)
        return adapter.run_query(sql, limit)