import snowflake.connector
from app.adapters.base import BaseAdapter


class SnowflakeAdapter(BaseAdapter):
    def __init__(self, config: dict):
        self.config = config

    def _connect(self):
        return snowflake.connector.connect(
            user=self.config["username"],
            password=self.config["password"],
            account=self.config["account"],
            warehouse=self.config.get("warehouse"),
            database=self.config.get("database_name"),
            schema=self.config.get("schema_name"),
            role=self.config.get("role"),
        )

    def test_connection(self):
        conn = self._connect()
        conn.close()
        return {"status": "success", "message": "Snowflake connection successful"}

    def list_schemas(self):
        query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = [r[0] for r in cur.fetchall()]
            cur.close()
            return results

    def list_objects(self, schema: str):
        query = f"""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        ORDER BY TABLE_NAME
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = [{"name": r[0], "type": r[1]} for r in cur.fetchall()]
            cur.close()
            return results

    def list_columns(self, schema: str, object_name: str):
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{object_name}'
        ORDER BY ORDINAL_POSITION
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = [
                {
                    "name": r[0],
                    "data_type": r[1],
                    "nullable": r[2] == "YES",
                    "ordinal_position": r[3],
                }
                for r in cur.fetchall()
            ]
            cur.close()
            return results

    def get_ddl(self, schema: str, object_name: str):
        query = f"SELECT GET_DDL('TABLE', '{schema}.{object_name}')"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            row = cur.fetchone()
            cur.close()
            return row[0] if row else ""

    def run_query(self, sql: str, limit: int = 100):
        wrapped_sql = f"SELECT * FROM ({sql}) LIMIT {int(limit)}"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(wrapped_sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            return {
                "columns": columns,
                "rows": [dict(zip(columns, row)) for row in rows],
                "row_count": len(rows),
            }