import mysql.connector
from app.adapters.base import BaseAdapter


class MySQLAdapter(BaseAdapter):
    def __init__(self, config: dict):
        self.config = config

    def _connect(self):
        return mysql.connector.connect(
            host=self.config["host"],
            port=int(self.config.get("port") or 3306),
            database=self.config["database_name"],
            user=self.config["username"],
            password=self.config["password"],
        )

    def test_connection(self):
        conn = self._connect()
        conn.close()
        return {"status": "success", "message": "MySQL connection successful"}

    def list_schemas(self):
        query = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = [r[0] for r in cur.fetchall()]
            cur.close()
            return results

    def list_objects(self, schema: str):
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (schema,))
            results = [{"name": r[0], "type": r[1]} for r in cur.fetchall()]
            cur.close()
            return results

    def list_columns(self, schema: str, object_name: str):
        query = """
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, (schema, object_name))
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
        query = f"SHOW CREATE TABLE `{schema}`.`{object_name}`"
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            row = cur.fetchone()
            cur.close()
            return row[1] if row else ""

    def run_query(self, sql: str, limit: int = 100):
        wrapped_sql = f"SELECT * FROM ({sql}) AS q LIMIT {int(limit)}"
        with self._connect() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute(wrapped_sql)
            rows = cur.fetchall()
            columns = list(rows[0].keys()) if rows else []
            cur.close()
            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
            }