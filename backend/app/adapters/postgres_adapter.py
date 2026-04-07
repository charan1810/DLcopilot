import psycopg2
from app.adapters.base import BaseAdapter


class PostgresAdapter(BaseAdapter):
    def __init__(self, config: dict):
        self.config = config

    def _connect(self):
        return psycopg2.connect(
            host=self.config["host"],
            port=self.config.get("port") or 5432,
            dbname=self.config["database_name"],
            user=self.config["username"],
            password=self.config["password"],
        )

    def test_connection(self):
        conn = self._connect()
        conn.close()
        return {"status": "success", "message": "PostgreSQL connection successful"}

    def list_schemas(self):
        query = """
        SELECT schema_name
        FROM information_schema.schemata
        ORDER BY schema_name
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query)
            return [r[0] for r in cur.fetchall()]

    def list_objects(self, schema: str):
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (schema,))
            return [{"name": r[0], "type": r[1]} for r in cur.fetchall()]

    def list_columns(self, schema: str, object_name: str):
        query = """
        SELECT column_name, data_type, is_nullable, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (schema, object_name))
            return [
                {
                    "name": r[0],
                    "data_type": r[1],
                    "nullable": r[2] == "YES",
                    "ordinal_position": r[3],
                }
                for r in cur.fetchall()
            ]

    def get_ddl(self, schema: str, object_name: str):
        # MVP simplified DDL representation
        cols = self.list_columns(schema, object_name)
        ddl_lines = [f"CREATE TABLE {schema}.{object_name} ("]
        for i, c in enumerate(cols):
            null_part = "" if c["nullable"] else " NOT NULL"
            comma = "," if i < len(cols) - 1 else ""
            ddl_lines.append(f'    {c["name"]} {c["data_type"]}{null_part}{comma}')
        ddl_lines.append(");")
        return "\n".join(ddl_lines)

    def run_query(self, sql: str, limit: int = 100):
        wrapped_sql = f"SELECT * FROM ({sql}) AS q LIMIT {int(limit)}"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(wrapped_sql)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return {
                "columns": columns,
                "rows": [dict(zip(columns, row)) for row in rows],
                "row_count": len(rows),
            }