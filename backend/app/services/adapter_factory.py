from fastapi import HTTPException
from app.adapters.snowflake_adapter import SnowflakeAdapter
from app.adapters.postgres_adapter import PostgresAdapter
from app.adapters.mysql_adapter import MySQLAdapter


def get_adapter(db_type: str, config: dict):
    db_type = db_type.lower()

    if db_type == "snowflake":
        return SnowflakeAdapter(config)
    if db_type == "postgres":
        return PostgresAdapter(config)
    if db_type == "mysql":
        return MySQLAdapter(config)

    raise HTTPException(status_code=400, detail=f"Unsupported db_type: {db_type}")