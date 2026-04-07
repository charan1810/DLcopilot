from pydantic import BaseModel, Field
from typing import Any, List, Dict


class QueryRequest(BaseModel):
    connection_id: int
    sql: str
    limit: int = Field(default=100, ge=1, le=1000)


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int