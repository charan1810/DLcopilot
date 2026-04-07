from pydantic import BaseModel
from typing import List, Optional


class ObjectInfo(BaseModel):
    name: str
    type: str


class ColumnInfo(BaseModel):
    name: str
    data_type: str
    nullable: bool
    ordinal_position: int


class SchemasResponse(BaseModel):
    connection_id: int
    schemas: List[str]


class ObjectsResponse(BaseModel):
    connection_id: int
    schema_name: str
    objects: List[ObjectInfo]


class ColumnsResponse(BaseModel):
    connection_id: int
    schema_name: str
    object_name: str
    columns: List[ColumnInfo]


class DDLResponse(BaseModel):
    connection_id: int
    schema_name: Optional[str] = None
    object_name: str
    ddl: str