from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, List, Optional
from datetime import datetime


class ColumnDefinition(BaseModel):
    name: str
    data_type: str = "VARCHAR(255)"
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default_value: Optional[str] = None
    comment: Optional[str] = None


class ModelCreate(BaseModel):
    name: str
    description: Optional[str] = None

    source_connection_id: Optional[int] = None
    source_schema: Optional[str] = None
    source_tables: Optional[List[str]] = None

    target_connection_id: Optional[int] = None
    target_schema: Optional[str] = None

    object_type: str = "table"           # "table" | "view"
    table_type: Optional[str] = None    # "regular" | "iceberg" | "transient" | "temporary"

    columns_def: Optional[List[Dict[str, Any]]] = None
    transformations: Optional[Dict[str, Any]] = None
    business_rules: Optional[str] = None


class ModelUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

    source_connection_id: Optional[int] = None
    source_schema: Optional[str] = None
    source_tables: Optional[List[str]] = None

    target_connection_id: Optional[int] = None
    target_schema: Optional[str] = None

    object_type: Optional[str] = None
    table_type: Optional[str] = None

    columns_def: Optional[List[Dict[str, Any]]] = None
    transformations: Optional[Dict[str, Any]] = None
    business_rules: Optional[str] = None


class ModelValidateRequest(BaseModel):
    business_rules: Optional[str] = None


class ModelDeployRequest(BaseModel):
    target_connection_id: int
    target_schema: str


class ModelTemplateCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source_schema: Optional[str] = None
    target_schema_default: Optional[str] = None
    object_type: str = "table"
    table_type: Optional[str] = None
    columns_def: Optional[List[Dict[str, Any]]] = None
    transformations: Optional[Dict[str, Any]] = None
    business_rules: Optional[str] = None


class ModelTemplateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: Optional[str] = None
    source_schema: Optional[str] = None
    target_schema_default: Optional[str] = None
    object_type: str
    table_type: Optional[str] = None
    columns_def: Optional[List[Dict[str, Any]]] = None
    transformations: Optional[Dict[str, Any]] = None
    business_rules: Optional[str] = None
    created_by_user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None


class BulkCreateFromSchemaRequest(BaseModel):
    source_connection_id: int
    source_schema: str
    source_password: Optional[str] = None
    target_connection_id: int
    target_schema: str
    template_id: Optional[int] = None
    reference_model_id: Optional[int] = None
    include_objects: Optional[List[str]] = None
    name_prefix: Optional[str] = ""
    name_suffix: Optional[str] = ""


class BulkCreateFromSchemaResponse(BaseModel):
    created_count: int
    skipped_count: int
    created_model_ids: List[int]
    skipped_objects: List[str]


class ModelResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: Optional[str] = None

    source_connection_id: Optional[int] = None
    source_schema: Optional[str] = None
    source_tables: Optional[List[str]] = None

    target_connection_id: Optional[int] = None
    target_schema: Optional[str] = None

    object_type: str
    table_type: Optional[str] = None

    columns_def: Optional[List[Dict[str, Any]]] = None
    transformations: Optional[Dict[str, Any]] = None
    business_rules: Optional[str] = None

    generated_sql: Optional[str] = None
    version: int
    status: str

    acceptance_score: Optional[float] = None
    validation_notes: Optional[str] = None

    created_at: datetime
    updated_at: Optional[datetime] = None
