from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import get_current_user
from app.schemas.metadata import (
    SchemasResponse,
    ObjectsResponse,
    ColumnsResponse,
    DDLResponse,
)
from app.services.metadata_service import MetadataService

router = APIRouter(prefix="/metadata", tags=["Metadata"])


@router.get("/schemas/{connection_id}", response_model=SchemasResponse)
def get_schemas(connection_id: int, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    schemas = MetadataService.list_schemas(db, connection_id)
    return {
        "connection_id": connection_id,
        "schemas": schemas,
    }


@router.get("/objects/{connection_id}", response_model=ObjectsResponse)
def get_objects(
    connection_id: int,
    schema_name: str = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    objects = MetadataService.list_objects(db, connection_id, schema_name)
    return {
        "connection_id": connection_id,
        "schema_name": schema_name,
        "objects": objects,
    }


@router.get("/columns/{connection_id}", response_model=ColumnsResponse)
def get_columns(
    connection_id: int,
    schema_name: str = Query(...),
    object_name: str = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    columns = MetadataService.list_columns(db, connection_id, schema_name, object_name)
    return {
        "connection_id": connection_id,
        "schema_name": schema_name,
        "object_name": object_name,
        "columns": columns,
    }


@router.get("/ddl/{connection_id}", response_model=DDLResponse)
def get_ddl(
    connection_id: int,
    schema_name: str = Query(...),
    object_name: str = Query(...),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    ddl = MetadataService.get_ddl(db, connection_id, schema_name, object_name)
    return {
        "connection_id": connection_id,
        "schema_name": schema_name,
        "object_name": object_name,
        "ddl": ddl,
    }