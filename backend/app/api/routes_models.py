from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import os

from app.core.database import get_db
from app.core.security import get_current_user, require_role
from app.schemas.data_model import (
    BulkCreateFromSchemaRequest,
    BulkCreateFromSchemaResponse,
    ModelCreate,
    ModelDeployRequest,
    ModelResponse,
    ModelTemplateCreate,
    ModelTemplateResponse,
    ModelUpdate,
    ModelValidateRequest,
)
from app.services.model_service import ModelService

router = APIRouter(prefix="/api/models", tags=["Models"])

_dev_or_admin = require_role("admin", "architect", "developer")


@router.post("", response_model=ModelResponse, status_code=201)
def create_model(
    payload: ModelCreate,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    return ModelService.create_model(db, payload, current_user.id)


@router.get("/templates", response_model=list[ModelTemplateResponse])
def list_model_templates(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    return ModelService.list_templates(db)


@router.post("/templates", response_model=ModelTemplateResponse, status_code=201)
def create_model_template(
    payload: ModelTemplateCreate,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    return ModelService.create_template(db, payload, current_user.id)


@router.post("/bulk-from-schema", response_model=BulkCreateFromSchemaResponse)
def create_models_from_schema(
    payload: BulkCreateFromSchemaRequest,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    return ModelService.create_models_from_schema(db, payload, current_user.id)


@router.get("", response_model=list[ModelResponse])
def list_models(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    return ModelService.list_models(db, current_user.id)


@router.get("/{model_id}", response_model=ModelResponse)
def get_model(
    model_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    return ModelService.get_model(db, model_id, current_user.id)


@router.put("/{model_id}", response_model=ModelResponse)
def update_model(
    model_id: int,
    payload: ModelUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    return ModelService.update_model(db, model_id, payload, current_user.id, current_user.role)


@router.delete("/{model_id}", status_code=204)
def delete_model(
    model_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    ModelService.delete_model(db, model_id, current_user.id, current_user.role)


@router.post("/{model_id}/validate", response_model=ModelResponse)
def validate_model(
    model_id: int,
    payload: ModelValidateRequest,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    openai_client = None
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        from openai import OpenAI
        openai_client = OpenAI(api_key=api_key)
    return ModelService.validate_model(
        db, model_id, payload.business_rules, current_user.id, openai_client
    )


@router.post("/{model_id}/deploy", response_model=ModelResponse)
def deploy_model(
    model_id: int,
    payload: ModelDeployRequest,
    db: Session = Depends(get_db),
    current_user=Depends(_dev_or_admin),
):
    return ModelService.deploy_model(
        db, model_id, payload.target_connection_id, payload.target_schema, current_user.id
    )
