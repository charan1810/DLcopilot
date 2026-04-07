from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.security import get_current_user, require_role
from app.schemas.connection import ConnectionCreate, ConnectionTest, ConnectionResponse
from app.services.connection_service import ConnectionService

router = APIRouter(prefix="/connections", tags=["Connections"])

_dev_or_admin = require_role("admin", "developer")


@router.post("/test")
def test_connection(payload: ConnectionTest, current_user=Depends(_dev_or_admin)):
    return ConnectionService.test_connection(payload)


@router.post("/save", response_model=ConnectionResponse)
def save_connection(payload: ConnectionCreate, db: Session = Depends(get_db), current_user=Depends(_dev_or_admin)):
    return ConnectionService.create_connection(db, payload)


@router.get("", response_model=list[ConnectionResponse])
def list_connections(db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    return ConnectionService.list_connections(db)