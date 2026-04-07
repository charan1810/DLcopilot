from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import require_role
from app.schemas.auth import UserResponse, UserUpdate
from app.services.auth_service import AuthService

router = APIRouter(prefix="/admin", tags=["Admin"])

_admin_only = require_role("admin")


@router.get("/users", response_model=list[UserResponse])
def list_users(
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.list_users(db)


@router.put("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: int,
    payload: UserUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.update_user(db, user_id, payload)


@router.delete("/users/{user_id}", response_model=UserResponse)
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(_admin_only),
):
    return AuthService.delete_user(db, user_id)
