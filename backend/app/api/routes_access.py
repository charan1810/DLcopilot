from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import get_current_user, require_role
from app.schemas.access import ModuleAccessCreate
from app.services.access_service import AccessService

router = APIRouter(prefix="/access", tags=["Access"])

_admin_only = require_role("admin")


@router.post("/grant")
def grant_access(payload: ModuleAccessCreate, db: Session = Depends(get_db), current_user=Depends(_admin_only)):
    return AccessService.grant_module_access(
        db=db,
        user_id=payload.user_id,
        module_name=payload.module_name,
        can_view=payload.can_view,
        can_execute=payload.can_execute,
        can_admin=payload.can_admin,
    )


@router.get("/user/{user_id}")
def get_user_access(user_id: int, db: Session = Depends(get_db), current_user=Depends(get_current_user)):
    return AccessService.get_user_access(db, user_id)