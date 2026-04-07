from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.models.module_access import ModuleAccess
from app.core.constants import SUPPORTED_MODULES


class AccessService:
    @staticmethod
    def grant_module_access(
        db: Session,
        user_id: int,
        module_name: str,
        can_view: bool,
        can_execute: bool,
        can_admin: bool,
    ):
        if module_name not in SUPPORTED_MODULES:
            raise HTTPException(status_code=400, detail="Invalid module name")

        row = (
            db.query(ModuleAccess)
            .filter(ModuleAccess.user_id == user_id, ModuleAccess.module_name == module_name)
            .first()
        )

        if row:
            row.can_view = can_view
            row.can_execute = can_execute
            row.can_admin = can_admin
        else:
            row = ModuleAccess(
                user_id=user_id,
                module_name=module_name,
                can_view=can_view,
                can_execute=can_execute,
                can_admin=can_admin,
            )
            db.add(row)

        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def get_user_access(db: Session, user_id: int):
        return db.query(ModuleAccess).filter(ModuleAccess.user_id == user_id).all()