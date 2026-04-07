from pydantic import BaseModel


class ModuleAccessCreate(BaseModel):
    user_id: int
    module_name: str
    can_view: bool = False
    can_execute: bool = False
    can_admin: bool = False