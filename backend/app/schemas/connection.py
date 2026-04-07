from pydantic import BaseModel, Field
from typing import Optional


class ConnectionBase(BaseModel):
    name: str
    db_type: str = Field(..., description="snowflake | postgres | mysql")

    host: Optional[str] = None
    port: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None


class ConnectionCreate(ConnectionBase):
    owner_user_id: int


class ConnectionTest(ConnectionBase):
    pass


class ConnectionResponse(BaseModel):
    id: int
    owner_user_id: int
    name: str
    db_type: str
    host: Optional[str] = None
    port: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True