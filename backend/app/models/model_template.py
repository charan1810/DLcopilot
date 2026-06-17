from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.models.base import Base


class ModelTemplate(Base):
    __tablename__ = "model_templates"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)

    source_schema = Column(String(255), nullable=True)
    target_schema_default = Column(String(255), nullable=True)

    object_type = Column(String(50), nullable=False, default="table")
    table_type = Column(String(50), nullable=True)

    columns_def = Column(JSONB, nullable=True)
    transformations = Column(JSONB, nullable=True)
    business_rules = Column(Text, nullable=True)

    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
