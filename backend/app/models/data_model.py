from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.models.base import Base


class DataModel(Base):
    __tablename__ = "data_models"

    id = Column(Integer, primary_key=True, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # Source configuration
    source_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=True)
    source_schema = Column(String(255), nullable=True)
    source_tables = Column(JSONB, nullable=True)  # list of source table references

    # Target configuration
    target_connection_id = Column(Integer, ForeignKey("connections.id"), nullable=True)
    target_schema = Column(String(255), nullable=True)

    # Object type
    object_type = Column(String(50), nullable=False, default="table")  # "table" | "view"
    table_type = Column(String(50), nullable=True)  # "regular" | "iceberg" | "transient" | "temporary"

    # Model definition
    columns_def = Column(JSONB, nullable=True)       # list of column definitions
    transformations = Column(JSONB, nullable=True)   # join conditions, filters, etc.
    business_rules = Column(Text, nullable=True)     # natural-language requirements

    # Generated DDL
    generated_sql = Column(Text, nullable=True)

    # Lifecycle
    version = Column(Integer, default=1, nullable=False)
    status = Column(String(50), default="draft", nullable=False)  # draft | validated | deployed

    # AI validation result
    acceptance_score = Column(Float, nullable=True)   # 0.0 – 1.0
    validation_notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
