from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey
from app.models.base import Base


class Connection(Base):
    __tablename__ = "connections"

    id = Column(Integer, primary_key=True, index=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    name = Column(String(255), nullable=False)
    db_type = Column(String(50), nullable=False)

    host = Column(String(255), nullable=True)
    port = Column(String(50), nullable=True)
    database_name = Column(String(255), nullable=True)
    schema_name = Column(String(255), nullable=True)

    username = Column(String(255), nullable=True)
    password_encrypted = Column(Text, nullable=True)

    # Snowflake-specific
    account = Column(String(255), nullable=True)
    warehouse = Column(String(255), nullable=True)
    role = Column(String(255), nullable=True)

    is_active = Column(Boolean, default=True, nullable=False)