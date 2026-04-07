from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, UniqueConstraint
from app.models.base import Base


class ModuleAccess(Base):
    __tablename__ = "module_access"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    module_name = Column(String(100), nullable=False)

    can_view = Column(Boolean, default=False, nullable=False)
    can_execute = Column(Boolean, default=False, nullable=False)
    can_admin = Column(Boolean, default=False, nullable=False)

    __table_args__ = (
        UniqueConstraint("user_id", "module_name", name="uq_user_module"),
    )