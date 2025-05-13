from datetime import datetime, timezone
from typing import Dict

from sqlalchemy import (
    String,
    TIMESTAMP,
    PrimaryKeyConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class SupplierIgnoredDirectory(Base):
    __tablename__ = "supplier_ignored_directories"
    __table_args__ = (
        PrimaryKeyConstraint("directory_id"),
    )
    directory_id: Mapped[str] = mapped_column(
        "directory_id",
        String,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at",
        TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.now(tz=timezone.utc),
        server_default=func.now(),
    )
   
    def to_dict(self) -> Dict[str, str]:
        return {
            "id": self.directory_id,
            "ignored_at": self.created_at.isoformat()
        }