from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    String,
    TIMESTAMP,
    PrimaryKeyConstraint,
    Integer,
    types,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class SupplierInfo(Base):
    __tablename__ = "supplier_info"
    __table_args__ = (
        PrimaryKeyConstraint("id"),
    )

    id: Mapped[UUID] = mapped_column(
        "id",
        types.Uuid,
        nullable=False,
        default=uuid4,
    )
    supplier_id: Mapped[str] = mapped_column(
        "supplier_id",
        String,
        nullable=False,
    )
    # How many total sync failures have happened
    failed_sync_count: Mapped[int] = mapped_column("failed_sync_count", Integer, nullable=False)
    # How many times the sync has failed in a row
    failed_attempts: Mapped[int] = mapped_column("failed_attempts", Integer, nullable=False)
    # The last time the sync succeeded
    last_success_sync: Mapped[datetime] = mapped_column(
        "last_success_sync",
        TIMESTAMP(timezone=True),
        nullable=True,
    )
