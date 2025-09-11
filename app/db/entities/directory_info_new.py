from datetime import datetime

from sqlalchemy import String, TIMESTAMP, PrimaryKeyConstraint, Integer, BOOLEAN
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class DirectoryInfoNew(Base):
    __tablename__ = "directory_info_new"
    __table_args__ = (PrimaryKeyConstraint("id"),)
    id: Mapped[str] = mapped_column(
        "id",
        String,
        nullable=False,
    )
    endpoint_address: Mapped[str] = mapped_column(
        "endpoint_address",
        String,
        nullable=False,
    )
    failed_sync_count: Mapped[int] = mapped_column(
        "failed_sync_count", Integer, nullable=False
    )
    failed_attempts: Mapped[int] = mapped_column(
        "failed_attempts", Integer, nullable=False
    )
    last_success_sync: Mapped[datetime] = mapped_column(
        "last_success_sync",
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    is_ignored: Mapped[bool] = mapped_column(
        "is_ignored",
        BOOLEAN,
        nullable=False,
        default=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at",
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at",
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    deleted_at: Mapped[datetime] = mapped_column(
        "deleted_at",
        TIMESTAMP(timezone=True),
        nullable=True,
    )
