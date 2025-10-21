from datetime import datetime

from sqlalchemy import String, TIMESTAMP, PrimaryKeyConstraint, Integer, BOOLEAN
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base
from app.models.directory.dto import DirectoryDto


class DirectoryInfo(Base):
    __tablename__ = "directory_info"
    __table_args__ = (PrimaryKeyConstraint("id"),)
    id: Mapped[str] = mapped_column(
        "id",
        String,
        nullable=False,
    )
    ura: Mapped[str] = mapped_column(
        "ura",
        String,
        default="",
        nullable=False,
    )
    endpoint_address: Mapped[str] = mapped_column(
        "endpoint_address",
        String,
        nullable=False,
    )
    failed_sync_count: Mapped[int] = mapped_column(
        "failed_sync_count", Integer, nullable=False, default=0
    )
    failed_attempts: Mapped[int] = mapped_column(
        "failed_attempts", Integer, nullable=False, default=0
    )
    last_success_sync: Mapped[datetime | None] = mapped_column(
        "last_success_sync",
        TIMESTAMP(timezone=True),
        nullable=True,
        default=None,
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
        default=datetime.now()
    )
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at",
        TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.now(),
        onupdate=datetime.now(),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        "deleted_at",
        TIMESTAMP(timezone=True),
        nullable=True,
        default=None,
    )

    def to_dto(self) -> DirectoryDto:
        return DirectoryDto(
            id=self.id,
            ura=self.ura,
            endpoint_address=self.endpoint_address,
            deleted_at=self.deleted_at,
            is_ignored=self.is_ignored,
            failed_sync_count=self.failed_sync_count,
            failed_attempts=self.failed_attempts,
            last_success_sync=self.last_success_sync,
        )
