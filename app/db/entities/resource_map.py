from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    String,
    TIMESTAMP,
    func,
    PrimaryKeyConstraint,
    types,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class ResourceMap(Base):
    __tablename__ = "resource_maps"
    __table_args__ = (
        PrimaryKeyConstraint("id"),
        UniqueConstraint("directory_id", "resource_type", "directory_resource_id"),
    )

    id: Mapped[UUID] = mapped_column(
        "id",
        types.Uuid,
        nullable=False,
        default=uuid4,
    )
    directory_id: Mapped[str] = mapped_column(
        "directory_id",
        String,
        nullable=False,
    )
    resource_type: Mapped[str] = mapped_column("resource_type", String, nullable=False)
    directory_resource_id: Mapped[str] = mapped_column(
        "directory_resource_id", String, nullable=False
    )
    update_client_resource_id: Mapped[str] = mapped_column(
        "update_client_resource_id", String, nullable=False, unique=True
    )
    last_update: Mapped[datetime] = mapped_column(
        "last_update",
        TIMESTAMP(timezone=True),
        nullable=False,
        default=func.now(),
        onupdate=datetime.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        "created_at", TIMESTAMP, nullable=False, default=datetime.now()
    )
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at", TIMESTAMP, nullable=False, default=datetime.now()
    )
