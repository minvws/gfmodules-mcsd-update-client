from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import types, String, TIMESTAMP, func, PrimaryKeyConstraint, Integer
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class resource_map(Base):
    __tablename__ = "resource_maps"
    __table_args__ = (PrimaryKeyConstraint("id"),)

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
    resource_type: Mapped[str] = mapped_column("resource_type", String, nullable=False)
    supplier_resource_id: Mapped[str] = mapped_column(
        "supplier_resource_id", String, nullable=False, unique=True
    )
    supplier_resource_version: Mapped[int] = mapped_column(
        "supplier_resource_version", Integer, nullable=False
    )
    consumer_resource_id: Mapped[str] = mapped_column(
        "consumer_resource_id", String, nullable=False, unique=True
    )
    consumer_resource_version: Mapped[int] = mapped_column(
        "consumer_resource_version", Integer, nullable=False
    )
    last_update: Mapped[datetime] = mapped_column(
        "last_update",
        TIMESTAMP(timezone=True),
        nullable=False,
        default=func.now(),
    )
    create_at: Mapped[datetime] = mapped_column("created_at", TIMESTAMP, nullable=False)
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at", TIMESTAMP, nullable=False
    )
