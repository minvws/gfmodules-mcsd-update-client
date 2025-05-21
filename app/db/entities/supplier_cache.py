from sqlalchemy import (
    String,
    PrimaryKeyConstraint,
    types,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class SupplierCache(Base):
    __tablename__ = "supplier_cache"
    __table_args__ = (
        PrimaryKeyConstraint("supplier_id"),
    )

    supplier_id: Mapped[str] = mapped_column(
        "supplier_id",
        String,
        nullable=False,
    )

    ura_number: Mapped[str] = mapped_column(
        "ura_number",
        String,
        nullable=False,
    )

    # The endpoint URL for the supplier
    endpoint: Mapped[str] = mapped_column(
        "endpoint",
        String,
        nullable=False,
    )
    is_deleted: Mapped[bool] = mapped_column(
        "is_deleted",
        types.Boolean,
        nullable=False,
        default=False,
    )
   