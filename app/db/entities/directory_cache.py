from sqlalchemy import (
    String,
    PrimaryKeyConstraint,
    types,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class DirectoryCache(Base):
    __tablename__ = "directory_cache"
    __table_args__ = (
        PrimaryKeyConstraint("directory_id"),
    )

    directory_id: Mapped[str] = mapped_column(
        "directory_id",
        String,
        nullable=False,
    )

    # The endpoint URL for the directory
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
   