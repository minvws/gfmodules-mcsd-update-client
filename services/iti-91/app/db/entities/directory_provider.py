from datetime import datetime

from sqlalchemy import BOOLEAN, Integer, String, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class DirectoryProvider(Base):
    __tablename__ = "directory_providers"

    id: Mapped[int] = mapped_column("id", Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column("url", String, nullable=False, unique=True)
    enabled: Mapped[bool] = mapped_column("enabled", BOOLEAN, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        "created_at", TIMESTAMP(timezone=True), nullable=False, default=datetime.now
    )
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at",
        TIMESTAMP(timezone=True),
        nullable=False,
        default=datetime.now,
        onupdate=datetime.now,
    )
    last_refresh_at: Mapped[datetime | None] = mapped_column(
        "last_refresh_at", TIMESTAMP(timezone=True), nullable=True, default=None
    )
