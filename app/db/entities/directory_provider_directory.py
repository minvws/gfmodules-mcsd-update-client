from datetime import datetime

from sqlalchemy import ForeignKey, Integer, PrimaryKeyConstraint, String, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from app.db.entities.base import Base


class DirectoryProviderDirectory(Base):
    __tablename__ = "directory_provider_directories"
    __table_args__ = (PrimaryKeyConstraint("provider_id", "directory_id"),)

    provider_id: Mapped[int] = mapped_column(
        "provider_id", Integer, ForeignKey("directory_providers.id", ondelete="CASCADE"), nullable=False
    )
    directory_id: Mapped[str] = mapped_column(
        "directory_id", String, ForeignKey("directory_info.id", ondelete="CASCADE"), nullable=False
    )
    first_seen_at: Mapped[datetime] = mapped_column(
        "first_seen_at", TIMESTAMP(timezone=True), nullable=False, default=datetime.now
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        "last_seen_at", TIMESTAMP(timezone=True), nullable=False, default=datetime.now
    )
    removed_at: Mapped[datetime | None] = mapped_column(
        "removed_at", TIMESTAMP(timezone=True), nullable=True, default=None
    )
