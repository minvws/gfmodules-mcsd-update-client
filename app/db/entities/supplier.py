from datetime import datetime

from sqlalchemy import TIMESTAMP, PrimaryKeyConstraint, String
from sqlalchemy.orm import mapped_column, Mapped

from app.db.entities.base import Base


class Supplier(Base):
    __tablename__ = "suppliers"
    __table_args__ = (PrimaryKeyConstraint("id"),)

    id: Mapped[str] = mapped_column("id", String(8), nullable=False, unique=True)
    name: Mapped[str] = mapped_column("name", String(150), nullable=False)
    endpoint: Mapped[str] = mapped_column("endpoint", String, nullable=False)
    create_at: Mapped[datetime] = mapped_column(
        "created_at", TIMESTAMP, nullable=False, default=datetime.now()
    )
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at", TIMESTAMP, nullable=False, default=datetime.now()
    )
