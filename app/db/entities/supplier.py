from datetime import datetime

from sqlalchemy import TIMESTAMP, PrimaryKeyConstraint, String
from sqlalchemy.orm import mapped_column, Mapped

from app.db.entities.base import Base


class Supplier(Base):
    __tablename__ = "supplier_endpoints"
    __table_args__ = (PrimaryKeyConstraint("id"),)

    id: Mapped[str] = mapped_column("id", String(8), nullable=False, unique=True)
    care_provider_name: Mapped[str] = mapped_column(
        "care_provider_name", String(150), nullable=False
    )
    update_supplier_endpoint: Mapped[str] = mapped_column(
        "update_supplier_endpoint", String, nullable=False
    )
    create_at: Mapped[datetime] = mapped_column("created_at", TIMESTAMP, nullable=False)
    modified_at: Mapped[datetime] = mapped_column(
        "modified_at", TIMESTAMP, nullable=False
    )
