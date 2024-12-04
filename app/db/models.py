from __future__ import annotations
from typing import List
from pydantic import BaseModel
from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped


class Base(DeclarativeBase):
    pass


class Example(Base):
    __tablename__ = "examples"

    id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    name: Mapped[str] = mapped_column("name", String(100), nullable=False)

    def __repr__(self) -> str:
        return f"<Example(id={self.id}, name={self.name})>"


class Meta(BaseModel):
    limit: int
    offset: int
    total: int


class ExampleResponse(BaseModel):
    meta: Meta
    items: List[str]
