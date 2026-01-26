from typing import TypeVar, Dict, Any

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    def to_dict(self) -> Dict[str, Any]:
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


TBase = TypeVar("TBase", bound=Base, covariant=True)
