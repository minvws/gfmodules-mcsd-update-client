from collections.abc import Sequence
from typing import Any
import logging
from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.exc import DatabaseError
from app.db.decorator import repository
from app.db.entities.resource_map import ResourceMap
from app.db.repositories.repository_base import RepositoryBase

logger = logging.getLogger(__name__)


@repository(ResourceMap)
class ResourceMapRepository(RepositoryBase):
    def get(
        self, **kwargs: bool | str | UUID | dict[str, str] | int
    ) -> ResourceMap | None:
        conditions = {k: v for k, v in kwargs.items() if v is not None}
        stmt = select(ResourceMap).filter_by(**conditions)
        return self.db_session.session.execute(stmt).scalars().first()

    def find(
        self, **conditions: bool | str | int | UUID | dict[str, Any] | None
    ) -> Sequence[ResourceMap]:
        conditions = {k: v for k, v in conditions.items() if v is not None}
        filter_conditions = []
        if "supplier_id" in conditions:
            filter_conditions.append(
                ResourceMap.supplier_id == conditions["supplier_id"]
            )

        if "supplier_resource_id" in conditions:
            filter_conditions.append(
                ResourceMap.supplier_resource_id == conditions["supplier_resource_id"]
            )

        if "history_size" in conditions:
            filter_conditions.append(
                ResourceMap.history_size == conditions["history_size"]
            )

        if "consumer_resource_id" in conditions:
            filter_conditions.append(
                ResourceMap.consumer_resource_id == conditions["consumer_resource_id"]
            )

        if "resource_type" in conditions:
            filter_conditions.append(
                ResourceMap.resource_type == conditions["resource_type"]
            )

        return (
            self.db_session.session.execute(
                select(ResourceMap).where(*filter_conditions)
            )
            .scalars()
            .all()
        )

    def create(self, data: ResourceMap) -> ResourceMap:
        try:
            self.db_session.add(data)
            self.db_session.commit()
            return data
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to add organization {data.id}: {e}")
            raise

    def update(self, data: ResourceMap) -> ResourceMap:
        try:
            self.db_session.add(data)
            self.db_session.commit()
            self.db_session.session.refresh(data)
            return data
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to update organization {data.id}: {e}")
            raise

    def delete(self, data: ResourceMap) -> None:
        try:
            self.db_session.delete(data)
            self.db_session.commit()

        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to delete organization {data.id}: {e}")
            raise

    def resource_map_exists(
        self, supplier_resource_id: str, consumer_resource_id: str
    ) -> bool:
        stmt = (
            exists(1)
            .where(
                ResourceMap.supplier_resource_id == supplier_resource_id,
                ResourceMap.consumer_resource_id == consumer_resource_id,
            )
            .select()
        )
        results = self.db_session.session.execute(stmt).scalar()
        if not isinstance(results, bool):
            raise TypeError("Incorrect return from sql statement")

        return results
