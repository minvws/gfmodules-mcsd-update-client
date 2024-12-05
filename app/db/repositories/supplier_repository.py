import logging

from sqlalchemy import select
from sqlalchemy.exc import DatabaseError

from app.db.decorator import repository
from app.db.entities.supplier import Supplier
from app.db.repositories.repository_base import RepositoryBase

logger = logging.getLogger(__name__)


@repository(Supplier)
class SuppliersRepository(RepositoryBase):
    def get(self, supplie_id: str) -> Supplier | None:
        stmt = select(Supplier).where(Supplier.id == supplie_id)
        supplier = self.db_session.session.execute(stmt).scalars().first()
        return supplier

    def create(self, supplier_entity: Supplier) -> Supplier:
        try:
            self.db_session.add(supplier_entity)
            self.db_session.commit()
            return supplier_entity
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to add Supplier: {e}")
            self.db_session.session.rollback()
            raise

    def update(self, supplier_entity: Supplier) -> Supplier:
        try:
            self.db_session.add(supplier_entity)
            self.db_session.commit()
            return supplier_entity
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to update Supplier: {e}")
            self.db_session.session.rollback()
            raise

    def delete(self, supplier_entity: Supplier) -> None:
        try:
            self.db_session.delete(supplier_entity)
            self.db_session.commit()
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to delete Supplier: {e}")
            self.db_session.session.rollback()
            raise
