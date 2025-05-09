import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.supplier_ignored_directory import SupplierIgnoredDirectory
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)


@repository(SupplierIgnoredDirectory)
class SupplierIgnoredDirectoryRepository(RepositoryBase):
    def get(self, directory_id: str) -> SupplierIgnoredDirectory | None:
        query = select(SupplierIgnoredDirectory).where(SupplierIgnoredDirectory.directory_id == directory_id)
        return self.db_session.session.execute(query).scalars().first()
    
    def get_all(self) -> Sequence[SupplierIgnoredDirectory]:
        query = select(SupplierIgnoredDirectory)
        return self.db_session.session.execute(query).scalars().all()
    
