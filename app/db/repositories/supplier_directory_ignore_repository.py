import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.supplier_directory_ignore import SupplierDirectoryIgnore
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)


@repository(SupplierDirectoryIgnore)
class SupplierDirectoryIgnoreRepository(RepositoryBase):
    def get(self, directory_id: str) -> SupplierDirectoryIgnore | None:
        query = select(SupplierDirectoryIgnore).where(SupplierDirectoryIgnore.directory_id == directory_id)
        return self.db_session.session.execute(query).scalars().first()
    
    def get_all(self) -> Sequence[SupplierDirectoryIgnore]:
        query = select(SupplierDirectoryIgnore)
        return self.db_session.session.execute(query).scalars().all()
    
