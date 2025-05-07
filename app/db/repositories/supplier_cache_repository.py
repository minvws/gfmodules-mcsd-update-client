import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.supplier_cache import SupplierCache
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)

    
@repository(SupplierCache)
class SupplierCacheRepository(RepositoryBase):

    def get(self, supplier_id: str, with_deleted: bool = False) -> SupplierCache | None:
        query = select(SupplierCache).where(SupplierCache.supplier_id == supplier_id)
        if not with_deleted:
            query = query.where(~SupplierCache.is_deleted)
        cache = self.db_session.session.execute(query).scalars().first()
        return cache
    
    def get_all(self, with_deleted: bool = False) -> Sequence[SupplierCache]:
        query = select(SupplierCache)
        if not with_deleted:
            query = query.where(~SupplierCache.is_deleted)
        caches = self.db_session.session.execute(query).scalars().all()
        return caches
