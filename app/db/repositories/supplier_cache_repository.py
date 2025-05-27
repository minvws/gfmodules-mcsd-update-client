import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.supplier_cache import SupplierCache
from app.db.entities.supplier_ignored_directory import SupplierIgnoredDirectory
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

    def get_all(self, with_deleted: bool = False, include_ignored: bool = False) -> Sequence[SupplierCache]:
        query = select(SupplierCache)
        if not with_deleted: # Exclude deleted caches
            query = query.where(~SupplierCache.is_deleted)
        if not include_ignored: # Exclude ignored directories
            query = query.outerjoin(
                SupplierIgnoredDirectory,
                SupplierCache.supplier_id == SupplierIgnoredDirectory.directory_id
            ).where(SupplierIgnoredDirectory.directory_id.is_(None))
        caches = self.db_session.session.execute(query).scalars().all()
        return caches
    
    def get_all_include_ignored_ids(self, include_ignored_ids: list[str], with_deleted: bool = False) -> Sequence[SupplierCache]:
        query = select(SupplierCache)

        if not with_deleted:
            query = query.where(~SupplierCache.is_deleted)

        # Left join to identify ignored suppliers
        query = query.outerjoin(
            SupplierIgnoredDirectory,
            SupplierCache.supplier_id == SupplierIgnoredDirectory.directory_id
        ).where(
            # Either not ignored or explicitly included
            (SupplierIgnoredDirectory.directory_id.is_(None)) |
            (SupplierCache.supplier_id.in_(include_ignored_ids))
        )

        caches = self.db_session.session.execute(query).scalars().all()
        return caches
