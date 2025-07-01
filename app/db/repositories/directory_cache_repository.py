import logging
from typing import Sequence

from app.db.decorator import repository
from app.db.entities.directory_cache import DirectoryCache
from app.db.entities.ignored_directory import IgnoredDirectory
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)

    
@repository(DirectoryCache)
class DirectoryCacheRepository(RepositoryBase):

    def get(self, directory_id: str, with_deleted: bool = False) -> DirectoryCache | None:
        query = select(DirectoryCache).where(DirectoryCache.directory_id == directory_id)
        if not with_deleted:
            query = query.where(~DirectoryCache.is_deleted)
        cache = self.db_session.session.execute(query).scalars().first()
        return cache

    def get_all(self, with_deleted: bool = False, include_ignored: bool = False) -> Sequence[DirectoryCache]:
        query = select(DirectoryCache)
        if not with_deleted: # Exclude deleted caches
            query = query.where(~DirectoryCache.is_deleted)
        if not include_ignored: # Exclude ignored directories
            query = query.outerjoin(
                IgnoredDirectory,
                DirectoryCache.directory_id == IgnoredDirectory.directory_id
            ).where(IgnoredDirectory.directory_id.is_(None))
        caches = self.db_session.session.execute(query).scalars().all()
        return caches
    
    def get_all_include_ignored_ids(self, include_ignored_ids: list[str], with_deleted: bool = False) -> Sequence[DirectoryCache]:
        query = select(DirectoryCache)

        if not with_deleted:
            query = query.where(~DirectoryCache.is_deleted)

        # Left join to identify ignored directories
        query = query.outerjoin(
            IgnoredDirectory,
            DirectoryCache.directory_id == IgnoredDirectory.directory_id
        ).where(
            # Either not ignored or explicitly included
            (IgnoredDirectory.directory_id.is_(None)) |
            (DirectoryCache.directory_id.in_(include_ignored_ids))
        )

        caches = self.db_session.session.execute(query).scalars().all()
        return caches
