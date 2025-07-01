import logging
from typing import Sequence

from sqlalchemy.exc import DatabaseError
from app.db.decorator import repository
from app.db.entities.ignored_directory import IgnoredDirectory
from app.db.entities.directory_info import DirectoryInfo
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select

logger = logging.getLogger(__name__)


@repository(DirectoryInfo)
class DirectoryInfoRepository(RepositoryBase):

    def get(self, directory_id: str) -> DirectoryInfo:
        query = select(DirectoryInfo).filter_by(directory_id=directory_id)
        info = self.db_session.session.execute(query).scalars().first()
        if info is None:
            info = self.create(directory_id)
        return info

    def get_all(self, include_ignored: bool = False) -> Sequence[DirectoryInfo]:
        query = select(DirectoryInfo).order_by(DirectoryInfo.directory_id)
        if not include_ignored:
            query = query.outerjoin(
                IgnoredDirectory,
                DirectoryInfo.directory_id == IgnoredDirectory.directory_id
            ).where(IgnoredDirectory.directory_id.is_(None))
        return self.db_session.session.execute(query).scalars().all()
    
    def get_all_include_ignored_ids(self, include_ignored_ids: list[str]) -> Sequence[DirectoryInfo]:
        query = select(DirectoryInfo).order_by(DirectoryInfo.directory_id)

        query = query.outerjoin(
            IgnoredDirectory,
            DirectoryInfo.directory_id == IgnoredDirectory.directory_id
        ).where(
            (IgnoredDirectory.directory_id.is_(None)) |
            (DirectoryInfo.directory_id.in_(include_ignored_ids))
        )

        return self.db_session.session.execute(query).scalars().all()

    def update(self, info: DirectoryInfo) -> DirectoryInfo:
        try:
            self.db_session.add(info)
            self.db_session.commit()
            self.db_session.session.refresh(info)
            return info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to update directory info {info.directory_id}: {e}")
            raise

    def create(self, directory_id: str) -> DirectoryInfo:
        try:
            info = DirectoryInfo(directory_id=directory_id, failed_sync_count=0, failed_attempts=0)
            self.db_session.add(info)
            self.db_session.commit()
            return info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to add suppler info {info.directory_id}: {e}")
            raise
