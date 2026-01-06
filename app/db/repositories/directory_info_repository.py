from typing import List, Sequence
from app.db.entities.directory_info import DirectoryInfo
from app.db.repositories.repository_base import RepositoryBase
from sqlalchemy import select, or_


class DirectoryInfoRepository(RepositoryBase):
    """
    Repository for managing DirectoryInfo entities.
    """
    def exists(self, id_: str) -> bool:
        stmt = select(DirectoryInfo).where(DirectoryInfo.id == id_)
        return self.db_session.session.scalars(stmt).first() is not None

    def get_by_id(self, id_: str) -> DirectoryInfo | None:
        stmt = select(DirectoryInfo).where(DirectoryInfo.id == id_)
        return self.db_session.session.scalars(stmt).first()

    def get_by_endpoint_address(self, endpoint_address: str) -> DirectoryInfo | None:
        stmt = select(DirectoryInfo).where(DirectoryInfo.endpoint_address == endpoint_address)
        return self.db_session.session.scalars(stmt).first()

    def get_all(
        self, include_deleted: bool = False, include_ignored: bool = False
    ) -> Sequence[DirectoryInfo]:
        stmt = select(DirectoryInfo)
        if not include_deleted:
            stmt = stmt.where(DirectoryInfo.deleted_at.is_(None))
        if not include_ignored:
            stmt = stmt.where(DirectoryInfo.is_ignored.is_(False))
        return self.db_session.session.scalars(stmt).all()

    def get_all_including_ignored_ids(
        self, include_deleted: bool = False, include_ignored_ids: List[str] | None = None
    ) -> Sequence[DirectoryInfo]:
        stmt = select(DirectoryInfo)
        if not include_deleted:
            stmt = stmt.where(DirectoryInfo.deleted_at.is_(None))
        if include_ignored_ids is not None:
            stmt = stmt.where(
                or_(~DirectoryInfo.is_ignored, DirectoryInfo.id.in_(include_ignored_ids))
            )
        return self.db_session.session.scalars(stmt).all()

    def is_ignored_by_id(self, id_: str) -> bool:
        stmt = select(DirectoryInfo).where(
            DirectoryInfo.is_ignored.is_(True),
            DirectoryInfo.deleted_at.is_(None),
            DirectoryInfo.id == id_
        )
        return self.db_session.session.scalars(stmt).first() is not None

    def get_all_ignored(self) -> Sequence[DirectoryInfo]:
        stmt = select(DirectoryInfo).where(
            DirectoryInfo.is_ignored.is_(True),
            DirectoryInfo.deleted_at.is_(None)
        )
        return self.db_session.session.scalars(stmt).all()
    
    def get_all_deleted(self) -> Sequence[DirectoryInfo]:
        stmt = select(DirectoryInfo).where(DirectoryInfo.deleted_at.is_not(None))
        return self.db_session.session.scalars(stmt).all()
