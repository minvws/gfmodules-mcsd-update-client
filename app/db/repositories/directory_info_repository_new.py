import logging
from typing import List, Sequence
from app.db.entities.directory_info_new import DirectoryInfoNew
from app.db.repositories.repository_base import RepositoryBase
from datetime import datetime, timezone


class DirectoryInfoNewRepository(RepositoryBase):
    """
    Repository for managing DirectoryInfoNew entities.
    """

    def get_by_id(self, id_: str) -> DirectoryInfoNew | None:
        return self.db_session.session.query(DirectoryInfoNew).filter_by(id=id_).first()

    def get_all(
        self, include_deleted: bool = False, include_ignored: bool = False
    ) -> list[DirectoryInfoNew]:
        query = self.db_session.session.query(DirectoryInfoNew)
        if not include_deleted:
            query = query.filter_by(deleted_at=None)
        if not include_ignored:
            query = query.filter_by(is_ignored=False)
        return query.all()

    def get_all_including_ignored_ids(
        self, include_deleted: bool = False, include_ignored_ids: List[str] | None = None
    ) -> Sequence[DirectoryInfoNew]:
        from sqlalchemy import select, or_
        stmt = select(DirectoryInfoNew)
        if not include_deleted:
            stmt = stmt.where(DirectoryInfoNew.deleted_at.is_(None))
        if include_ignored_ids is not None:
            stmt = stmt.where(
                or_(~DirectoryInfoNew.is_ignored, DirectoryInfoNew.id.in_(include_ignored_ids))
            )
        return self.db_session.session.scalars(stmt).all()

    def is_ignored_by_id(self, id_: str) -> bool:
        return (
            self.db_session.session.query(DirectoryInfoNew)
            .filter_by(is_ignored=True, deleted_at=None, id=id_)
            .first()
            is not None
        )

    def get_all_ignored(self) -> Sequence[DirectoryInfoNew]:
        return (
            self.db_session.session.query(DirectoryInfoNew)
            .filter_by(is_ignored=True, deleted_at=None)
            .all()
        )

    def set_ignored_status(self, id_: str, ignored: bool = True) -> DirectoryInfoNew:
        obj = self.get_by_id(id_)
        if obj:
            obj.is_ignored = ignored
            self.db_session.session.commit()
            return obj
        action = "ignored" if ignored else "unignored"
        logging.error(f"Failed to mark directory info {id_} as {action}, id not found")
        raise ValueError(f"Failed to mark as {action}: id not found")
    

    def mark_deleted(self, id_: str) -> DirectoryInfoNew:
        obj = self.get_by_id(id_)
        if obj:
            obj.deleted_at = datetime.now(tz=timezone.utc)
            self.db_session.session.commit()
            return obj
        logging.error(f"Failed to mark directory info {id_} as deleted, id not found")
        raise ValueError("Failed to mark as deleted: id not found")

    def update_last_success_sync(self, id_: str) -> DirectoryInfoNew:
        obj = self.get_by_id(id_)
        if obj:
            obj.last_success_sync = datetime.now(tz=timezone.utc)
            self.db_session.session.commit()
            return obj
        logging.error(
            f"Failed to update last_success_sync for directory info {id_}, id not found"
        )
        raise ValueError("Failed to update last_success_sync: id not found")

  