import logging
from sqlalchemy.exc import DatabaseError
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

    def get_all_ignored(self) -> list[DirectoryInfoNew]:
        return (
            self.db_session.session.query(DirectoryInfoNew)
            .filter_by(is_ignored=True, deleted_at=None)
            .all()
        )

    def mark_ignored(self, id_: str) -> DirectoryInfoNew:
        obj = self.get_by_id(id_)
        if obj:
            obj.is_ignored = True
            self.db_session.session.commit()
            return obj
        logging.error(f"Failed to mark directory info {id_} as ignored, id not found")
        raise ValueError("Failed to mark as ignored: id not found")

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

    def create(self, directory_info: DirectoryInfoNew) -> DirectoryInfoNew:
        try:
            self.db_session.add(directory_info)
            self.db_session.commit()
            return directory_info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to create directory info: {e}")
            raise

    def update(self, directory_info: DirectoryInfoNew) -> DirectoryInfoNew | None:
        try:
            self.db_session.add(directory_info)
            self.db_session.commit()
            self.db_session.session.refresh(directory_info)
            return directory_info
        except DatabaseError as e:
            self.db_session.rollback()
            logging.error(f"Failed to update directory info {directory_info.id}: {e}")
            raise
