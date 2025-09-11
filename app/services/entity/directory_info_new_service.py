from datetime import datetime, timezone
from typing import List, Sequence

from app.db.db import Database
from app.db.entities.directory_info_new import DirectoryInfoNew
from app.db.repositories.directory_info_repository_new import DirectoryInfoNewRepository
from app.models.directory.dto import DirectoryDto


class DirectoryInfoNewService:
    """
    Service to manage directory information in the database.
    """

    def __init__(
        self, database: Database, directory_stale_timeout_seconds: int
    ) -> None:
        self.__database = database
        self.__directory_stale_timeout_seconds = directory_stale_timeout_seconds

    def create(self, directory_info: DirectoryInfoNew) -> DirectoryInfoNew:
        """
        Creates a new directory information entry.
        """
        with self.__database.get_db_session() as session:
            session.add(directory_info)
            session.commit()
            session.session.refresh(directory_info)
            return directory_info

    def update(self, directory_info: DirectoryInfoNew) -> DirectoryInfoNew:
        """
        Updates an existing directory information entry.
        """
        with self.__database.get_db_session() as session:
            session.add(directory_info)
            session.commit()
            session.session.refresh(directory_info)
            return directory_info

    def delete(self, directory_info: DirectoryInfoNew) -> None:
        """
        Deletes a directory information entry.
        """
        with self.__database.get_db_session() as session:
            session.delete(directory_info)
            session.commit()


    def get_one_by_id(self, directory_id: str) -> DirectoryInfoNew:
        """
        Retrieves a single directory information entry by its ID.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            return repository.get_by_id(directory_id)

    def get_all(
        self, include_ignored: bool = False, include_deleted: bool = False
    ) -> Sequence[DirectoryInfoNew]:
        """
        Retrieves all directory information entries.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            return repository.get_all(include_deleted=include_deleted, include_ignored=include_ignored)

    def get_all_deleted(self) -> Sequence[DirectoryInfoNew]:
        """
        Retrieves all directories marked as deleted.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            return repository.get_all(include_deleted=True, include_ignored=False)

    def is_ignored_by_id(self, directory_id: str) -> bool:
        """
        Returns whether the directory with the given ID is marked as ignored.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            return repository.is_ignored_by_id(directory_id)

    def get_all_ignored(self) -> Sequence[DirectoryInfoNew]:
        """
        Retrieves all ignored directory entries.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            return repository.get_all_ignored()

    def set_ignored_status(self, directory_id: str, ignored: bool = True) -> None:
        """
        Sets the ignored status of a directory.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoNewRepository)
            repository.set_ignored_status(directory_id, ignored)

    def _hydrate_to_directory_dto(
        self, directory_info: DirectoryInfoNew
    ) -> DirectoryDto:
        return DirectoryDto(
            id=directory_info.id,
            endpoint_address=directory_info.endpoint_address,
            deleted_at=directory_info.deleted_at,
            is_ignored=directory_info.is_ignored,
        )

    def health_check(self) -> bool:
        """
        Checks the health of all directories based on their last successful sync time.
        """

        def is_directory_healthy(directory_info: DirectoryInfoNew) -> bool:
            """
            A directory is considered healthy if it has a last_success_sync timestamp
            and the time since that timestamp is less than the configured stale timeout.
            """
            if not directory_info.last_success_sync:
                return False # Never synced successfully

            time_since_last_success = (
                datetime.now(timezone.utc)
                - directory_info.last_success_sync.astimezone(timezone.utc)
            ).total_seconds()

            return time_since_last_success < self.__directory_stale_timeout_seconds

        directories = self.get_all(include_ignored=False, include_deleted=False)
        return all(
            is_directory_healthy(directory_info) for directory_info in directories
        )

    def get_prometheus_metrics(self) -> List[str]:
        """
        Generates Prometheus metrics for directory information.
        """
        lines = []

        lines.append("# HELP directory_failed_sync_total Total failed syncs")
        lines.append("# TYPE directory_failed_sync_total counter")

        lines.append("# HELP directory_failed_attempts Consecutive failed attempts")
        lines.append("# TYPE directory_failed_attempts gauge")

        lines.append("# HELP directory_last_success_sync Timestamp of last success")
        lines.append("# TYPE directory_last_success_sync gauge")

        lines.append("# HELP directory_is_ignored Is directory ignored")
        lines.append("# TYPE directory_is_ignored gauge")

        lines.append("# HELP directory_is_deleted Timestamp of when directory was or is deleted")
        lines.append("# TYPE directory_is_deleted gauge")

        for s in self.get_all(include_ignored=False, include_deleted=False):
            labels = f'directory_id="{s.directory_id}"'

            lines.append(
                f"directory_failed_sync_total{{{labels}}} {s.failed_sync_count}"
            )
            lines.append(f"directory_failed_attempts{{{labels}}} {s.failed_attempts}")

            if s.last_success_sync:
                ts = int(s.last_success_sync.timestamp())
                lines.append(f"directory_last_success_sync{{{labels}}} {ts}")

            is_ignored = 1 if s.is_ignored else 0
            lines.append(f"directory_is_ignored{{{labels}}} {is_ignored}")

            if s.deleted_at:
                is_deleted = int(s.deleted_at.timestamp())
                lines.append(f"directory_is_deleted{{{labels}}} {is_deleted}")

        return lines
