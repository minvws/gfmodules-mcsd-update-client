from datetime import datetime, timezone
import logging
from typing import List

from app.db.db import Database
from app.db.entities.directory_info import DirectoryInfo
from app.db.repositories.directory_info_repository import DirectoryInfoRepository
from app.models.directory.dto import DirectoryDto

logger = logging.getLogger(__name__)

class DirectoryInfoService:
    """
    Service to manage directory information in the database.
    """

    def __init__(
        self, database: Database, directory_stale_timeout_seconds: int
    ) -> None:
        self.__database = database
        self.__directory_stale_timeout_seconds = directory_stale_timeout_seconds

    def update(self, directory_dto: DirectoryDto) -> DirectoryDto:
        """
        Updates an existing directory information entry.
        """
        with self.__database.get_db_session() as session:
            # Get existing directory info
            repository = session.get_repository(DirectoryInfoRepository)
            directory_info = repository.get_by_id(directory_dto.id)
            if directory_info is None:
                directory_info = DirectoryInfo(
                    id=directory_dto.id,
                    endpoint_address=directory_dto.endpoint_address,
                )
            else:
                directory_info.endpoint_address = directory_dto.endpoint_address
            directory_info.deleted_at = directory_dto.deleted_at
            directory_info.is_ignored = directory_dto.is_ignored
            directory_info.failed_sync_count = directory_dto.failed_sync_count
            directory_info.failed_attempts = directory_dto.failed_attempts
            directory_info.last_success_sync = directory_dto.last_success_sync

            session.add(directory_info)
            session.commit()
            session.session.refresh(directory_info)
            return directory_info.to_dto()
    

    def delete(self, directory_id: str) -> None:
        """
        Deletes a directory information entry.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directory_info = repository.get_by_id(directory_id)
            if directory_info is None:
                raise ValueError(f"Directory with ID {directory_id} not found")
            session.delete(directory_info)
            session.commit()


    def get_one_by_id(self, directory_id: str) -> DirectoryDto:
        """
        Retrieves a single directory information entry by its ID.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directory = repository.get_by_id(directory_id)
        if directory is None:
            raise ValueError(f"Directory with ID {directory_id} not found")
        return directory.to_dto()

    def get_all(
        self, include_ignored: bool = False, include_deleted: bool = False
    ) -> List[DirectoryDto]:
        """
        Retrieves all directory information entries.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directories = repository.get_all(include_deleted=include_deleted, include_ignored=include_ignored)
            return [directory.to_dto() for directory in directories]
    
    def get_all_including_ignored_ids(
        self, include_ignored_ids: List[str] | None = None, include_deleted: bool = False
    ) -> List[DirectoryDto]:
        """
        Retrieves all directory information entries, including those with IDs in the ignored list.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directories = repository.get_all_including_ignored_ids(include_deleted=include_deleted, include_ignored_ids=include_ignored_ids)
            return [directory.to_dto() for directory in directories]

    def get_all_deleted(self) -> List[DirectoryDto]:
        """
        Retrieves all directories marked as deleted.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directories = repository.get_all_deleted()
            return [directory.to_dto() for directory in directories]

    def get_all_ignored(self) -> List[DirectoryDto]:
        """
        Retrieves all ignored directory entries.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directories = repository.get_all_ignored()
            return [directory.to_dto() for directory in directories]

    def set_ignored_status(self, directory_id: str, ignored: bool = True) -> None:
        """
        Sets the ignored status of a directory.
        """
        directory_dto = self.get_one_by_id(directory_id)
        directory_dto.is_ignored = ignored
        self.update(directory_dto)

    def health_check(self) -> bool:
        """
        Checks the health of all directories based on their last successful sync time.
        """
        def is_directory_healthy(directory_dto: DirectoryDto) -> bool:
            """
            A directory is considered healthy if it has a last_success_sync timestamp
            and the time since that timestamp is less than the configured stale timeout.
            """
            if not directory_dto.last_success_sync:
                return False # Never synced successfully

            time_since_last_success = (
                datetime.now(tz=timezone.utc)
                - directory_dto.last_success_sync.astimezone(timezone.utc)
            ).total_seconds()

            return time_since_last_success < self.__directory_stale_timeout_seconds

        directories = self.get_all(include_deleted=False, include_ignored=False)
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

        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryInfoRepository)
            directories = repository.get_all(include_deleted=False, include_ignored=False)
            
            for s in directories:
                labels = f'directory_id="{s.id}"'

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
