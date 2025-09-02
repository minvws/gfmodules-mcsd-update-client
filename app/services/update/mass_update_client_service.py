import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.db.entities.directory_info import DirectoryInfo
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.update.update_client_service import UpdateClientService
from app.stats import Stats

logger = logging.getLogger(__name__)


class MassUpdateClientService:
    def __init__(
        self,
        update_client_service: UpdateClientService,
        directory_provider: DirectoryProvider,
        directory_info_service: DirectoryInfoService,
        ignored_directory_service: IgnoredDirectoryService,
        cleanup_client_directory_after_success_timeout_seconds: int,
        stats: Stats,
        directory_cache_service: DirectoryCacheService,
        cleanup_client_directory_after_directory_delete: bool,
        ignore_directory_after_success_timeout_seconds: int,
        ignore_directory_after_failed_attempts_threshold: int,
    ) -> None:
        self.__directory_provider = directory_provider
        self.__update_client_service = update_client_service
        self.__directory_info_service = directory_info_service
        self.__ignored_directory_service = ignored_directory_service
        self.__cleanup_client_directory_after_success_timeout_seconds = cleanup_client_directory_after_success_timeout_seconds
        self.__stats = stats
        self.__directory_cache_service = directory_cache_service
        self.__cleanup_client_directory_after_directory_delete = cleanup_client_directory_after_directory_delete 
        self.__ignore_directory_after_success_timeout_seconds = ignore_directory_after_success_timeout_seconds
        self.__ignore_directory_after_failed_attempts_threshold = ignore_directory_after_failed_attempts_threshold


    def update_all(self) -> list[dict[str, Any]]:
        with self.__stats.timer("update_all_directories"):
            try:
                all_directories = self.__directory_provider.get_all_directories()
            except Exception as e:
                logging.error(f"Failed to retrieve directories: {e}")
                return []
            data: list[dict[str, Any]] = []
            for directory in all_directories:
                info = self.__directory_info_service.get_directory_info(directory.id)
                new_updated = datetime.now() - timedelta(seconds=60)
                try:
                    data.append(
                        self.__update_client_service.update(
                            directory, info.last_success_sync
                        )
                    )

                    info.last_success_sync = new_updated
                    info.failed_attempts = 0
                    info.last_success_sync = new_updated
                    self.__directory_info_service.update_directory_info(info)
                except Exception as e:
                    logging.error(f"Failed to update directory {directory.id}: {e}")
                    info.failed_attempts += 1
                    info.failed_sync_count += 1
                    self.__directory_info_service.update_directory_info(info)

            return data
    
    def cleanup_old_directories(self) -> None:
        with self.__stats.timer("cleanup_old_directories"):
            directories = self.__directory_info_service.get_all_directories_info(include_ignored=True)

            for directory_info in directories:
                self.__process_directory(directory_info)

            if self.__cleanup_client_directory_after_directory_delete:
                self.__cleanup_deleted_directories()
            else:
                logging.info("Skipping cleanup of deleted directories as per configuration.")

    def __process_directory(self, directory_info: DirectoryInfo) -> None:
        """Process one directory: either synced at least once, or never synced."""
        dir_id = directory_info.directory_id
        is_not_ignored = (
            self.__ignored_directory_service.get_ignored_directory(directory_id=dir_id) is None
        )

        if directory_info.last_success_sync is not None:
            self.__process_successful_directory(directory_info, dir_id, is_not_ignored)
        else:
            self.__process_never_synced_directory(directory_info, dir_id, is_not_ignored)

    def __process_successful_directory(self, directory_info: DirectoryInfo, dir_id: str, is_not_ignored: bool) -> None:
        """Handle directories that have been synced successfully at least once."""
        elapsed_time = (
            datetime.now(timezone.utc) - directory_info.last_success_sync.astimezone(timezone.utc)
        ).total_seconds()

        directory_is_stale = (
            elapsed_time > self.__ignore_directory_after_success_timeout_seconds and is_not_ignored
        )
        directory_is_obsolete = (
            elapsed_time > self.__cleanup_client_directory_after_success_timeout_seconds
        )

        if directory_is_stale:
            logging.warning(
                f"Directory {dir_id} has not been updated for {elapsed_time} seconds, ignoring it."
            )
            self.__ignored_directory_service.add_directory_to_ignore_list(dir_id)

        if directory_is_obsolete:
            logging.info(f"Cleaning up directory for outdated directory {dir_id}")
            self.__delete_data(dir_id)

    def __process_never_synced_directory(self, directory_info: DirectoryInfo, dir_id: str, is_not_ignored: bool) -> None:
        """Handle directories that have never synced successfully."""
        if (
            directory_info.failed_attempts
            >= self.__ignore_directory_after_failed_attempts_threshold
            and is_not_ignored
        ):
            logging.warning(
                f"Directory {dir_id} has failed {directory_info.failed_attempts} times and will be ignored."
            )
            self.__ignored_directory_service.add_directory_to_ignore_list(dir_id)

    def __cleanup_deleted_directories(self) -> None:
        """Delete all directories that have been marked as deleted."""
        deleted_directories = self.__directory_cache_service.get_deleted_directories()
        for directory in deleted_directories:
            logging.info(f"Cleaning up deleted directory {directory.id}")
            self.__delete_data(directory.id)

    def __delete_data(self, directory_id: str) -> None:
        """Delete all data related to a directory."""
        self.__directory_info_service.delete_directory_info(directory_id)
        self.__directory_cache_service.delete_directory_cache(directory_id)
        self.__update_client_service.cleanup(directory_id)