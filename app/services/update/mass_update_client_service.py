import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.models.directory.dto import DirectoryDto
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
        mark_client_directory_as_deleted_after_success_timeout_seconds: int,
        stats: Stats,
        mark_client_directory_as_deleted_after_lrza_delete: bool,
        ignore_client_directory_after_success_timeout_seconds: int,
        ignore_client_directory_after_failed_attempts_threshold: int,
    ) -> None:
        self.__directory_provider = directory_provider
        self.__update_client_service = update_client_service
        self.__directory_info_service = directory_info_service
        self.__mark_client_directory_as_deleted_after_success_timeout_seconds = mark_client_directory_as_deleted_after_success_timeout_seconds
        self.__stats = stats
        self.__mark_client_directory_as_deleted_after_lrza_delete = mark_client_directory_as_deleted_after_lrza_delete 
        self.__ignore_client_directory_after_success_timeout_seconds = ignore_client_directory_after_success_timeout_seconds
        self.__ignore_client_directory_after_failed_attempts_threshold = ignore_client_directory_after_failed_attempts_threshold


    def update_all(self) -> list[dict[str, Any]]:
        with self.__stats.timer("update_all_directories"):
            try:
                all_directories = self.__directory_provider.get_all_directories()
            except Exception as e:
                logging.error(f"Failed to retrieve directories: {e}")
                return []
            data: list[dict[str, Any]] = []
            for directory in all_directories:
                info = self.__directory_info_service.get_one_by_id(directory.id)
                new_updated = datetime.now() - timedelta(seconds=60)
                try:
                    data.append(
                        self.__update_client_service.update(
                            directory, info.last_success_sync
                        )
                    )

                    info.last_success_sync = new_updated # Update last success time
                    info.failed_attempts = 0 # Reset on success
                    self.__directory_info_service.update(directory_id=info.id, last_success_sync=info.last_success_sync, failed_attempts=info.failed_attempts)
                except Exception as e:
                    logging.error(f"Failed to update directory {directory.id}: {e}")
                    info.failed_attempts += 1 # Increment failed attempts
                    info.failed_sync_count += 1 # Increment total failed sync count
                    self.__directory_info_service.update(directory_id=info.id, failed_attempts=info.failed_attempts, failed_sync_count=info.failed_sync_count)

            return data
    
    def cleanup_old_directories(self) -> None:
        with self.__stats.timer("cleanup_old_directories"):
            directories = self.__directory_info_service.get_all(include_ignored=True, include_deleted=True)

            for directory_info in directories:
                self.__process_directory(directory_info)

            if self.__mark_client_directory_as_deleted_after_lrza_delete:
                self.__cleanup_deleted_directories()
            else:
                logging.info("Skipping cleanup of deleted directories as per configuration.")

    def __process_directory(self, directory: DirectoryDto) -> None:
        """Process one directory: either synced at least once, or never synced."""
        dir_id = directory.id
        is_not_ignored = self.__directory_info_service.get_one_by_id(directory_id=dir_id).is_ignored is False

        if directory.last_success_sync is not None:
            self.__process_successful_directory(directory, dir_id, is_not_ignored)
        else:
            self.__process_never_synced_directory(directory, dir_id, is_not_ignored)

    def __process_successful_directory(self, directory: DirectoryDto, dir_id: str, is_not_ignored: bool) -> None:
        """Handle directories that have been synced successfully at least once."""
        assert directory.last_success_sync is not None  # Guaranteed by caller
        elapsed_time = (
            datetime.now(tz=timezone.utc) - directory.last_success_sync.astimezone(timezone.utc)
        ).total_seconds()

        directory_is_stale = (
            elapsed_time > self.__ignore_client_directory_after_success_timeout_seconds and is_not_ignored
        )
        directory_is_outdated = (
            elapsed_time > self.__mark_client_directory_as_deleted_after_success_timeout_seconds
        )

        if directory_is_stale:
            logging.warning(
                f"Directory {dir_id} has not been updated for {elapsed_time} seconds, ignoring it."
            )
            self.__directory_info_service.set_ignored_status(dir_id, True)

        if directory_is_outdated:
            logging.info(f"Setting delete_at for outdated directory {dir_id}")
            self.__directory_info_service.set_deleted_at(dir_id)

    def __process_never_synced_directory(self, directory: DirectoryDto, dir_id: str, is_not_ignored: bool) -> None:
        """Handle directories that have never synced successfully."""
        if (
            directory.failed_attempts
            >= self.__ignore_client_directory_after_failed_attempts_threshold
            and is_not_ignored
        ):
            logging.warning(
                f"Directory {dir_id} has failed {directory.failed_attempts} times and will be ignored."
            )
            self.__directory_info_service.set_ignored_status(dir_id, True)

    def __cleanup_deleted_directories(self) -> None:
        """Delete all directories that have been marked as deleted."""
        deleted_directories = self.__directory_info_service.get_all_deleted()
        for directory in deleted_directories:
            logging.info(f"Cleaning up deleted directory {directory.id}")
            self.__delete_data(directory)

    def __delete_data(self, directory: DirectoryDto) -> None:
        """Delete all data related to a directory."""
        dir_id = directory.id
        if directory.deleted_at is None:
            logging.warning(f"Directory {dir_id} has no deleted_at timestamp, skipping deletion.")
            raise ValueError("Directory deleted_at is None")
        if datetime.now() > directory.deleted_at:
            logging.warning(f"Directory {dir_id} deleted_at timestamp reached, deleting data.")
            self.__directory_info_service.delete(dir_id)
            self.__update_client_service.cleanup(dir_id)