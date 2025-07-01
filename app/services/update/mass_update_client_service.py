import logging
from datetime import datetime, timedelta, timezone
from typing import Any

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
    ) -> None:
        self.__directory_provider = directory_provider
        self.__update_client_service = update_client_service
        self.__directory_info_service = directory_info_service
        self.__ignored_directory_service = ignored_directory_service
        self.__cleanup_client_directory_after_success_timeout_seconds = cleanup_client_directory_after_success_timeout_seconds
        self.__stats = stats

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
            directories = self.__directory_info_service.get_all_directories_info()
            for directory_info in directories:
                if directory_info.last_success_sync is not None:
                    elapsed_time = (datetime.now(timezone.utc) - directory_info.last_success_sync.astimezone(timezone.utc)).total_seconds()
                    if elapsed_time > self.__cleanup_client_directory_after_success_timeout_seconds:
                        logging.info(f"Cleaning up directory for outdated directory {directory_info.directory_id}")
                        self.__update_client_service.cleanup(directory_info.directory_id)
                        self.__ignored_directory_service.add_directory_to_ignore_list(directory_info.directory_id)