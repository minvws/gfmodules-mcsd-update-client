from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.update.filter_ura import create_ura_whitelist
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
        max_concurrent_directory_updates: int = 1,
    ) -> None:
        self.__directory_provider = directory_provider
        self.__update_client_service = update_client_service
        self.__directory_info_service = directory_info_service
        self.__mark_client_directory_as_deleted_after_success_timeout_seconds = (
            mark_client_directory_as_deleted_after_success_timeout_seconds
        )
        self.__stats = stats
        self.__mark_client_directory_as_deleted_after_lrza_delete = (
            mark_client_directory_as_deleted_after_lrza_delete
        )
        self.__ignore_client_directory_after_success_timeout_seconds = (
            ignore_client_directory_after_success_timeout_seconds
        )
        self.__ignore_client_directory_after_failed_attempts_threshold = (
            ignore_client_directory_after_failed_attempts_threshold
        )
        self.__max_concurrent_directory_updates = max(1, max_concurrent_directory_updates)

    def update_all(self) -> list[dict[str, Any]]:
        """Update all directories (optionally in parallel)."""
        with self.__stats.timer("update_all_directories"):
            try:
                all_directories = self.__directory_provider.get_all_directories()
            except Exception:
                logger.exception("Failed to retrieve directories")
                return []

            ura_whitelist = create_ura_whitelist(all_directories)

            if self.__max_concurrent_directory_updates <= 1:
                return [self.__update_one(d, ura_whitelist) for d in all_directories]

            results: list[dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=self.__max_concurrent_directory_updates) as executor:
                future_map = {
                    executor.submit(self.__update_one, directory, ura_whitelist): directory
                    for directory in all_directories
                }
                for future in as_completed(future_map):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        directory = future_map[future]
                        logger.exception("Unhandled exception while updating directory %s", directory.id)
                        results.append(
                            {
                                "directory_id": directory.id,
                                "status": "error",
                                "error": str(e),
                            }
                        )
            return results

    def __update_one(self, directory: DirectoryDto, ura_whitelist: dict[str, list[str]]):
        info = self.__directory_info_service.get_one_by_id(directory.id)

        try:
            result = self.__update_client_service.update(
                directory,
                info.last_success_sync,
                ura_whitelist,
            )

            # Only mark as successful when the update was actually executed.
            if isinstance(result, dict) and result.get("status") == "success":
                # We apply a small lookback window to reduce risk of missing events due to clock skew.
                new_updated = datetime.now(timezone.utc) - timedelta(seconds=60)
                info.last_success_sync = new_updated
                info.failed_attempts = 0
                self.__directory_info_service.update(
                    directory_id=info.id,
                    last_success_sync=info.last_success_sync,
                    failed_attempts=info.failed_attempts,
                )

            return result

        except Exception as e:
            logger.exception("Failed to update directory %s", directory.id)
            info.failed_attempts += 1
            info.failed_sync_count += 1
            self.__directory_info_service.update(
                directory_id=info.id,
                failed_attempts=info.failed_attempts,
                failed_sync_count=info.failed_sync_count,
            )
            return {
                "directory_id": directory.id,
                "status": "error",
                "error": str(e),
            }

    def cleanup_old_directories(self) -> None:
        with self.__stats.timer("cleanup_old_directories"):
            directories = self.__directory_info_service.get_all(
                include_ignored=True, include_deleted=True
            )

            for directory_info in directories:
                self.__process_directory(directory_info)

            if self.__mark_client_directory_as_deleted_after_lrza_delete:
                self.__cleanup_deleted_directories()
            else:
                logger.info("Skipping cleanup of deleted directories as per configuration.")

    def __process_directory(self, directory: DirectoryDto) -> None:
        """Process one directory: either synced at least once, or never synced."""
        dir_id = directory.id
        is_not_ignored = (
            self.__directory_info_service.get_one_by_id(directory_id=dir_id).is_ignored is False
        )

        if directory.last_success_sync is not None:
            self.__process_successful_directory(directory, dir_id, is_not_ignored)
        else:
            self.__process_never_synced_directory(directory, dir_id, is_not_ignored)

    def __process_successful_directory(
        self, directory: DirectoryDto, dir_id: str, is_not_ignored: bool
    ) -> None:
        """Handle directories that have been synced successfully at least once."""
        assert directory.last_success_sync is not None  # Guaranteed by caller
        last_success_sync = directory.last_success_sync
        if last_success_sync.tzinfo is None:
            last_success_sync = last_success_sync.replace(tzinfo=timezone.utc)
        else:
            last_success_sync = last_success_sync.astimezone(timezone.utc)

        elapsed_time = (datetime.now(tz=timezone.utc) - last_success_sync).total_seconds()

        directory_is_stale = (
            elapsed_time > self.__ignore_client_directory_after_success_timeout_seconds and is_not_ignored
        )
        directory_is_outdated = (
            elapsed_time > self.__mark_client_directory_as_deleted_after_success_timeout_seconds
        )

        if directory_is_stale:
            logger.warning(
                "Directory %s has not been updated for %s seconds, ignoring it.",
                dir_id,
                elapsed_time,
            )
            self.__directory_info_service.set_ignored_status(dir_id, True)

        if directory_is_outdated:
            logger.info("Setting delete_at for outdated directory %s", dir_id)
            self.__directory_info_service.set_deleted_at(dir_id)

    def __process_never_synced_directory(
        self, directory: DirectoryDto, dir_id: str, is_not_ignored: bool
    ) -> None:
        """Handle directories that have never synced successfully."""
        if (
            directory.failed_attempts >= self.__ignore_client_directory_after_failed_attempts_threshold
            and is_not_ignored
        ):
            logger.warning(
                "Directory %s has failed %s times and will be ignored.",
                dir_id,
                directory.failed_attempts,
            )
            self.__directory_info_service.set_ignored_status(dir_id, True)

    def __cleanup_deleted_directories(self) -> None:
        """Delete all directories that have been marked as deleted."""
        deleted_directories = self.__directory_info_service.get_all_deleted()
        for directory in deleted_directories:
            logger.info("Cleaning up deleted directory %s", directory.id)
            self.__delete_data(directory)

    def __delete_data(self, directory: DirectoryDto) -> None:
        """Delete all data related to a directory."""
        dir_id = directory.id
        if directory.deleted_at is None:
            logger.warning(
                "Directory %s has no deleted_at timestamp, skipping deletion.", dir_id
            )
            raise ValueError("Directory deleted_at is None")

        if datetime.now(timezone.utc) > directory.deleted_at.astimezone(timezone.utc):
            logger.warning("Directory %s deleted_at timestamp reached, deleting data.", dir_id)
            self.__directory_info_service.delete(dir_id)
            self.__update_client_service.cleanup(dir_id)
