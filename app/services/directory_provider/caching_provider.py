from datetime import datetime
import logging
from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.api.directory_api_service import (
    DirectoryApiService,
)
from app.services.directory_provider.directory_provider import DirectoryProvider

logger = logging.getLogger(__name__)


class CachingDirectoryProvider(DirectoryProvider):
    """
    Service to manage directories with caching mechanism.
    """

    def __init__(
        self,
        api_service: DirectoryApiService,
        directory_info_service: DirectoryInfoService,
    ) -> None:
        self.__api_service = api_service
        self.directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        """
        Returns a list of all directories, also including ignored ones if specified, otherwise these are filtered out.
        """
        cached_directories = self.directory_info_service.get_all(
            include_ignored=include_ignored, include_deleted=False
        )
        return self.__common_directory_cache_logic(cached_directories=cached_directories)

    def get_all_directories_include_ignored_ids(
        self, include_ignored_ids: List[str]
    ) -> List[DirectoryDto]:
        """
        Returns a list of all directories including, if specified, the directories which id is in the ignore list, otherwise these are filtered out.
        """
        cached_directories: List[DirectoryDto] = []
        if include_ignored_ids:
            cached_directories = self.directory_info_service.get_all_including_ignored_ids(include_ignored_ids=include_ignored_ids)
        else:
            cached_directories = self.directory_info_service.get_all(include_ignored=False, include_deleted=False)

        return self.__common_directory_cache_logic(cached_directories=cached_directories)

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        """
        Returns a specific directory by their unique identifier or raises Exception if the directory provider could not be reached.
        """
        try:
            directory = self.__api_service.fetch_one_directory(directory_id)
            try:
                cached_dir = self.directory_info_service.get_one_by_id(directory_id)
                # Preserve the is_ignored and failed attempts state from the cache
                directory.is_ignored = cached_dir.is_ignored
                directory.failed_attempts = cached_dir.failed_attempts
                directory.failed_sync_count = cached_dir.failed_sync_count
                directory.deleted_at = cached_dir.deleted_at
                directory.last_success_sync = cached_dir.last_success_sync
            except ValueError: # If directory is not found in cache
                pass
            return self.directory_info_service.update(directory)
        except Exception:  # Retrieving directory failed
            try:
                original_directory = self.directory_info_service.get_one_by_id(
                    directory_id
                )
            except ValueError:
                raise HTTPException(
                    status_code=404,
                    detail="Failed to retrieve directory from the directory provider and no cached data was available.",
                )
                
            if self.__api_service.check_if_directory_is_deleted(directory_id):
                original_directory.deleted_at = datetime.now() # TODO: Add a specific deletion timestamp
                self.directory_info_service.update(original_directory)
            return original_directory
        

    def __common_directory_cache_logic(
        self, cached_directories: List[DirectoryDto]
    ) -> List[DirectoryDto]:
        try:
            directories = self.__api_service.fetch_directories()
        except Exception as e:  # Retrieving directories failed
            logger.warning(
                f"Failed to retrieve directories using API: {e} using cached data."
            )
            return cached_directories

        dirs: List[DirectoryDto] = []
        for dir in directories:
            cached_dir = next(
                (d for d in cached_directories if d.id == dir.id), None
            )
            if cached_dir:
                # Preserve the is_ignored and failed attempts state from the cache
                dir.is_ignored = cached_dir.is_ignored
                dir.failed_attempts = cached_dir.failed_attempts
                dir.failed_sync_count = cached_dir.failed_sync_count
                dir.deleted_at = cached_dir.deleted_at
                dir.last_success_sync = cached_dir.last_success_sync
            dirs.append(
                self.directory_info_service.update(dir)
            )

        # Finally, check if any cached directories are no longer present in the fetched directories
        # and if they are deleted in the directory provider, mark them as deleted in the cache
        for cached_dir in cached_directories:
            if (
                cached_dir.id not in [directory.id for directory in directories] # If cached directory is not in the fetched directories
                and self.__api_service.check_if_directory_is_deleted(cached_dir.id) # and it is deleted in the directory provider
            ):
                if cached_dir.deleted_at is None:
                    cached_dir.deleted_at = datetime.now() # TODO: Add a specific deletion timestamp
                dirs.append(self.directory_info_service.update(cached_dir))
        return dirs