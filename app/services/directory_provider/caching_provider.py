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
        validate_capability_statement: bool = False,
    ) -> None:
        self.__api_service = api_service
        self.directory_info_service = directory_info_service
        self.__validate_capability_statement = validate_capability_statement

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
            
        except HTTPException:  # Retrieving directory failed
            try:
                directory = self.directory_info_service.get_one_by_id(
                    directory_id
                )
            except Exception:
                raise HTTPException(
                    status_code=404,
                    detail="Failed to retrieve directory from the directory provider and no cached data was available.",
                )
            if self.__api_service.check_if_directory_is_deleted(directory_id):
                self.directory_info_service.set_deleted_at(directory_id)
        if self.__validate_capability_statement and not self.check_capability_statement(directory):
            raise HTTPException(
                status_code=400,
                detail=f"Directory {directory.id} at {directory.endpoint_address} does not support mCSD requirements",
            )
        return self.directory_info_service.update(directory_id, endpoint_address=directory.endpoint_address)

    def __common_directory_cache_logic(
        self, cached_directories: List[DirectoryDto]
    ) -> List[DirectoryDto]:
        try:
            directories = self.__api_service.fetch_directories()
            if self.__validate_capability_statement:
                directories = [d for d in directories if self.check_capability_statement(d)]
        except Exception as e:  # Retrieving directories failed
            logger.warning(
                f"Failed to retrieve directories using API: {e} using cached data."
            )
            return cached_directories

        dirs: List[DirectoryDto] = []
        for dir in directories: # Update or add all fetched directories to the cache
            updated = self.directory_info_service.update(directory_id=dir.id, endpoint_address=dir.endpoint_address)
            dirs.append(updated)

        # Finally, check if any cached directories are no longer present in the fetched directories
        # and if they are deleted in the directory provider, mark them as deleted in the cache
        for cached_dir in cached_directories:
            cached_dir_not_in_fetched = cached_dir.id not in [directory.id for directory in directories]
            deleted_in_provider = self.__api_service.check_if_directory_is_deleted(cached_dir.id)
            if (
                cached_dir_not_in_fetched # If cached directory is not in the fetched directories
                and deleted_in_provider # and it is deleted in the directory provider
            ):
                if cached_dir.deleted_at is None:
                    self.directory_info_service.set_deleted_at(cached_dir.id)
                continue  # Skip adding this deleted directory to the result list
            dirs.append(cached_dir)
        return dirs