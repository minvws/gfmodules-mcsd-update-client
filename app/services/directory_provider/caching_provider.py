import logging
from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.directory_provider.api_provider import DirectoryApiProvider
from app.services.directory_provider.directory_provider import DirectoryProvider

logger = logging.getLogger(__name__)

class CachingDirectoryProvider(DirectoryProvider):
    """
    Service to manage directories with caching mechanism.
    """
    def __init__(
        self,
        directory_provider: DirectoryApiProvider,
        directory_cache_service: DirectoryCacheService,
        validate_capability_statement: bool = False,
    ) -> None:
        self.__directoryProvider = directory_provider
        self.__directory_cache_service = directory_cache_service
        self.__validate_capability_statement = validate_capability_statement

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        """
        Returns a list of all directories, also including ignored ones if specified, otherwise these are filtered out.
        """
        original_directories = self.__directory_cache_service.get_all_directories_caches(include_ignored=include_ignored)
        return self.__common_directory_cache_logic(include_ignored=include_ignored, original_directories=original_directories)

    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        """
        Returns a list of all directories including, if specified, the directories which id is in the ignore list, otherwise these are filtered out.
        """
        original_directories = []
        if include_ignored_ids:
            original_directories = self.__directory_cache_service.get_all_directories_caches_include_ignored_ids(include_ignored_ids=include_ignored_ids)
        else:
            original_directories = self.__directory_cache_service.get_all_directories_caches(include_ignored=False)

        return self.__common_directory_cache_logic(include_ignored=True, original_directories=original_directories)

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        """
        Returns a specific directory by their unique identifier or raises Exception if the directory provider could not be reached.
        """
        try:
            directory = self.__directoryProvider.get_one_directory(directory_id)
            if self.__validate_capability_statement and not self.check_capability_statement(directory):
                raise HTTPException(
                    status_code=400,
                    detail=f"Directory {directory.id} at {directory.endpoint} does not support mCSD requirements",
                )
        except Exception:  # Retrieving directory failed
            original_directory = self.__directory_cache_service.get_directory_cache(
                directory_id
            )
            if original_directory is None:
                raise HTTPException(
                status_code=404,
                detail="Failed to retrieve directory from the directory provider and no cached data was available.",
            )
            if self.__directoryProvider.check_if_directory_is_deleted(directory_id):
                original_directory.is_deleted = True
                self.__directory_cache_service.set_directory_cache(
                    original_directory
                )
            if self.__validate_capability_statement and not self.check_capability_statement(original_directory):
                raise HTTPException(
                    status_code=400,
                    detail=f"Directory {original_directory.id} at {original_directory.endpoint} does not support mCSD requirements",
                )
            return original_directory
        self.__directory_cache_service.set_directory_cache(
            directory
        )
        return directory

    def __common_directory_cache_logic(self, include_ignored: bool, original_directories: list[DirectoryDto]) -> list[DirectoryDto]:
        try:
            directories = self.__directoryProvider.get_all_directories(include_ignored=include_ignored)
            if self.__validate_capability_statement:
                directories = [d for d in directories if self.check_capability_statement(d)]
        except Exception as e:  # Retrieving directories failed
            logger.warning(f"Failed to retrieve directories from the directory provider: {e}")
            if self.__validate_capability_statement:
                original_directories = [d for d in original_directories if self.check_capability_statement(d)]
            return original_directories

        for directory in directories:
            self.__directory_cache_service.set_directory_cache(
                DirectoryDto(
                    id=directory.id,
                    name=directory.name,
                    endpoint=directory.endpoint,
                )
            )

        for original_directory in original_directories:
            if original_directory.id not in [directory.id for directory in directories]:
                if self.__directoryProvider.check_if_directory_is_deleted(original_directory.id):
                    self.__directory_cache_service.set_directory_cache(
                        DirectoryDto(
                            id=original_directory.id,
                            name=original_directory.name,
                            endpoint=original_directory.endpoint,
                            is_deleted=True,
                        )
                    )
        return directories
