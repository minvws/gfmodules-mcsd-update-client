from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.directory_provider.api_provider import DirectoryApiProvider
from app.services.directory_provider.directory_provider import DirectoryProvider


class CachingDirectoryProvider(DirectoryProvider):
    def __init__(
        self,
        directory_provider: DirectoryApiProvider,
        directory_cache_service: DirectoryCacheService,
    ) -> None:
        self.__directoryProvider = directory_provider
        self.__directory_cache_service = directory_cache_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        original_directories = self.__directory_cache_service.get_all_directories_caches(include_ignored=include_ignored)
        return self.__common_directory_cache_logic(include_ignored=include_ignored, original_directories=original_directories)
    
    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        original_directories = []
        if include_ignored_ids:
            original_directories = self.__directory_cache_service.get_all_directories_caches_include_ignored_ids(include_ignored_ids=include_ignored_ids)
        else:
            original_directories = self.__directory_cache_service.get_all_directories_caches(include_ignored=False)

        return self.__common_directory_cache_logic(include_ignored=True, original_directories=original_directories)

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        try:
            directory = self.__directoryProvider.get_one_directory(directory_id)
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
            return original_directory
        self.__directory_cache_service.set_directory_cache(
            directory
        )
        return directory

    def __common_directory_cache_logic(self, include_ignored: bool, original_directories: list[DirectoryDto]) -> list[DirectoryDto]:
        try:
            directories = self.__directoryProvider.get_all_directories(include_ignored=include_ignored)
        except Exception:  # Retrieving directories failed
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
