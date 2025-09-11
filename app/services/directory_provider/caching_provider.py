from datetime import datetime, timezone
import logging
from typing import List, Sequence

from fastapi import HTTPException
from app.db.entities.directory_info_new import DirectoryInfoNew
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_new_service import DirectoryInfoNewService
from app.services.directory_provider.directory_api_service import (
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
        directory_info_service: DirectoryInfoNewService,
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
        return [
            self.directory_info_service._hydrate_to_directory_dto(dir) 
                for dir in self.__common_directory_cache_logic(
                    cached_directories=cached_directories
            )
        ]
        

    def get_all_directories_include_ignored_ids(
        self, include_ignored_ids: List[str]
    ) -> List[DirectoryDto]:
        """
        Returns a list of all directories including, if specified, the directories which id is in the ignore list, otherwise these are filtered out.
        """
        cached_directories: Sequence[DirectoryInfoNew] = []
        if include_ignored_ids:
            cached_directories = (
                self.directory_info_service.get_all_including_ignored_ids(
                    include_ignored_ids=include_ignored_ids
                )
            )
        else:
            cached_directories = self.directory_info_service.get_all(
                include_ignored=False, include_deleted=False
            )

        return [
            self.directory_info_service._hydrate_to_directory_dto(dir) 
                for dir in self.__common_directory_cache_logic(
                    cached_directories=cached_directories
            )
        ]

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        """
        Returns a specific directory by their unique identifier or raises Exception if the directory provider could not be reached.
        """
        try:
            directory = self.__api_service.fetch_one_directory(directory_id)
            return self.directory_info_service._hydrate_to_directory_dto(
                self.directory_info_service.update(
                    DirectoryInfoNew(
                        id=directory.id,
                        endpoint_address=directory.endpoint_address,
                    )
                )
            )
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
                original_directory.deleted_at = datetime.now(timezone.utc) # TODO: Add a specific deletion timestamp
                self.directory_info_service.update(original_directory)
            return self.directory_info_service._hydrate_to_directory_dto(original_directory)
        

    def __common_directory_cache_logic(
        self, cached_directories: Sequence[DirectoryInfoNew]
    ) -> Sequence[DirectoryInfoNew]:
        try:
            directories = self.__api_service.fetch_directories()
        except Exception as e:  # Retrieving directories failed
            logger.warning(
                f"Failed to retrieve directories using API: {e} using cached data."
            )
            return cached_directories

        dirs = []
        for dir in directories:
            dirs.append(self.directory_info_service.update(
                DirectoryInfoNew(
                    id=dir.id,
                    endpoint_address=dir.endpoint_address,
                )
            ))

        # Finally, check if any cached directories are no longer present in the fetched directories
        # and if they are deleted in the directory provider, mark them as deleted in the cache
        for dir in cached_directories:
            if (
                dir.id not in [directory.id for directory in directories] # If cached directory is not in the fetched directories
                and self.__api_service.check_if_directory_is_deleted(dir.id) # and it is deleted in the directory provider
            ):
                self.directory_info_service.delete(
                    self.directory_info_service.get_one_by_id(dir.id)
                )
        return dirs