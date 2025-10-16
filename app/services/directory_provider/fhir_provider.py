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


class FhirDirectoryProvider(DirectoryProvider):
    """
    Service to manage directories with caching mechanism.
    """
    def __init__(
        self,
        api_provider: DirectoryApiService,
        directory_info_service: DirectoryInfoService,
        validate_capability_statement: bool = False,
    ) -> None:
        # Api provider to fetch directories from FHIR server
        self.__api_provider = api_provider
        # Service to manage directory info like ignored directories
        self.__directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        """
        Returns a list of all directories, also including ignored ones if specified, otherwise these are filtered out.
        """
        dirs = self.__api_provider.fetch_directories()
        if not include_ignored:
            ignored = self.__directory_info_service.get_all_ignored()
            dirs = [d for d in dirs if d.id not in [i.id for i in ignored]]

        return dirs

    def get_all_directories_include_ignored_ids(
        self, include_ignored_ids: List[str]
    ) -> List[DirectoryDto]:
        """
        Returns a list of all directories including, if specified, the directories which id is in the ignore list, otherwise these are filtered out.
        """
        dirs = self.__api_provider.fetch_directories()
        if include_ignored_ids:
            ignored = self.__directory_info_service.get_all_ignored()
            dirs = [
                d
                for d in dirs
                if d.id in include_ignored_ids or d.id not in [i.id for i in ignored]
            ]

        return dirs

    def get_one_directory(self, directory_id: str) -> DirectoryDto|None:
        """
        Returns a specific directory by their unique identifier
        """
        try:
            directory = self.__api_provider.fetch_one_directory(directory_id)
        except HTTPException:
            logger.warning(f"Fetching directory with ID '{directory_id}' from FHIR provider failed.")
            return None

        return directory

    def is_deleted(self, dto: DirectoryDto) -> bool:
        """
        Checks if the directory is marked as deleted in the FHIR provider.
        """
        return self.__api_provider.check_if_directory_is_deleted(dto.id)
