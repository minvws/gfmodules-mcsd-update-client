from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_new_service import DirectoryInfoNewService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    directory_urls: List[DirectoryDto]

    def __init__(self, directories_json_data: List[DirectoryDto], directory_info_service: DirectoryInfoNewService) -> None:
        self.__directories = directories_json_data
        self.__directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        if not include_ignored:
            ignored = self.__directory_info_service.get_all_ignored()
            ignored_ids = {dir.id for dir in ignored}
            return [directory for directory in self.__directories if directory.id not in ignored_ids]

        return self.__directories

    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored = self.__directory_info_service.get_all_ignored()
        ignored_ids = {dir.id for dir in ignored}
        return [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or directory.id not in ignored_ids
        ]

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        directory = next((s for s in self.__directories if s.id == directory_id), None)
        if directory is None:
            raise HTTPException(
                status_code=404,
                detail=f"Directory with ID '{directory_id}' not found."
            )
        return directory
