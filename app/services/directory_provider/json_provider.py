from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    directory_urls: List[DirectoryDto]

    def __init__(self, directories_json_data: List[DirectoryDto], ignored_directory_service: IgnoredDirectoryService) -> None:
        self.__directories = directories_json_data
        self.__ignored_directory_service = ignored_directory_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        if not include_ignored:
            self.__ignored_directory_service.get_all_ignored_directories()
            return [
                directory for directory in self.__directories
                if not any(dir.directory_id == directory.id for dir in self.__ignored_directory_service.get_all_ignored_directories())
            ]
        return self.__directories
    
    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored_directories = self.__ignored_directory_service.get_all_ignored_directories()
        return [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or not any(dir.directory_id == directory.id for dir in ignored_directories)
        ]

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        directory = next((s for s in self.__directories if s.id == directory_id), None)
        if directory is None:
            raise HTTPException(
                status_code=404,
                detail=f"Directory with ID '{directory_id}' not found."
            )
        return directory
