from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """

    directory_urls: List[DirectoryDto]

    def __init__(self, directories_json_data: List[DirectoryDto], ignored_directory_service: IgnoredDirectoryService, validate_capability_statement: bool = False) -> None:
        self.__directories = directories_json_data
        self.__ignored_directory_service = ignored_directory_service
        self.__validate_capability_statement = validate_capability_statement

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        if not include_ignored:
            self.__ignored_directory_service.get_all_ignored_directories()
            directories = [
                directory for directory in self.__directories
                if not any(dir.directory_id == directory.id for dir in self.__ignored_directory_service.get_all_ignored_directories())
            ]
            if self.__validate_capability_statement:
                directories = [d for d in directories if self.check_capability_statement(d)]
            return directories
        return self.__directories

    def get_all_directories_include_ignored(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored_directories = self.__ignored_directory_service.get_all_ignored_directories()
        directories = [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or not any(dir.directory_id == directory.id for dir in ignored_directories)
        ]
        if self.__validate_capability_statement:
            directories = [d for d in directories if self.check_capability_statement(d)]
        return directories

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        directory = next((s for s in self.__directories if s.id == directory_id), None)
        if directory is None:
            raise HTTPException(
                status_code=404,
                detail=f"Directory with ID '{directory_id}' not found."
            )
        if self.__validate_capability_statement and not self.check_capability_statement(directory):
            raise HTTPException(
                status_code=400,
                detail=f"Directory {directory.id} at {directory.endpoint} does not support mCSD requirements",
            )
        return directory

    def directory_is_deleted(self, directory_id: str) -> bool:
        return False
