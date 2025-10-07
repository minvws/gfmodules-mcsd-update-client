from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    def __init__(self, directories: List[DirectoryDto], directory_info_service: DirectoryInfoService, validate_capability_statement: bool = False) -> None:
        self.__directories = directories
        self.__directory_info_service = directory_info_service
        self.__validate_capability_statement = validate_capability_statement

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return_dirs: List[DirectoryDto] = []
        if not include_ignored:
            self.__directory_info_service.get_all_ignored()
            return_dirs = [
                directory for directory in self.__directories
                if not any(dir.id == directory.id for dir in self.__directory_info_service.get_all_ignored())
            ]
        else:
            return_dirs = self.__directories
        if self.__validate_capability_statement:
            return_dirs = [d for d in return_dirs if self.check_capability_statement(d)]
        updated_dirs = []
        for dir in return_dirs:
            updated_dir = self.__directory_info_service.update(directory_id=dir.id, endpoint_address=dir.endpoint_address)
            updated_dirs.append(updated_dir)
        return updated_dirs

    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored_directories = self.__directory_info_service.get_all_ignored()
        return_dirs = [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or not any(dir.id == directory.id for dir in ignored_directories)
        ]
        if self.__validate_capability_statement:
            return_dirs = [d for d in return_dirs if self.check_capability_statement(d)]
        updated_dirs = []
        for dir in return_dirs:
            updated_dir = self.__directory_info_service.update(directory_id=dir.id, endpoint_address=dir.endpoint_address)
            updated_dirs.append(updated_dir)
        return updated_dirs

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
                detail=f"Directory {directory.id} at {directory.endpoint_address} does not support mCSD requirements",
            )
        return self.__directory_info_service.update(directory_id=directory.id, endpoint_address=directory.endpoint_address)
