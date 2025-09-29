from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    def __init__(self, directories: List[DirectoryDto], directory_info_service: DirectoryInfoService) -> None:
        self.__directories = directories
        self.__directory_info_service = directory_info_service

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
        return self.__directory_info_service.update(directory_id=directory.id, endpoint_address=directory.endpoint_address)
