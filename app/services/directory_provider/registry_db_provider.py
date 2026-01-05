from typing import List

from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService


class RegistryDbDirectoryProvider(DirectoryProvider):
    def __init__(self, directory_info_service: DirectoryInfoService) -> None:
        self.__directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return self.__directory_info_service.get_all(include_ignored=include_ignored, include_deleted=False)

    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        return self.__directory_info_service.get_all_including_ignored_ids(include_ignored_ids=include_ignored_ids, include_deleted=False)

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        return self.__directory_info_service.get_one_by_id(directory_id)
