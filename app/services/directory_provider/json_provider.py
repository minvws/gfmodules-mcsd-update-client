import json
from typing import List

from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    def __init__(self, json_path: str, directory_info_service: DirectoryInfoService) -> None:
        self.__directories = self._read_directories_file(json_path)
        self.__directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        return_dirs: List[DirectoryDto] = self.__directories

        if not include_ignored:
            # We need to filter out ignored directories
            ignored_directories = self.__directory_info_service.get_all_ignored()
            return_dirs = [
                directory for directory in return_dirs
                if not any(dir.id == directory.id for dir in ignored_directories)
            ]

        return return_dirs

    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored_directories = self.__directory_info_service.get_all_ignored()

        return_dirs = [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or not any(dir.id == directory.id for dir in ignored_directories)
        ]

        return return_dirs

    def get_one_directory(self, directory_id: str) -> DirectoryDto|None:
        return next((s for s in self.__directories if s.id == directory_id), None)


    @staticmethod
    def _read_directories_file(directory_urls_path: str) -> List[DirectoryDto]:
        print(__name__)
        try:
            with open(directory_urls_path) as f:
                data = json.load(f)
                return [DirectoryDto(**item) for item in data["directories"]]
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing directory URLs file: {e}")
