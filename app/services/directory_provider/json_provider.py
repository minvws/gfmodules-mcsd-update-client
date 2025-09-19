from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.directory_provider import DirectoryProvider


class DirectoryJsonProvider(DirectoryProvider):
    """
    Service to manage directories from a JSON data source.
    """
    directory_urls: List[DirectoryDto]

    def __init__(self, directories_json_data: List[DirectoryDto], directory_info_service: DirectoryInfoService) -> None:
        self.__directories = directories_json_data
        self.__directory_info_service = directory_info_service

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        possible_dirs = self.__directories
        if not include_ignored:
            ignored = self.__directory_info_service.get_all_ignored()
            ignored_ids = {dir.id for dir in ignored}
            possible_dirs = [directory for directory in self.__directories if directory.id not in ignored_ids]
        
        # Update directory info service with all directories
        dirs = []
        for directory in possible_dirs:
            try:
                cached_dir = self.__directory_info_service.get_one_by_id(directory.id)
                #  Preserve the is_ignored and failed attempts state from the cache
                directory.is_ignored = cached_dir.is_ignored
                directory.failed_attempts = cached_dir.failed_attempts
                directory.failed_sync_count = cached_dir.failed_sync_count
                directory.deleted_at = cached_dir.deleted_at
                directory.last_success_sync = cached_dir.last_success_sync
            except ValueError: # Directory not found
                pass
            dirs.append(self.__directory_info_service.update(directory))

        return dirs

    def get_all_directories_include_ignored_ids(self, include_ignored_ids: List[str]) -> List[DirectoryDto]:
        ignored = self.__directory_info_service.get_all_ignored()
        ignored_ids = {dir.id for dir in ignored}
        dirs = [
            directory for directory in self.__directories
            if directory.id in include_ignored_ids or directory.id not in ignored_ids
        ]
        for dir in dirs:
            try:
                cached_dir = self.__directory_info_service.get_one_by_id(dir.id)
                # Preserve the is_ignored and failed attempts state from the cache
                dir.is_ignored = cached_dir.is_ignored
                dir.failed_attempts = cached_dir.failed_attempts
                dir.failed_sync_count = cached_dir.failed_sync_count
                dir.deleted_at = cached_dir.deleted_at
                dir.last_success_sync = cached_dir.last_success_sync
            except ValueError: # If directory is not found in cache
                pass
            self.__directory_info_service.update(dir)
        return dirs

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        directory = next((s for s in self.__directories if s.id == directory_id), None)
        if directory is None:
            raise HTTPException(
                status_code=404,
                detail=f"Directory with ID '{directory_id}' not found."
            )
        try:
            cached_dir = self.__directory_info_service.get_one_by_id(directory_id)
            # Preserve the is_ignored and failed attempts state from the cache
            directory.is_ignored = cached_dir.is_ignored
            directory.failed_attempts = cached_dir.failed_attempts
            directory.failed_sync_count = cached_dir.failed_sync_count
            directory.deleted_at = cached_dir.deleted_at
            directory.last_success_sync = cached_dir.last_success_sync
        except ValueError: # If directory is not found in cache
            pass
        return self.__directory_info_service.update(directory)