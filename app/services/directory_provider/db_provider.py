import logging
from typing import List

from fastapi import HTTPException
from app.models.directory.dto import DirectoryDto
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService

logger = logging.getLogger(__name__)


class DbProvider(DirectoryProvider):
    """
    Service to manage directories with database persistence.
    """

    def __init__(
        self,
        inner: DirectoryProvider,
        directory_info_service: DirectoryInfoService,
    ) -> None:
        self.__inner = inner  # Fetch and Filter providers
        self.__directory_info_service = directory_info_service  # Caching in DB

    def get_all_directories(self, include_ignored: bool = False) -> List[DirectoryDto]:
        try:
            dirs = self.__inner.get_all_directories(include_ignored)
        except Exception as e:
            logger.error(f"Error fetching directories: {e}")
            return self.__directory_info_service.get_all(
                include_ignored=include_ignored
            )

        # Check for deleted directories
        self._check_and_set_if_deleted(dirs)

        return self._update_directories_in_db(dirs)

    def get_all_directories_include_ignored_ids(
        self, include_ignored_ids: List[str]
    ) -> List[DirectoryDto]:
        try:
            dirs = self.__inner.get_all_directories_include_ignored_ids(
                include_ignored_ids
            )
        except Exception as e:
            logger.error(f"Error fetching directories: {e}")
            return self.__directory_info_service.get_all_including_ignored_ids(
                include_ignored_ids=include_ignored_ids
            )

        # Check for deleted directories
        self._check_and_set_if_deleted(dirs)

        return self._update_directories_in_db(dirs)

    def get_one_directory(self, directory_id: str) -> DirectoryDto:
        try:
            dir_dto = self.__inner.get_one_directory(directory_id)
        except Exception as e:
            logger.error(f"Error fetching directory with ID '{directory_id}': {e}")
            self._check_and_set_if_deleted(
                [DirectoryDto(id=directory_id, ura="", endpoint_address="")]
            )
            dir_dto = None
        try:
            db_dir = self.__directory_info_service.get_one_by_id(directory_id)
        except Exception:
            logger.info(f"Directory with ID '{directory_id}' not found in DB.")
            if dir_dto is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Directory with ID '{directory_id}' could not be fetched from provider or found in DB.",
                )
        return db_dir

    def _update_directories_in_db(self, dirs: List[DirectoryDto]) -> List[DirectoryDto]:
        """
        Updates or inserts the fetched directories into the database.
        """
        updated_dirs = []
        for dir_dto in dirs:
            updated_dir = self.__directory_info_service.create_or_update(
                directory_id=dir_dto.id,
                endpoint_address=dir_dto.endpoint_address,
                ura=dir_dto.ura,
            )
            updated_dirs.append(updated_dir)

        return updated_dirs

    def _check_and_set_if_deleted(self, dirs: List[DirectoryDto]) -> None:
        """
        If a directory exists in the database but not in the fetched list, check if its deleted
        """
        try:
            for dir_dto in dirs:
                if not self.__directory_info_service.exists(dir_dto.id):
                    continue  # Not in DB so its new
                if self.__inner.is_deleted(dir_dto):
                    self.__directory_info_service.set_deleted_at(dir_dto.id)
        except Exception as e:
            logger.error(f"Error in checking whether directories are deleted: {e}")
