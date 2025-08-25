import logging
from typing import Sequence, TypeVar

from fastapi import HTTPException

from app.db.db import Database
from app.db.entities.ignored_directory import IgnoredDirectory
from app.db.repositories.ignored_directory_repository import IgnoredDirectoryRepository

logger = logging.getLogger(__name__)

T = TypeVar("T")

class IgnoredDirectoryService:
    """
    Service to manage ignored directories in the database.
    """
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_ignored_directory(self, directory_id: str) -> IgnoredDirectory| None:
        """
        Retrieves an ignored directory entry by its ID.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(IgnoredDirectoryRepository)
            return repository.get(directory_id)

    def get_all_ignored_directories(self) -> Sequence[IgnoredDirectory]:
        """
        Retrieves all ignored directory entries.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(IgnoredDirectoryRepository)
            return repository.get_all()

    def add_directory_to_ignore_list(self, directory_id: str) -> None:
        """
        Adds a directory to the ignore list.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(IgnoredDirectoryRepository)
            directory_ignored = IgnoredDirectory(
                directory_id=directory_id,
            )
            if repository.get(directory_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"Directory {directory_id} is already in the ignore list",
                )
            session.add(directory_ignored)
            session.commit()

    def remove_directory_from_ignore_list(self, directory_id: str) -> None:
        """
        Removes a directory from the ignore list.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(IgnoredDirectoryRepository)
            directory_ignored = repository.get(directory_id)
            if directory_ignored is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Directory {directory_id} is not in the ignore list, cannot be removed",
                )

            session.delete(directory_ignored)
            session.commit()
