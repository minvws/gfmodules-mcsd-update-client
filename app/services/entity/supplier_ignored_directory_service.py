import logging
from typing import Sequence, TypeVar

from fastapi import HTTPException

from app.db.db import Database
from app.db.entities.supplier_ignored_directory import SupplierIgnoredDirectory
from app.db.repositories.supplier_ignored_directory_repository import SupplierIgnoredDirectoryRepository

logger = logging.getLogger(__name__)

T = TypeVar("T")

class SupplierIgnoredDirectoryService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_ignored_directory(self, directory_id: str) -> SupplierIgnoredDirectory| None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierIgnoredDirectoryRepository)
            return repository.get(directory_id)
        
    def get_all_ignored_directories(self) -> Sequence[SupplierIgnoredDirectory]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierIgnoredDirectoryRepository)
            return repository.get_all()
    
    def add_directory_to_ignore_list(self, directory_id: str) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierIgnoredDirectoryRepository)
            directory_ignored = SupplierIgnoredDirectory(
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
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierIgnoredDirectoryRepository)
            directory_ignored = repository.get(directory_id)
            if directory_ignored is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Directory {directory_id} is not in the ignore list, cannot be removed",
                )
            session.delete(directory_ignored)
            session.commit()