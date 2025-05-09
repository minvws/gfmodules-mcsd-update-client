import logging
from typing import Callable, List, Sequence, TypeVar

from fastapi import HTTPException

from app.db.db import Database
from app.db.entities.supplier_directory_ignore import SupplierDirectoryIgnore
from app.db.entities.supplier_info import SupplierInfo
from app.db.repositories.supplier_directory_ignore_repository import SupplierDirectoryIgnoreRepository
from app.models.supplier.dto import SupplierDto

logger = logging.getLogger(__name__)


class SupplierDirectoryIgnoreService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    
    T = TypeVar("T")

    def _filter_ignored(self, directories: Sequence[T], id_getter: Callable[[T], str]) -> List[T]:
        ignored = {d.directory_id for d in self.get_all_ignored_directories()}
        return [d for d in directories if id_getter(d) not in ignored]

    def filter_ignored_supplier_info(self, directories: Sequence[SupplierInfo]) -> List[SupplierInfo]:
        return self._filter_ignored(directories, lambda d: d.supplier_id)

    def filter_ignored_supplier_dto(self, directories: Sequence[SupplierDto]) -> List[SupplierDto]:
        return self._filter_ignored(directories, lambda d: d.id)

    def get_ignored_directory(self, directory_id: str) -> SupplierDirectoryIgnore| None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierDirectoryIgnoreRepository)
            return repository.get(directory_id)
        
    def get_all_ignored_directories(self) -> Sequence[SupplierDirectoryIgnore]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierDirectoryIgnoreRepository)
            return repository.get_all()
    
    def add_directory_to_ignore_list(self, directory_id: str) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierDirectoryIgnoreRepository)
            directory_ignored = SupplierDirectoryIgnore(
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
            repository = session.get_repository(SupplierDirectoryIgnoreRepository)
            directory_ignored = repository.get(directory_id)
            if directory_ignored is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Directory {directory_id} is not in the ignore list, cannot be removed",
                )
            session.delete(directory_ignored)
            session.commit()