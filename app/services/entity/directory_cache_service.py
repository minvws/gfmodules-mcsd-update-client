import logging

from app.db.db import Database
from app.db.entities.directory_cache import DirectoryCache
from app.db.repositories.directory_cache_repository import DirectoryCacheRepository
from app.models.directory.dto import DirectoryDto
logger = logging.getLogger(__name__)

class DirectoryCacheService:
    """
    Service to manage directory caches in the database.
    """
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_deleted_directories(self) -> list[DirectoryDto]:
        """
        Retrieves all directories marked as deleted.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            deleted_directories = repository.get_deleted_directories()
            return [self._hydrate_to_directory_dto(directory) for directory in deleted_directories]

    def get_directory_cache(self, directory_id: str, with_deleted: bool = False) -> DirectoryDto|None:
        """
        Retrieves a directory cache by its ID. Includes deleted directories if specified.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            cache = repository.get(directory_id, with_deleted=with_deleted)
            return self._hydrate_to_directory_dto(cache) if cache else None

    def get_all_directories_caches(self, with_deleted: bool = False, include_ignored: bool = False) -> list[DirectoryDto]:
        """
        Retrieves all directory caches. Can include deleted and/or ignored directories based on parameters.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            return [self._hydrate_to_directory_dto(cache) for cache in repository.get_all(with_deleted=with_deleted, include_ignored=include_ignored)]

    def get_all_directories_caches_include_ignored_ids(self, include_ignored_ids: list[str], with_deleted: bool = False) -> list[DirectoryDto]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            return [self._hydrate_to_directory_dto(cache) for cache in repository.get_all_include_ignored_ids(with_deleted=with_deleted, include_ignored_ids=include_ignored_ids)]

    def set_directory_cache(self, directory: DirectoryDto) -> None:
        """
        Creates or updates a directory cache in the database.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            cache = repository.get(directory.id, with_deleted=True)
            if cache is None:
                session.add(DirectoryCache(directory_id=directory.id, endpoint=directory.endpoint))
            else:
                cache.endpoint = directory.endpoint
                cache.is_deleted = directory.is_deleted if directory.is_deleted is not None else cache.is_deleted
            session.commit()

    def delete_directory_cache(self, directory_id: str) -> None:
        """
        Deletes a directory cache from the database by its ID.
        """
        with self.__database.get_db_session() as session:
            repository = session.get_repository(DirectoryCacheRepository)
            cache = repository.get(directory_id, with_deleted=True)
            if cache is None:
                logger.error(f"Directory cache {directory_id} not found for deletion.")
                raise ValueError(f"Directory cache {directory_id} not found for deletion.")
            session.delete(cache)
            session.commit()

    def _hydrate_to_directory_dto(self, directory_cache: DirectoryCache) -> DirectoryDto:
        return DirectoryDto(
            id=directory_cache.directory_id,
            name= "",
            endpoint=directory_cache.endpoint,
            is_deleted=directory_cache.is_deleted,
        )
