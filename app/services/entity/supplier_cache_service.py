import logging

from app.db.db import Database
from app.db.entities.supplier_cache import SupplierCache
from app.db.repositories.supplier_cache_repository import SupplierCacheRepository
from app.models.supplier.dto import SupplierDto
logger = logging.getLogger(__name__)

class SupplierCacheService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_supplier_cache(self, supplier_id: str, with_deleted: bool = False) -> SupplierDto|None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierCacheRepository)
            cache = repository.get(supplier_id, with_deleted=with_deleted)
            return self._hydrate_to_supplier_dto(cache) if cache else None
        
    def get_all_suppliers_caches(self, with_deleted: bool = False) -> list[SupplierDto]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierCacheRepository)
            return [self._hydrate_to_supplier_dto(cache) for cache in repository.get_all(with_deleted=with_deleted)]

    def set_supplier_cache(self, supplier_id: str, endpoint: str, is_deleted: bool|None = None) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierCacheRepository)
            cache = repository.get(supplier_id, with_deleted=True)
            if cache is None:
                session.add(SupplierCache(supplier_id=supplier_id, endpoint=endpoint))
            else:
                cache.endpoint = endpoint
                cache.is_deleted = is_deleted if is_deleted is not None else cache.is_deleted
            session.commit()

    def delete_supplier_cache(self, supplier_id: str) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierCacheRepository)
            cache = repository.get(supplier_id, with_deleted=True)
            if cache is None:
                logger.error(f"Supplier cache {supplier_id} not found for deletion.")
                raise ValueError(f"Supplier cache {supplier_id} not found for deletion.")
            session.delete(cache)
            session.commit()

    def _hydrate_to_supplier_dto(self, supplier_cache: SupplierCache) -> SupplierDto:
        return SupplierDto(
            id=supplier_cache.supplier_id,
            name= "",
            endpoint=supplier_cache.endpoint,
            is_deleted=supplier_cache.is_deleted,
        )