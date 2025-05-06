import logging

from app.db.db import Database
from app.db.entities.supplier_info import SupplierInfo
from app.db.repositories.supplier_info_repository import SupplierInfoRepository

logger = logging.getLogger(__name__)


class SupplierInfoService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_supplier_info(self, supplier_id: str) -> SupplierInfo:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierInfoRepository)
            return repository.get(supplier_id)

    def update_supplier_info(self, info: SupplierInfo) -> None:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SupplierInfoRepository)
            repository.update(info)
