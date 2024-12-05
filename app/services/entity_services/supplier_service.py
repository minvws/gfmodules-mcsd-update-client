from collections.abc import Sequence
from fastapi import HTTPException
from app.db.db import Database
from app.db.entities.supplier import Supplier
from app.db.repositories.supplier_repository import SuppliersRepository
from app.models.supplier.dto import SupplierCreateDto, SupplierUpdateDto


class SupplierService:
    def __init__(self, database: Database) -> None:
        self.__database = database

    def get_one(self, supplier_id: str) -> Supplier:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SuppliersRepository)
            supplier = repository.get(supplier_id=supplier_id)
            if supplier is None:
                raise HTTPException(status_code=404)

            return supplier

    def get_all(self) -> Sequence[Supplier]:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SuppliersRepository)
            suppliers = repository.get_all()
            return suppliers

    def add_one(self, dto: SupplierCreateDto) -> Supplier:
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SuppliersRepository)
            new_supplier = repository.create(Supplier(**dto.model_dump()))
            return new_supplier

    def update_one(self, supplier_id: str, dto: SupplierUpdateDto) -> Supplier:
        target_supplier = self.get_one(supplier_id)
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SuppliersRepository)
            target_supplier.name = dto.name
            target_supplier.endpoint = dto.endpoint
            return repository.update(target_supplier)

    def delete_one(self, supplier_id: str) -> None:
        target = self.get_one(supplier_id)
        with self.__database.get_db_session() as session:
            repository = session.get_repository(SuppliersRepository)
            return repository.delete(target)
