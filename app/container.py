import inject

from app.db.db import Database
from app.config import get_config
from app.services.entity_services.supplier_service import SupplierService


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    supplier_service = SupplierService(db)
    binder.bind(SupplierService, supplier_service)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_service() -> SupplierService:
    return inject.instance(SupplierService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
