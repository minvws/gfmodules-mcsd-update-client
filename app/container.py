import inject

from app.db.db import Database
from app.config import get_config
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.entity_services.supplier_service import SupplierService


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    supplier_service = SupplierService(db)
    binder.bind(SupplierService, supplier_service)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_service() -> SupplierService:
    return inject.instance(SupplierService)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
