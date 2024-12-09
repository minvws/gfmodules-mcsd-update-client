import inject

from app.db.db import Database
from app.config import get_config
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.entity_services.supplier_service import SupplierService
from app.services.request_services.supplier_request_service import (
    SupplierRequestsService,
)
from app.services.request_services.consumer_request_service import (
    ConsumerRequestService,
)
from app.services.mcsd_services.update_consumer_service import UpdateConsumerService


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    supplier_service = SupplierService(db)
    binder.bind(SupplierService, supplier_service)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)

    consumer_request_service = ConsumerRequestService(config.mcsd.consumer_url)
    supplier_request_service = SupplierRequestsService(supplier_service)

    update_consumer_service = UpdateConsumerService(
        consumer_request_service=consumer_request_service,
        supplier_request_service=supplier_request_service,
        resource_map_service=resource_map_service,
    )
    binder.bind(UpdateConsumerService, update_consumer_service)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_service() -> SupplierService:
    return inject.instance(SupplierService)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_consumer_service() -> UpdateConsumerService:
    return inject.instance(UpdateConsumerService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
