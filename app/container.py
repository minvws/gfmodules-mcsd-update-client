from app.services.entity.supplier_info_service import SupplierInfoService
from app.services.supplier_provider.factory import SupplierProviderFactory
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.services.update.update_all_consumers_service import UpdateAllConsumersService
from app.services.scheduler import Scheduler
import inject
from app.db.db import Database
from app.config import get_config
from app.services.entity.resource_map_service import ResourceMapService
from app.services.api.authenticators.factory import AuthenticatorFactory
from app.services.update.update_consumer_service import UpdateConsumerService

def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)

    auth_factory = AuthenticatorFactory(config=config)
    auth = auth_factory.create_authenticator()

    supplier_provider_factory = SupplierProviderFactory(config=config, database=db)
    supplier_provider = supplier_provider_factory.create()
    
    update_service = UpdateConsumerService(
        consumer_url=config.mcsd.consumer_url,
        strict_validation=config.mcsd.strict_validation,
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        request_count=config.mcsd.request_count,
        resource_map_service=resource_map_service,
        supplier_provider=supplier_provider,
        auth=auth,
    )
    binder.bind(UpdateConsumerService, update_service)

    supplier_info_service = SupplierInfoService(db)

    update_all_service = UpdateAllConsumersService(
        update_consumer_service=update_service,
        supplier_provider=supplier_provider,
        supplier_info_service=supplier_info_service,
    )
    scheduler = Scheduler(
        function=update_all_service.update_all,
        delay=config.scheduler.delay,
        max_logs_entries=config.scheduler.max_logs_entries,
    )
    binder.bind(Scheduler, scheduler)


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_provider() -> SupplierProvider:
    return inject.instance(SupplierProvider)  # type: ignore


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_scheduler() -> Scheduler:
    return inject.instance(Scheduler)


def get_update_consumer_service() -> UpdateConsumerService:
    return inject.instance(UpdateConsumerService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
