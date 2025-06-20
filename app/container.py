from app.services.entity.supplier_ignored_directory_service import (
    SupplierIgnoredDirectoryService,
)
from app.services.entity.supplier_info_service import SupplierInfoService
from app.services.supplier_provider.factory import SupplierProviderFactory
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.services.update.cache.provider import CacheProvider
from app.services.update.mass_update_consumer_service import MassUpdateConsumerService
from app.services.scheduler import Scheduler
import inject
from app.db.db import Database
from app.config import get_config
from app.services.entity.resource_map_service import ResourceMapService
from app.services.api.authenticators.factory import AuthenticatorFactory
from app.services.update.update_consumer_service import UpdateConsumerService
from typing import cast

from app.stats import get_stats


def container_config(binder: inject.Binder) -> None:
    config = get_config()

    db = Database(dsn=config.database.dsn)
    binder.bind(Database, db)

    resource_map_service = ResourceMapService(db)
    binder.bind(ResourceMapService, resource_map_service)

    auth_factory = AuthenticatorFactory(config=config)
    auth = auth_factory.create_authenticator()

    supplier_provider_factory = SupplierProviderFactory(
        config=config, database=db, auth=auth
    )
    supplier_provider = supplier_provider_factory.create()
    binder.bind(SupplierProvider, supplier_provider)

    cache_provider = CacheProvider(config=config.external_cache)
    update_service = UpdateConsumerService(
        consumer_url=config.mcsd.consumer_url,
        strict_validation=config.mcsd.strict_validation,
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        request_count=config.mcsd.request_count,
        resource_map_service=resource_map_service,
        auth=auth,
        cache_provider=cache_provider,
    )
    binder.bind(UpdateConsumerService, update_service)

    supplier_ignored_directory_service = SupplierIgnoredDirectoryService(db)
    binder.bind(SupplierIgnoredDirectoryService, supplier_ignored_directory_service)

    supplier_info_service = SupplierInfoService(
        db,
        supplier_stale_timeout_seconds=config.scheduler.supplier_stale_timeout_in_sec,  # type: ignore
    )
    binder.bind(SupplierInfoService, supplier_info_service)

    update_all_service = MassUpdateConsumerService(
        update_consumer_service=update_service,
        supplier_provider=supplier_provider,
        supplier_info_service=supplier_info_service,
        supplier_ignored_directory_service=supplier_ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=config.scheduler.cleanup_client_directory_after_success_timeout_in_sec,  # type: ignore
        stats=get_stats(),
    )

    update_scheduler = Scheduler(
        function=update_all_service.update_all,
        delay=config.scheduler.delay_input_in_sec,  # type: ignore
        max_logs_entries=config.scheduler.max_logs_entries,
    )

    cleanup_scheduler = Scheduler(
        function=update_all_service.cleanup_old_directories,
        delay=config.scheduler.cleanup_client_directory_after_success_timeout_in_sec,  # type: ignore
        max_logs_entries=config.scheduler.max_logs_entries,
    )
    binder.bind("update_scheduler", update_scheduler)
    binder.bind("cleanup_scheduler", cleanup_scheduler)


def get_update_scheduler() -> Scheduler:
    return cast(Scheduler, inject.instance("update_scheduler"))


def get_cleanup_scheduler() -> Scheduler:
    return cast(Scheduler, inject.instance("cleanup_scheduler"))


def get_database() -> Database:
    return inject.instance(Database)


def get_supplier_provider() -> SupplierProvider:
    return inject.instance(SupplierProvider)  # type: ignore


def get_supplier_ignored_directory_service() -> SupplierIgnoredDirectoryService:
    return inject.instance(SupplierIgnoredDirectoryService)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_consumer_service() -> UpdateConsumerService:
    return inject.instance(UpdateConsumerService)


def get_supplier_info_service() -> SupplierInfoService:
    return inject.instance(SupplierInfoService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
