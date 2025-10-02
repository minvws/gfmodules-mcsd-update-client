from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.directory_provider.factory import DirectoryProviderFactory
from app.services.directory_provider.directory_provider import DirectoryProvider
from app.services.update.cache.provider import CacheProvider
from app.services.update.mass_update_client_service import MassUpdateClientService
from app.services.scheduler import Scheduler
import inject
from app.db.db import Database
from app.config import get_config
from app.services.entity.resource_map_service import ResourceMapService
from app.services.api.authenticators.factory import AuthenticatorFactory
from app.services.update.update_client_service import UpdateClientService
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

    directory_info_service = DirectoryInfoService(
        db, 
        config.client_directory.directory_marked_as_unhealthy_after_success_timeout_in_sec, # type: ignore
        config.client_directory.cleanup_delay_after_client_directory_marked_deleted_in_sec # type: ignore
    )
    binder.bind(DirectoryInfoService, directory_info_service)

    directory_provider_factory = DirectoryProviderFactory(
        config=config, auth=auth, directory_info_service=directory_info_service
    )
    directory_provider = directory_provider_factory.create()
    binder.bind(DirectoryProvider, directory_provider)

    cache_provider = CacheProvider(config=config.external_cache)
    update_service = UpdateClientService(
        update_client_url=config.mcsd.update_client_url,
        fill_required_fields=config.mcsd.fill_required_fields,
        timeout=config.client_directory.timeout,
        backoff=config.client_directory.backoff,
        request_count=config.mcsd.request_count,
        resource_map_service=resource_map_service,
        auth=auth,
        cache_provider=cache_provider,
        retries=config.client_directory.retries,
        mtls_cert=config.mcsd.mtls_client_cert_path,
        mtls_key=config.mcsd.mtls_client_key_path,
        mtls_ca=config.mcsd.mtls_server_ca_path,
    )
    binder.bind(UpdateClientService, update_service)



    update_all_service = MassUpdateClientService(
        update_client_service=update_service,
        directory_provider=directory_provider,
        directory_info_service=directory_info_service,
        mark_client_directory_as_deleted_after_success_timeout_seconds=config.client_directory.mark_client_directory_as_deleted_after_success_timeout_in_sec,  # type: ignore
        stats=get_stats(),
        mark_client_directory_as_deleted_after_lrza_delete=config.client_directory.mark_client_directory_as_deleted_after_lrza_delete,
        ignore_client_directory_after_success_timeout_seconds=config.client_directory.ignore_client_directory_after_success_timeout_in_sec,  # type: ignore
        ignore_client_directory_after_failed_attempts_threshold=config.client_directory.ignore_client_directory_after_failed_attempts_threshold,
    )

    update_scheduler = Scheduler(
        function=update_all_service.update_all,
        delay=config.scheduler.delay_input_in_sec,  # type: ignore
        max_logs_entries=config.scheduler.max_logs_entries,
    )

    cleanup_scheduler = Scheduler(
        function=update_all_service.cleanup_old_directories,
        delay=config.client_directory.mark_client_directory_as_deleted_after_success_timeout_in_sec,  # type: ignore
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


def get_directory_provider() -> DirectoryProvider:
    return inject.instance(DirectoryProvider)  # type: ignore

def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_client_service() -> UpdateClientService:
    return inject.instance(UpdateClientService)


def get_directory_info_service() -> DirectoryInfoService:
    return inject.instance(DirectoryInfoService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
