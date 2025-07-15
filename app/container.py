from app.services.entity.ignored_directory_service import IgnoredDirectoryService
from app.services.entity.directory_cache_service import DirectoryCacheService

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

    directory_provider_factory = DirectoryProviderFactory(
        config=config, database=db, auth=auth
    )
    directory_provider = directory_provider_factory.create()
    binder.bind(DirectoryProvider, directory_provider)

    cache_provider = CacheProvider(config=config.external_cache)
    update_service = UpdateClientService(
        update_client_url=config.mcsd.update_client_url,
        strict_validation=config.mcsd.strict_validation,
        timeout=config.directory_api.timeout,
        backoff=config.directory_api.backoff,
        request_count=config.mcsd.request_count,
        resource_map_service=resource_map_service,
        auth=auth,
        cache_provider=cache_provider,
    )
    binder.bind(UpdateClientService, update_service)

    ignored_directory_service = IgnoredDirectoryService(db)
    binder.bind(IgnoredDirectoryService, ignored_directory_service)

    directory_info_service = DirectoryInfoService(
        db,
        directory_stale_timeout_seconds=config.scheduler.directory_stale_timeout_in_sec,  # type: ignore
    )
    binder.bind(DirectoryInfoService, directory_info_service)

    directory_cache_service = DirectoryCacheService(db)

    update_all_service = MassUpdateClientService(
        update_client_service=update_service,
        directory_provider=directory_provider,
        directory_info_service=directory_info_service,
        ignored_directory_service=ignored_directory_service,
        cleanup_client_directory_after_success_timeout_seconds=config.scheduler.cleanup_client_directory_after_success_timeout_in_sec,  # type: ignore
        stats=get_stats(),
        directory_cache_service=directory_cache_service,
        cleanup_client_directory_after_directory_delete=config.scheduler.cleanup_client_directory_after_directory_delete,
        ignore_directory_after_success_timeout_seconds=config.scheduler.ignore_directory_after_success_timeout_in_sec,  # type: ignore
        ignore_directory_after_failed_attempts_threshold=config.scheduler.ignore_directory_after_failed_attempts_threshold,
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


def get_directory_provider() -> DirectoryProvider:
    return inject.instance(DirectoryProvider)  # type: ignore


def get_ignored_directory_service() -> IgnoredDirectoryService:
    return inject.instance(IgnoredDirectoryService)


def get_resource_map_service() -> ResourceMapService:
    return inject.instance(ResourceMapService)


def get_update_client_service() -> UpdateClientService:
    return inject.instance(UpdateClientService)


def get_directory_info_service() -> DirectoryInfoService:
    return inject.instance(DirectoryInfoService)


def setup_container() -> None:
    inject.configure(container_config, once=True)
