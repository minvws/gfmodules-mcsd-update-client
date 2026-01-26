import os

if os.getenv("APP_ENV") is None:
    os.environ["APP_ENV"] = "test"  # noqa

from datetime import datetime, timezone

from app.db.db import Database
from app.services.directory_provider.registry_db_provider import RegistryDbDirectoryProvider
from app.services.entity.directory_info_service import DirectoryInfoService


def test_registry_db_provider_filters_deleted(database: Database) -> None:
    directory_info_service = DirectoryInfoService(
        database=database,
        directory_marked_as_unhealthy_after_success_timeout_seconds=3600,
        cleanup_delay_after_client_directory_marked_deleted_in_sec=2592000,
    )
    directory_info_service.create_or_update(
        directory_id="a",
        endpoint_address="http://a",
        ura="",
        origin="manual",
    )
    directory_info_service.create_or_update(
        directory_id="b",
        endpoint_address="http://b",
        ura="",
        origin="provider",
    )
    directory_info_service.set_deleted_at("b", specific_datetime=datetime.now(timezone.utc))

    provider = RegistryDbDirectoryProvider(directory_info_service=directory_info_service)
    dirs = provider.get_all_directories()
    ids = {d.id for d in dirs}
    assert ids == {"a"}


def test_registry_db_provider_get_one(database: Database) -> None:
    directory_info_service = DirectoryInfoService(
        database=database,
        directory_marked_as_unhealthy_after_success_timeout_seconds=3600,
        cleanup_delay_after_client_directory_marked_deleted_in_sec=2592000,
    )
    directory_info_service.create_or_update(
        directory_id="a",
        endpoint_address="http://a",
        ura="",
        origin="manual",
    )

    provider = RegistryDbDirectoryProvider(directory_info_service=directory_info_service)
    one = provider.get_one_directory("a")
    assert one.endpoint_address == "http://a"
