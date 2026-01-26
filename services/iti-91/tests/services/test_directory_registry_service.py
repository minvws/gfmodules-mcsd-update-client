import os

if os.getenv("APP_ENV") is None:
    os.environ["APP_ENV"] = "test"  # noqa

import pytest

from app.config import get_config, reset_config, set_config
from app.db.db import Database
from app.db.repositories.directory_provider_directory_repository import (
    DirectoryProviderDirectoryRepository,
)
from app.models.directory.dto import DirectoryDto
from app.services.api.directory_api_service import DirectoryApiService
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.directory_registry_service import DirectoryRegistryService
from app.services.entity.directory_info_service import DirectoryInfoService


class FakeUpdateClientService:
    def __init__(self) -> None:
        self.cleaned: list[str] = []

    def cleanup(self, directory_id: str) -> None:
        self.cleaned.append(directory_id)


@pytest.fixture()
def config():
    cfg = get_config()
    if hasattr(cfg, "model_copy"):
        return cfg.model_copy(deep=True)
    return cfg


@pytest.fixture(autouse=True)
def _set_global_config(config):
    set_config(config)
    yield
    reset_config()


@pytest.fixture()
def directory_info_service(database: Database, config) -> DirectoryInfoService:
    return DirectoryInfoService(
        database=database,
        directory_marked_as_unhealthy_after_success_timeout_seconds=config.client_directory.directory_marked_as_unhealthy_after_success_timeout_in_sec,
        cleanup_delay_after_client_directory_marked_deleted_in_sec=config.client_directory.cleanup_delay_after_client_directory_marked_deleted_in_sec,
    )


@pytest.fixture()
def update_client_service() -> FakeUpdateClientService:
    return FakeUpdateClientService()


@pytest.fixture()
def service(
    database: Database,
    config,
    directory_info_service: DirectoryInfoService,
    update_client_service: FakeUpdateClientService,
) -> DirectoryRegistryService:
    return DirectoryRegistryService(
        database=database,
        config=config,
        directory_info_service=directory_info_service,
        update_client_service=update_client_service,  # type: ignore[arg-type]
        auth=NullAuthenticator(),
    )


def test_add_provider_upserts(service: DirectoryRegistryService) -> None:
    created = service.add_provider(url="http://example.com/lrza", enabled=True)
    assert created["id"] is not None
    assert created["enabled"] is True

    updated = service.add_provider(url="http://example.com/lrza", enabled=False)
    assert updated["id"] == created["id"]
    assert updated["enabled"] is False


def test_list_providers_include_disabled_flag(service: DirectoryRegistryService) -> None:
    service.add_provider(url="http://enabled", enabled=True)
    service.add_provider(url="http://disabled", enabled=False)

    enabled_only = service.list_providers(include_disabled=False)
    urls = {p["url"] for p in enabled_only}
    assert urls == {"http://enabled"}


def test_add_manual_directory_defaults_id_and_origin(service: DirectoryRegistryService) -> None:
    dto = service.add_manual_directory(endpoint_address="http://example.com/fhir")
    assert dto.origin == "manual"
    assert dto.id is not None
    assert len(dto.id) == 32


def test_ensure_config_providers_seeds_from_list_and_single(service: DirectoryRegistryService, config) -> None:
    config.client_directory.directories_provider_urls = ["http://p1", " "]
    config.client_directory.directories_provider_url = "http://p2"
    config.client_directory.merge_directories_provider_url()

    service.ensure_config_providers()
    urls = {p["url"] for p in service.list_providers(include_disabled=True)}
    assert urls == {"http://p1", "http://p2"}


def test_refresh_provider_creates_directories_and_links(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    database: Database,
    directory_info_service: DirectoryInfoService,
) -> None:
    def _mock_fetch(self):
        return [
            DirectoryDto(id="d1", ura="", endpoint_address="http://a/fhir"),
            DirectoryDto(id="d2", ura="ura2", endpoint_address="http://b/fhir"),
        ]

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch)

    provider = service.add_provider(url="http://provider.example/lrza", enabled=True)
    out = service.refresh_provider(provider["id"])
    assert out["fetched"] == 2
    assert out["removed"] == 0

    all_dirs = directory_info_service.get_all(include_deleted=True, include_ignored=True)
    endpoints = {d.endpoint_address for d in all_dirs}
    assert "http://a/fhir" in endpoints
    assert "http://b/fhir" in endpoints

    with database.get_db_session() as session:
        link_repo = session.get_repository(DirectoryProviderDirectoryRepository)
        links = link_repo.get_active_by_provider(provider["id"])
        assert len(links) == 2


def test_refresh_all_enabled_providers_collects_errors(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
) -> None:
    p1 = service.add_provider(url="http://p1", enabled=True)
    p2 = service.add_provider(url="http://p2", enabled=True)

    def _refresh(provider_id: int):
        if provider_id == p1["id"]:
            return {"provider_id": provider_id, "ok": True}
        raise RuntimeError("boom")

    monkeypatch.setattr(service, "refresh_provider", _refresh)

    out = service.refresh_all_enabled_providers()
    assert out["providers"] == [{"provider_id": p1["id"], "ok": True}]
    assert out["errors"][0]["provider_id"] == p2["id"]


def test_refresh_provider_dedup_preserves_manual_origin(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    directory_info_service: DirectoryInfoService,
) -> None:
    manual = service.add_manual_directory(endpoint_address="http://same/fhir", directory_id="manual-id")
    assert manual.origin == "manual"

    def _mock_fetch(self):
        return [DirectoryDto(id="provider-id", ura="", endpoint_address="http://same/fhir")]

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch)

    provider = service.add_provider(url="http://provider.example/lrza", enabled=True)
    out = service.refresh_provider(provider["id"])
    assert out["fetched"] == 1

    after = directory_info_service.get_one_by_id("manual-id")
    assert after.origin == "manual"


def test_refresh_provider_returns_disabled_status_without_fetch(service: DirectoryRegistryService) -> None:
    provider = service.add_provider(url="http://provider.example/lrza", enabled=False)
    out = service.refresh_provider(provider["id"])
    assert out["status"] == "disabled"


def test_refresh_provider_removal_archives_provider_origin_when_confirmed_removed(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    directory_info_service: DirectoryInfoService,
    update_client_service: FakeUpdateClientService,
    config,
) -> None:
    config.client_directory.mark_client_directory_as_deleted_after_lrza_delete = True

    def _mock_fetch_one(self):
        return [DirectoryDto(id="d1", ura="", endpoint_address="http://a/fhir")]

    def _mock_fetch_none(self):
        return []

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_one)
    provider = service.add_provider(url="http://provider.example/lrza", enabled=True)
    service.refresh_provider(provider["id"])

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_none)
    out = service.refresh_provider(provider["id"])
    assert out["removed"] == 1
    assert out["archived"] == ["d1"]

    dto = directory_info_service.get_one_by_id("d1")
    assert dto.deleted_at is not None
    assert update_client_service.cleaned == ["d1"]


def test_refresh_provider_removal_does_not_archive_when_config_false(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    directory_info_service: DirectoryInfoService,
    config,
) -> None:
    config.client_directory.mark_client_directory_as_deleted_after_lrza_delete = False

    def _mock_fetch_one(self):
        return [DirectoryDto(id="d1", ura="", endpoint_address="http://a/fhir")]

    def _mock_fetch_none(self):
        return []

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_one)
    provider = service.add_provider(url="http://provider.example/lrza", enabled=True)
    service.refresh_provider(provider["id"])

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_none)
    out = service.refresh_provider(provider["id"])
    assert out["removed"] == 1
    assert out["archived"] == []

    dto = directory_info_service.get_one_by_id("d1")
    assert dto.deleted_at is None


def test_refresh_provider_does_not_archive_on_downtime(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    directory_info_service: DirectoryInfoService,
    config,
) -> None:
    config.client_directory.mark_client_directory_as_deleted_after_lrza_delete = True

    def _mock_fetch_one(self):
        return [DirectoryDto(id="d1", ura="", endpoint_address="http://a/fhir")]

    def _mock_fetch_raises(self):
        raise RuntimeError("provider down")

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_one)
    provider = service.add_provider(url="http://provider.example/lrza", enabled=True)
    service.refresh_provider(provider["id"])

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_raises)
    with pytest.raises(RuntimeError):
        service.refresh_provider(provider["id"])

    dto = directory_info_service.get_one_by_id("d1")
    assert dto.deleted_at is None


def test_refresh_provider_does_not_archive_when_other_provider_still_active(
    monkeypatch: pytest.MonkeyPatch,
    service: DirectoryRegistryService,
    directory_info_service: DirectoryInfoService,
    config,
) -> None:
    config.client_directory.mark_client_directory_as_deleted_after_lrza_delete = True

    def _mock_fetch_one(self):
        return [DirectoryDto(id="d1", ura="", endpoint_address="http://a/fhir")]

    def _mock_fetch_none(self):
        return []

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_one)
    p1 = service.add_provider(url="http://provider1.example/lrza", enabled=True)
    p2 = service.add_provider(url="http://provider2.example/lrza", enabled=True)
    service.refresh_provider(p1["id"])
    service.refresh_provider(p2["id"])

    monkeypatch.setattr(DirectoryApiService, "fetch_directories", _mock_fetch_none)
    out = service.refresh_provider(p1["id"])
    assert out["removed"] == 1
    assert out["archived"] == []

    dto = directory_info_service.get_one_by_id("d1")
    assert dto.deleted_at is None
