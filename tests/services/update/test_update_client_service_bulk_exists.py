from unittest.mock import MagicMock, patch

import pytest

from app.models.directory.dto import DirectoryDto
from app.services.api.authenticators.null_authenticator import NullAuthenticator
from app.services.api.fhir_api import FhirApiConfig
from fhir.resources.bundle import BundleEntry
from fhir.resources.organization import Organization
from app.services.update.cache.provider import CacheProvider
from app.services.update.update_client_service import UpdateClientService
from app.config import ConfigExternalCache


class _FakeCache:
    def __init__(self) -> None:
        self.bulk_exists_called_with = None

    def bulk_exists(self, keys):  # type: ignore[no-untyped-def]
        self.bulk_exists_called_with = list(keys)
        return set()

    # Not used in these tests (we patch __clear_and_add_nodes), but present for safety
    def key_exists(self, key: str) -> bool:
        return False

    def add_node(self, node) -> None:  # type: ignore[no-untyped-def]
        return None


@pytest.fixture
def update_client_service() -> UpdateClientService:
    api_config = FhirApiConfig(
        base_url="https://example.com",
        fill_required_fields=False,
        timeout=30,
        backoff=0.1,
        retries=5,
        request_count=3,
        auth=NullAuthenticator(),
        mtls_cert=None,
        mtls_key=None,
        verify_ca=True,
    )
    return UpdateClientService(
        api_config=api_config,
        update_client_api_config=api_config,
        resource_map_service=MagicMock(),
        cache_provider=CacheProvider(config=ConfigExternalCache()),
        validate_capability_statement=False,
    )


@pytest.fixture
def directory_dto() -> DirectoryDto:
    return DirectoryDto(
        id="D1",
        ura="12345678",
        endpoint_address="https://directory.example/fhir",
    )


def test_update_resource_uses_bulk_exists_and_deduplicates_before_calling_update_page(
    update_client_service: UpdateClientService, directory_dto: DirectoryDto
) -> None:
    # History contains duplicates of the same resource id
    history = [
        BundleEntry.model_construct(fullUrl="urn:uuid:1", resource=Organization.model_construct(id="A")),
        BundleEntry.model_construct(fullUrl="urn:uuid:2", resource=Organization.model_construct(id="A")),
        BundleEntry.model_construct(fullUrl="urn:uuid:3", resource=Organization.model_construct(id="B")),
    ]

    fake_api = MagicMock()
    fake_api.validate_capability_statement.return_value = True
    fake_api.build_history_params.return_value = {"_count": "50"}
    fake_api.get_history_batch.side_effect = [(None, history)]

    fake_cache = _FakeCache()

    with patch("app.services.update.update_client_service.FhirApi", return_value=fake_api):
        update_client_service.update_page = MagicMock(return_value=[])  # type: ignore[method-assign]
        update_client_service._UpdateClientService__clear_and_add_nodes = MagicMock(return_value=0)  # type: ignore[attr-defined]

        update_client_service.update_resource(
            directory=directory_dto,
            resource_type="Organization",
            cache_service=fake_cache,  # type: ignore[arg-type]
            since=None,
            ura_whitelist={},
        )

    assert fake_cache.bulk_exists_called_with == ["Organization/A", "Organization/B"]
    update_client_service.update_page.assert_called_once()
    args, _kwargs = update_client_service.update_page.call_args
    # targets should contain only 2 unique entries (A and B)
    assert len(args[2]) == 2


def test_update_resource_skips_update_page_when_all_entries_already_processed(
    update_client_service: UpdateClientService, directory_dto: DirectoryDto
) -> None:
    history = [
        BundleEntry.model_construct(fullUrl="urn:uuid:1", resource=Organization.model_construct(id="A")),
        BundleEntry.model_construct(fullUrl="urn:uuid:2", resource=Organization.model_construct(id="B")),
    ]

    fake_api = MagicMock()
    fake_api.validate_capability_statement.return_value = True
    fake_api.build_history_params.return_value = {"_count": "50"}
    fake_api.get_history_batch.side_effect = [(None, history)]

    fake_cache = _FakeCache()
    fake_cache.bulk_exists = MagicMock(return_value={"Organization/A", "Organization/B"})  # type: ignore[assignment]

    with patch("app.services.update.update_client_service.FhirApi", return_value=fake_api):
        update_client_service.update_page = MagicMock(return_value=[])  # type: ignore[method-assign]
        update_client_service._UpdateClientService__clear_and_add_nodes = MagicMock(return_value=0)  # type: ignore[attr-defined]

        update_client_service.update_resource(
            directory=directory_dto,
            resource_type="Organization",
            cache_service=fake_cache,  # type: ignore[arg-type]
            since=None,
            ura_whitelist={},
        )

    update_client_service.update_page.assert_not_called()

def test_update_resource_does_not_fallback_to_individual_key_exists_for_history_filtering(
    update_client_service: UpdateClientService, directory_dto: DirectoryDto
):
    history = [
        BundleEntry.model_construct(fullUrl=f"urn:uuid:{i}", resource=Organization.model_construct(id=str(i)))
        for i in range(100)
    ] + [
        BundleEntry.model_construct(fullUrl="urn:uuid:A1", resource=Organization.model_construct(id="A")),
        BundleEntry.model_construct(fullUrl="urn:uuid:B1", resource=Organization.model_construct(id="B")),
        BundleEntry.model_construct(fullUrl="urn:uuid:A2", resource=Organization.model_construct(id="A")),
    ]

    fake_api = MagicMock()
    fake_api.validate_capability_statement.return_value = True
    fake_api.build_history_params.return_value = {"_count": "50"}
    fake_api.get_history_batch.side_effect = [(None, history)]

    cache_service = MagicMock()
    cache_service.bulk_exists = MagicMock(return_value=set())
    cache_service.key_exists = MagicMock(return_value=False)

    with patch("app.services.update.update_client_service.FhirApi", return_value=fake_api):
        update_client_service.update_page = MagicMock(return_value=[])  # type: ignore[method-assign]
        update_client_service._UpdateClientService__clear_and_add_nodes = MagicMock(return_value=0)  # type: ignore[attr-defined]

        update_client_service.update_resource(
            directory=directory_dto,
            resource_type="Organization",
            cache_service=cache_service,
            since=None,
            ura_whitelist={},
        )

    assert cache_service.bulk_exists.call_count == 1
    assert cache_service.key_exists.call_count == 0
