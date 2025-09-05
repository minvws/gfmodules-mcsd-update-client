from uuid import uuid4
from fhir.resources.R4B.bundle import Bundle
import pytest
from typing import Dict, Any, List
from app.config import Config, ConfigExternalCache, set_config
from app.db.db import Database
from app.models.adjacency.node import Node, NodeReference
from app.services.api.fhir_api import FhirApi
from app.services.entity.resource_map_service import ResourceMapService
from app.services.entity.ignored_directory_service import (
    IgnoredDirectoryService,
)
from app.services.entity.directory_cache_service import DirectoryCacheService
from app.services.entity.directory_info_service import DirectoryInfoService
from app.services.fhir.bundle.bundle_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.api.authenticators.null_authenticator import NullAuthenticator

from app.services.update.adjacency_map_service import AdjacencyMapService
from app.services.update.cache.external import ExternalCachingService
from app.services.update.cache.in_memory import InMemoryCachingService
from app.services.update.cache.provider import CacheProvider
from app.services.update.computation_service import ComputationService
from tests.test_config import get_test_config
from tests.utils.utils import create_mock_node


@pytest.fixture()
def config() -> Config:
    return get_test_config()


@pytest.fixture
def ignored_directory_service(
    database: Database, config: Config
) -> IgnoredDirectoryService:
    set_config(config)
    return IgnoredDirectoryService(database=database)


@pytest.fixture()
def directory_info_service(database: Database, config: Config) -> DirectoryInfoService:
    set_config(config)
    return DirectoryInfoService(database=database, directory_stale_timeout_seconds=0)


@pytest.fixture()
def directory_cache_service(
    database: Database, config: Config
) -> DirectoryCacheService:
    set_config(config)
    return DirectoryCacheService(database=database)


@pytest.fixture()
def resource_map_service(database: Database, config: Config) -> ResourceMapService:
    set_config(config)
    return ResourceMapService(database=database)


@pytest.fixture()
def mock_bundle_request(
    mock_org: Dict[str, Any], mock_ep: Dict[str, Any], fhir_service: FhirService
) -> Bundle:
    return fhir_service.create_bundle(
        {
            "type": "batch",
            "entry": [
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
                {"request": {"method": "GET", "url": f"Endpoint/{mock_ep['id']}"}},
            ],
        }
    )

@pytest.fixture()
def mock_bundle_with_errors_request(
    mock_org: Dict[str, Any], mock_ep: Dict[str, Any], fhir_service: FhirService
) -> Bundle:
    return fhir_service.create_bundle(
        {
            "type": "batch",
            "entry": [
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
                {"request": {"method": "GET", "url": f"Organization/{mock_org['id']}"}},
            ],
        }
    )



@pytest.fixture()
def mock_bundle_response(
    mock_org: Dict[str, Any], mock_ep: Dict[str, Any], fhir_service: FhirService
) -> Bundle:
    return fhir_service.create_bundle(
        {
            "type": "batch-response",
            "entry": [
                {"resource": mock_org, "response": {"status": "200 OK"}},
                {"resource": mock_ep, "response": {"status": "200 OK"}},
            ],
        }
    )

@pytest.fixture()
def mock_bundle_with_errors_response(
    mock_org: Dict[str, Any], mock_ep: Dict[str, Any], fhir_service: FhirService
) -> Bundle:
    return fhir_service.create_bundle(
        {
            "type": "batch-response",
            "entry": [
                {"resource": mock_org, "response": {"status": "200 OK"}},
                {"resource": mock_org, "response": {"status": "404 Not Found", "outcome": {"issue": [
                    {"severity": "error", "code": "not-found", "diagnostics": "Resource not found"},
                    {"severity": "warning", "code": "le-warn", "diagnostics": "Just a warning"},
                    {"severity": "fatal", "code": "le-fatal", "diagnostics": "Fatal stuff happened"},
                ]}}},
                {"resource": mock_org, "response": {"status": "200 OK"}},
                {"resource": mock_org, "response": {"status": "404 Not Found", "outcome": {"issue": [{"severity": "error", "code": "not-found", "diagnostics": "Resource not found"}]}}},
                {"resource": mock_org, "response": {"status": "200 OK"}},
            ],
        }
    )


@pytest.fixture()
def org_history_entry_1(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "PUT", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def org_history_entry_2(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "POST", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def ep_history_entry(mock_ep: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Endpoint/{mock_ep['id']}",
        "resource": mock_ep,
        "request": {"method": "POST", "url": f"Endpoint/{mock_ep['id']}"},
    }


@pytest.fixture()
def mock_org_history_bundle(
    org_history_entry_1: Dict[str, Any], org_history_entry_2: Dict[str, Any]
) -> Dict[str, Any]:
    bundle = {
        "type": "history",
        "entry": [org_history_entry_1, org_history_entry_2],
    }

    return bundle


@pytest.fixture
def mock_bundle_of_bundles(
    mock_org_bundle_entry: Dict[str, Any], mock_ep_bundle_entry: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "type": "batch",
        "entry": [
            {
                "resource": {
                    "resourceType": "Bundle",
                    "type": "history",
                    "entry": [
                        mock_org_bundle_entry,
                    ],
                }
            },
            {
                "resource": {
                    "resourceType": "Bundle",
                    "type": "history",
                    "entry": [
                        mock_ep_bundle_entry,
                    ],
                }
            },
        ],
    }


@pytest.fixture()
def mock_ep_history_bundle(ep_history_entry: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "history", "entry": [ep_history_entry]}


@pytest.fixture()
def expected_node_org(
    mock_org_bundle_entry: Dict[str, Any],
    org_node_references: List[NodeReference],
    fhir_service: FhirService,
    computation_service: ComputationService,
) -> Node:
    entry = create_bundle_entry(mock_org_bundle_entry)
    return create_mock_node(
        bundle_entry=entry,
        node_refs=org_node_references,
        fhir_service=fhir_service,
        directory_hash=computation_service.hash_directory_entry(entry),
    )


@pytest.fixture()
def null_authenticator() -> NullAuthenticator:
    return NullAuthenticator()


@pytest.fixture()
def fhir_api(config: Config, null_authenticator: NullAuthenticator) -> FhirApi:
    return FhirApi(
        timeout=config.directory_api.timeout,
        backoff=config.directory_api.backoff,
        retries=config.directory_api.retries,
        auth=null_authenticator,
        base_url="http://example.com/fhir",
        request_count=10,
        fill_required_fields=False,
    )


@pytest.fixture()
def in_memory_cache_service() -> InMemoryCachingService:
    return InMemoryCachingService(uuid4())


@pytest.fixture()
def external_caching_service() -> ExternalCachingService:
    return ExternalCachingService(
        run_id=uuid4(), host="http://example.com", port=8000, ssl_check_hostname=False
    )


@pytest.fixture()
def mock_config_external_cache() -> ConfigExternalCache:
    return ConfigExternalCache(host="http://example.com", port=8000)


@pytest.fixture()
def cache_provider(mock_config_external_cache: ConfigExternalCache) -> CacheProvider:
    return CacheProvider(config=mock_config_external_cache)


@pytest.fixture()
def adjacency_map_service(
    mock_directory_id: str,
    fhir_service: FhirService,
    fhir_api: FhirApi,
    resource_map_service: ResourceMapService,
    in_memory_cache_service: InMemoryCachingService,
) -> AdjacencyMapService:
    return AdjacencyMapService(
        directory_id=mock_directory_id,
        fhir_service=fhir_service,
        directory_api=fhir_api,
        update_client_api=fhir_api,
        resource_map_service=resource_map_service,
        cache_service=in_memory_cache_service,
    )
