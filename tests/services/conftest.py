from fhir.resources.R4B.bundle import Bundle
import pytest
from typing import Dict, Any
from app.config import Config, set_config
from app.db.db import Database
from app.services.api.fhir_api import FhirApi
from app.services.entity.resource_map_service import ResourceMapService
from app.services.entity.supplier_ignored_directory_service import (
    SupplierIgnoredDirectoryService,
)
from app.services.fhir.fhir_service import FhirService
from app.services.api.authenticators.null_authenticator import NullAuthenticator

from app.services.update.adjacency_map_service import AdjacencyMapService
from tests.test_config import get_test_config


@pytest.fixture()
def config() -> Config:
    return get_test_config()


@pytest.fixture
def supplier_ignored_directory_service(
    database: Database, config: Config
) -> SupplierIgnoredDirectoryService:
    set_config(config)
    return SupplierIgnoredDirectoryService(database=database)


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
def fhir_service() -> FhirService:
    return FhirService(strict_validation=False)


@pytest.fixture()
def null_authenticator() -> NullAuthenticator:
    return NullAuthenticator()


@pytest.fixture()
def fhir_api(config: Config, null_authenticator: NullAuthenticator) -> FhirApi:
    return FhirApi(
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        auth=null_authenticator,
        url="http://example.com/fhir",
        request_count=10,
        strict_validation=False,
    )


@pytest.fixture()
def adjacency_map_service(
    mock_supplier_id: str,
    fhir_service: FhirService,
    fhir_api: FhirApi,
    resource_map_service: ResourceMapService,
) -> AdjacencyMapService:
    return AdjacencyMapService(
        supplier_id=mock_supplier_id,
        fhir_service=fhir_service,
        supplier_api=fhir_api,
        consumer_api=fhir_api,
        resource_map_service=resource_map_service,
    )
