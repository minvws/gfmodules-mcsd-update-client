from fhir.resources.R4B.bundle import Bundle
import pytest
import copy
from typing import Dict, Any
from app.config import Config
from app.services.api.fhir_api import FhirApi
from app.services.fhir.fhir_service import FhirService
from app.services.api.authenticators.null_authenticator import NullAuthenticator

from tests.test_config import get_test_config
from tests.services.mock_data import organization, endpoint


@pytest.fixture()
def config() -> Config:
    return get_test_config()


@pytest.fixture
def mock_org() -> Dict[str, Any]:
    return copy.deepcopy(organization)


@pytest.fixture
def mock_ep() -> Dict[str, Any]:
    return copy.deepcopy(endpoint)


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
def history_entry_1(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "PUT", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def history_entry_2(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "POST", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def mock_history_bundle(
    history_entry_1: Dict[str, Any], history_entry_2: Dict[str, Any]
) -> Dict[str, Any]:
    bundle = {
        "type": "history",
        "entry": [history_entry_1, history_entry_2],
    }

    return bundle


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
