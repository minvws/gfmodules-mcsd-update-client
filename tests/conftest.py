import os
import shutil
from typing import Any, Dict
import copy
import pathlib
from typing import List

from collections.abc import Generator
from fastapi import FastAPI
from fastapi.testclient import TestClient
import inject
import pytest

from app.application import create_fastapi_app
from app.container import get_database
from app.db.db import Database
from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference
from app.services.fhir.bundle.parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.update.computation_service import ComputationService
from tests.mock_data import organization, endpoint, location
from tests.utils.utils import create_mock_node


# Don't search for tests in the mock_data directory
def pytest_ignore_collect(collection_path: pathlib.Path, config: Any) -> bool:
    return "tests/mock_data" in str(collection_path)


# Disable the test if running in GitHub Actions
def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        if "test_stress_test_update" in str(items):
            items[:] = [
                item for item in items if "test_stress_test_update" not in str(item)
            ]


@pytest.fixture
def database() -> Generator[Database, Any, None]:
    db = None
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e
    finally:
        if db is not None:
            db.engine.dispose()


@pytest.fixture
def fastapi_app() -> Generator[FastAPI, None, None]:
    app = create_fastapi_app()
    db = get_database()
    db.generate_tables()
    yield app
    db.engine.dispose()
    inject.clear()


@pytest.fixture
def api_client(fastapi_app: FastAPI) -> TestClient:
    return TestClient(fastapi_app)


@pytest.fixture()
def fhir_service() -> FhirService:
    return FhirService(fill_required_fields=False)


@pytest.fixture()
def computation_service(
    mock_directory_id: str,
) -> ComputationService:
    return ComputationService(
        directory_id=mock_directory_id,
    )


# Disable this if you want the mock data to persist after tests
@pytest.fixture(scope="session", autouse=True)
def cleanup_after_tests() -> Generator[None, None, None]:
    yield
    if os.path.exists("tests/mock_data"):
        shutil.rmtree("tests/mock_data")


@pytest.fixture()
def mock_org() -> Dict[str, Any]:
    return copy.deepcopy(organization)


@pytest.fixture()
def mock_ep() -> Dict[str, Any]:
    return endpoint


@pytest.fixture()
def mock_location() -> Dict[str, Any]:
    return location


@pytest.fixture()
def mock_directory_id() -> str:
    return "example-directory-id"


@pytest.fixture()
def org_node_references(mock_ep: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_ep["id"], resource_type=mock_ep["resourceType"])]


@pytest.fixture()
def ep_node_references(mock_org: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_org["id"], resource_type=mock_org["resourceType"])]


@pytest.fixture()
def location_node_references(mock_org: Dict[str, Any]) -> List[NodeReference]:
    return [NodeReference(id=mock_org["id"], resource_type=mock_org["resourceType"])]


@pytest.fixture()
def mock_org_bundle_entry(mock_org: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service.com/fhir/Organization/{mock_org['id']}",
        "resource": mock_org,
        "request": {"method": "PUT", "url": f"Organization/{mock_org['id']}"},
    }


@pytest.fixture()
def mock_ep_bundle_entry(mock_ep: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service/fhir/Endpoint/{mock_ep['id']}",
        "resource": mock_ep,
        "request": {"method": "PUT", "url": f"Endpoint/{mock_ep['id']}"},
    }


@pytest.fixture()
def mock_loc_bundle_entry(mock_location: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "fullUrl": f"http://example-service/fhir/Location/{mock_location['id']}",
        "resource": mock_location,
        "request": {"method": "PUT", "url": f"Location/{mock_location['id']}"},
    }


@pytest.fixture()
def mock_node_org(
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
def mock_node_ep(
    mock_ep_bundle_entry: Dict[str, Any],
    ep_node_references: List[NodeReference],
    fhir_service: FhirService,
    computation_service: ComputationService,
) -> Node:
    entry = create_bundle_entry(mock_ep_bundle_entry)
    return create_mock_node(
        bundle_entry=entry,
        node_refs=ep_node_references,
        fhir_service=fhir_service,
        directory_hash=computation_service.hash_directory_entry(entry),
    )


@pytest.fixture()
def adjacency_map(mock_node_org: Node, mock_node_ep: Node) -> AdjacencyMap:
    return AdjacencyMap(nodes=[mock_node_org, mock_node_ep])
