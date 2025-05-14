import shutil
from typing import Any, Dict
import copy
import pathlib

from collections.abc import Generator
from fastapi import FastAPI
from fastapi.testclient import TestClient
import inject
import pytest

from app.application import create_fastapi_app
from app.config import set_config
from app.container import get_database
from app.db.db import Database
from app.models.adjacency.adjacency_map import AdjacencyMap
from app.models.adjacency.node import Node, NodeReference
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from tests.test_config import get_test_config
from tests.mock_data import organization, endpoint, location
from tests.utils import create_mock_node

# Don't search for tests in the mock_data directory
def pytest_ignore_collect(collection_path: pathlib.Path, config: Any) -> bool:
    return "tests/mock_data" in str(collection_path)

# Disable the test if running in GitHub Actions
def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if os.getenv('GITHUB_ACTIONS') == 'true':
        if "test_stress_test_update" in str(items):
            items[:] = [item for item in items if "test_stress_test_update" not in str(item)]

@pytest.fixture
def database() -> Generator[Database, Any, None]:
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e


@pytest.fixture
def fastapi_app() -> Generator[FastAPI, None, None]:
    set_config(get_test_config())
    app = create_fastapi_app()
    db = get_database()
    db.generate_tables()
    yield app
    inject.clear()


@pytest.fixture
def api_client(fastapi_app: FastAPI) -> TestClient:
    return TestClient(fastapi_app)


@pytest.fixture()
def fhir_service() -> FhirService:
    return FhirService(strict_validation=False)


@pytest.fixture()
def fhir_service_strict_validation() -> FhirService:
    return FhirService(strict_validation=True)


@pytest.fixture()
def mock_org() -> Dict[str, Any]:
    return copy.deepcopy(organization)


@pytest.fixture()
def mock_ep() -> Dict[str, Any]:
    return endpoint


@pytest.fixture()
def mock_location() -> Dict[str, Any]:
    return location

# Disable this if you want the mock data to persist after tests
@pytest.fixture(scope="session", autouse=True)
def cleanup_after_tests() -> Generator[None, None, None]:
    yield
    if os.path.exists("tests/mock_data"):
        shutil.rmtree("tests/mock_data")
