import pathlib
from typing import Any

from collections.abc import Generator
from fastapi import FastAPI
from fastapi.testclient import TestClient
import inject
import pytest

from app.application import create_fastapi_app
from app.config import set_config
from app.container import get_database
from app.db.db import Database
from app.services.entity.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
import os
import shutil

from get_test_config import get_test_config


def pytest_ignore_collect(collection_path: pathlib.Path, config: Any) -> bool:
    return "tests/mock_data" in str(collection_path)

def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if os.getenv('GITHUB_ACTIONS') == 'true':
        if "test_consumer_update_with_timing" in str(items):
            items[:] = [item for item in items if "test_consumer_update_with_timing" not in str(item)]

@pytest.fixture
def database() -> Generator[Database, Any, None]:
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e


@pytest.fixture()
def resource_map_service(database: Database) -> ResourceMapService:
    set_config(get_test_config())
    return ResourceMapService(database=database)


@pytest.fixture()
def fhir_service() -> FhirService:
    return FhirService(strict_validation=False)


@pytest.fixture()
def fhir_service_strict_validation() -> FhirService:
    return FhirService(strict_validation=True)

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

# Disable this if you want the mock data to persist after tests
@pytest.fixture(scope="session", autouse=True)
def cleanup_after_tests() -> Generator[None, None, None]:
    yield
    if os.path.exists("tests/mock_data"):
        shutil.rmtree("tests/mock_data")
