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
from tests.test_config import get_test_config


@pytest.fixture()
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
