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
from app.services.entity_services.mock_api import MockApi
from app.services.entity_services.supplier_service import SupplierService
from app.services.entity_services.resource_map_service import ResourceMapService
from test_config import get_test_config
import os


def pytest_ignore_collect(path: str, config: Any) -> bool:
    return "tests/mock_data" in str(path)

def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if os.getenv('GITHUB_ACTIONS') == 'true':
        items[:] = [item for item in items if "test_consumer_update" not in item.nodeid]

@pytest.fixture
def database() -> Generator[Database, Any, None]:
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e


@pytest.fixture
def supplier_service() -> SupplierService:
    set_config(get_test_config())
    return SupplierService(MockApi())


@pytest.fixture
def resource_map_service(database: Database) -> ResourceMapService:
    set_config(get_test_config())
    return ResourceMapService(database=database)


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
