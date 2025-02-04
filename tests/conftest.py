from typing import Any

from collections.abc import Generator
import pytest

from app.db.db import Database
from app.services.entity_services.mock_api import MockApi
from app.services.entity_services.supplier_service import SupplierService
from app.services.entity_services.resource_map_service import ResourceMapService


@pytest.fixture()
def database() -> Generator[Database, Any, None]:
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e


@pytest.fixture
def supplier_service() -> SupplierService:
    return SupplierService(MockApi())


@pytest.fixture
def resource_map_service(database: Database) -> ResourceMapService:
    return ResourceMapService(database=database)
