from typing import Any

from collections.abc import Generator
import pytest

from app.db.db import Database
from app.services.entity_services.supplier_service import SupplierService


@pytest.fixture()
def database() -> Generator[Database, Any, None]:
    try:
        db = Database("sqlite:///:memory:")
        db.generate_tables()
        yield db
    except Exception as e:
        raise e


@pytest.fixture
def supplier_service(database: Database) -> SupplierService:
    return SupplierService(database=database)
