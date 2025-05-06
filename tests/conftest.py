from typing import Any

from collections.abc import Generator
import pytest

from app.config import set_config
from app.db.db import Database
from app.services.entity_services.resource_map_service import ResourceMapService
from app.services.fhir.fhir_service import FhirService
from tests.get_test_config import test_config


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
    set_config(test_config())
    return ResourceMapService(database=database)


@pytest.fixture()
def fhir_service() -> FhirService:
    return FhirService(strict_validation=False)


@pytest.fixture()
def fhir_service_strict_validation() -> FhirService:
    return FhirService(strict_validation=True)
