from typing import Any, Dict, Final
import pytest
from yarl import URL

from app.config import Config
from app.services.api.api_service import ApiService
from tests.get_test_config import test_config

config: Final[Config] = test_config()


@pytest.fixture()
def mock_url() -> URL:
    return URL("http://example.com")


@pytest.fixture()
def mock_params() -> Dict[str, Any]:
    return {"param": "example"}


@pytest.fixture()
def mock_body() -> Dict[str, Any]:
    return {"example": "some data"}


@pytest.fixture()
def api_service() -> ApiService:
    return ApiService(
        timeout=config.supplier_api.timeout,
        backoff=config.supplier_api.backoff,
        retries=1,
    )
