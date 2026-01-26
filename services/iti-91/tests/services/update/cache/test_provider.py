from unittest.mock import patch, MagicMock

import pytest

from app.config import ConfigExternalCache
from app.services.update.cache.external import ExternalCachingService
from app.services.update.cache.in_memory import InMemoryCachingService
from app.services.update.cache.provider import CacheProvider

PATCHED_MODULE = "app.services.update.cache.provider.ExternalCachingService.is_healthy"


@pytest.fixture()
def cache_provier_without_config() -> CacheProvider:
    return CacheProvider(config=ConfigExternalCache())


@patch(PATCHED_MODULE)
def test_create_should_return_external_cache_instance(
    mock_is_healthy: MagicMock,
    cache_provider: CacheProvider,
) -> None:
    mock_is_healthy.return_value = True
    cache_instance = cache_provider.create()

    assert isinstance(cache_instance, ExternalCachingService)


@patch(PATCHED_MODULE)
def test_create_should_return_in_memory_instance_when_external_is_not_healthy(
    mock_is_healthy: MagicMock, cache_provider: CacheProvider
) -> None:
    mock_is_healthy.return_value = False
    cache_instance = cache_provider.create()

    assert isinstance(cache_instance, InMemoryCachingService)


def test_create_should_return_in_memory_when_config_is_missing(
    cache_provier_without_config: CacheProvider,
) -> None:
    cache_instance = cache_provier_without_config.create()

    assert isinstance(cache_instance, InMemoryCachingService)
