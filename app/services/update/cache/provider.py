import logging
from uuid import uuid4

from app.config import ConfigExternalCache
from app.services.update.cache.caching_service import CachingService
from app.services.update.cache.external import ExternalCachingService
from app.services.update.cache.in_memory import InMemoryCachingService

logger = logging.getLogger(__name__)


class CacheProvider:
    """
    Factory class to create a caching service based on the provided configuration.
    """
    def __init__(self, config: ConfigExternalCache) -> None:
        self.__config = config

    def create(self) -> CachingService:
        run_id = uuid4()

        if self.__config.host is not None and self.__config.port is not None:
            external_cache_instance = ExternalCachingService(
                run_id=run_id,
                host=self.__config.host,
                port=self.__config.port,
                ssl=self.__config.ssl,
                ssl_keyfile=self.__config.key,
                ssl_certfile=self.__config.cert,
                ssl_ca_certs=self.__config.cafile,
                ssl_check_hostname=self.__config.check_hostname,
            )
            healthy_external_cache = external_cache_instance.is_healthy()

            if healthy_external_cache:
                logger.info(f"creating external cache instance with runner id {run_id}")
                return external_cache_instance

        logger.info(
            f"Unable to create external cache instance, defaulting to in memory with run id {run_id}"
        )

        return InMemoryCachingService(run_id)
