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

    - If an external cache is configured *and* healthy, use Redis.
    - Otherwise fall back to in-memory cache.
    """

    def __init__(self, config: ConfigExternalCache) -> None:
        self.__config = config

    def create(self) -> CachingService:
        run_id = uuid4()
        namespace = self.__config.default_cache_namespace or "mcsd"
        ttl = self.__config.object_ttl

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
                namespace=namespace,
                object_ttl_seconds=ttl,
            )
            if external_cache_instance.is_healthy():
                logger.info(
                    "Creating external cache instance. run_id=%s namespace=%s ttl=%s",
                    run_id,
                    namespace,
                    ttl,
                )
                return external_cache_instance

            logger.warning(
                "External cache configured but unhealthy; falling back to in-memory. run_id=%s",
                run_id,
            )

        logger.info(
            "Creating in-memory cache instance. run_id=%s namespace=%s ttl=%s",
            run_id,
            namespace,
            ttl,
        )
        return InMemoryCachingService(run_id=run_id, namespace=namespace, object_ttl_seconds=ttl)
