from uuid import uuid4
from app.services.update.cache.caching_service import InMemoryCachingService


class CacheProvider:

    def create(self) -> InMemoryCachingService:
        run_id = uuid4()
        return InMemoryCachingService(run_id)
