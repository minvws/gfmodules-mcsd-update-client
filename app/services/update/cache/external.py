from typing import List
from redis import Redis
from uuid import UUID
from app.models.adjacency.node import Node
from app.services.update.cache.caching_service import CachingService


class ExternalCachingService(CachingService):
    def __init__(
        self,
        run_id: UUID,
        host: str,
        port: int,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_ca_certs: str | None = None,
        ssl_check_hostname: bool = True,
    ) -> None:
        self.run_id = run_id
        self.__redis = Redis(
            host=host,
            port=port,
            db=0,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_ca_certs=ssl_ca_certs,
            ssl_check_hostname=ssl_check_hostname,
        )

    def get_node(self, id: str) -> Node | None:
        target_id = self.make_target_id(id)
        if self.key_exists(target_id) is False:
            return None

        serialized_data = self.__redis.get(target_id)
        return Node.model_validate_json(serialized_data)

    def add_node(self, node: Node) -> None:
        target_id = self.make_target_id(node.resource_id)
        serialized_data = node.model_dump_json()
        self.__redis.set(target_id, serialized_data)

    def key_exists(self, id: str) -> bool:
        target_id = self.make_target_id(id)
        return bool(self.__redis.exists(target_id))

    def clear(self) -> None:
        self.__redis.flushdb()

    def is_healthy(self) -> bool:
        try:
            healthy = self.__redis.ping()
            healthy = True
            return healthy
        except Exception:
            return False

    def keys(self) -> List[str]:
        return [key for key in self.__redis.scan_iter()]
