import json
from typing import List
from app.models.supplier.dto import SupplierDto
from app.services.api.api_service import ApiService
from app.services.entity.supplier_cache_service import SupplierCacheService
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.api_provider import SupplierApiProvider
from app.services.supplier_provider.caching_provider import CachingSupplierProvider
from app.services.supplier_provider.json_provider import SupplierJsonProvider
from app.services.supplier_provider.supplier_provider import SupplierProvider
from app.config import Config
from app.db.db import Database


class SupplierProviderFactory:
    def __init__(self, config: Config, database: Database) -> None:
        self.__supplier_config = config.supplier_api
        self.__db = database

    def create(self) -> SupplierProvider:
        if self.__supplier_config.supplier_urls_path is not None and len(
            self.__supplier_config.supplier_urls_path
        ) > 1:
            return SupplierJsonProvider(
                suppliers_json_data=SupplierProviderFactory._read_suppliers_file(
                    self.__supplier_config.supplier_urls_path
                ),
                supplier_ignored_directory_service=SupplierIgnoredDirectoryService(self.__db)

            )
        elif self.__supplier_config.suppliers_provider_url is not None:
            supplier_api_provider = SupplierApiProvider(
                supplier_provider_url=self.__supplier_config.suppliers_provider_url,
                api_service=ApiService(
                    timeout=self.__supplier_config.timeout,
                    backoff=self.__supplier_config.backoff,
                    retries=5,
                ),
                supplier_ignored_directory_service=SupplierIgnoredDirectoryService(self.__db)
            )
            supplier_cache_service = SupplierCacheService(self.__db)
            return CachingSupplierProvider(
                supplier_provider=supplier_api_provider,
                supplier_cache_service=supplier_cache_service,
            )
        else:
            raise ValueError(
                "Configuration error: Either 'supplier_urls_path' or 'suppliers_provider_url' must be provided. "
                f"Provided values - supplier_urls_path: {self.__supplier_config.supplier_urls_path}, "
                f"suppliers_provider_url: {self.__supplier_config.suppliers_provider_url}."
            )

    @staticmethod
    def _read_suppliers_file(supplier_urls_path: str) -> List[SupplierDto]:
        try:
            with open(supplier_urls_path) as f:
                supplier_urls: List[SupplierDto] = []
                supplier_data = json.load(f)
                for supplier in supplier_data["suppliers"]:
                    supplier_urls.append(
                        SupplierDto(
                            id=supplier["id"],
                            name=supplier["name"],
                            endpoint=supplier["endpoint"],
                            ura_number=supplier["ura_number"],
                        )
                    )
                return supplier_urls
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error processing suppliers file: {e}")