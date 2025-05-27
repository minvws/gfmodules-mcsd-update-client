import logging
from typing import Any, Dict, List, Sequence

from yarl import URL
from app.db.entities.supplier_ignored_directory import SupplierIgnoredDirectory
from app.models.supplier.dto import SupplierDto
from app.services.api.api_service import ApiService
from app.services.entity.supplier_ignored_directory_service import SupplierIgnoredDirectoryService
from app.services.supplier_provider.supplier_provider import SupplierProvider

logger = logging.getLogger(__name__)


class SupplierApiProvider(SupplierProvider):
    def __init__(self, supplier_provider_url: str, api_service: ApiService, supplier_ignored_directory_service: SupplierIgnoredDirectoryService) -> None:
        self.__supplier_provider_url = supplier_provider_url
        self.__api_service = api_service
        self.__supplier_ignored_directory_service = supplier_ignored_directory_service

    def get_all_suppliers(self, include_ignored: bool = False) -> List[SupplierDto]:
        return self.__fetch_suppliers(include_ignored=include_ignored, include_ignored_ids=None)


    def get_all_suppliers_include_ignored(self, include_ignored_ids: List[str]) -> List[SupplierDto]:
        return self.__fetch_suppliers(include_ignored=False, include_ignored_ids=include_ignored_ids)


    def __fetch_suppliers(
        self,
        include_ignored: bool,
        include_ignored_ids: List[str] | None = None
    ) -> List[SupplierDto]:
        suppliers: List[SupplierDto] = []

        ignored_directories: Sequence[SupplierIgnoredDirectory] = []
        if not include_ignored or include_ignored_ids:
            ignored_directories = self.__supplier_ignored_directory_service.get_all_ignored_directories()

        url = URL(f"{self.__supplier_provider_url}/Endpoint")
        while True:
            try:
                response = self.__api_service.do_request("GET", url)
                response.raise_for_status()

                results: Dict[str, Any] = response.json()
                if not results["data"]:
                    raise ValueError("No data found in the supplier provider")

                for result in results["data"]:
                    id = result["id"]
                    name = result["name"]
                    endpoint = result["endpoint"]

                    is_ignored = any(dir.directory_id == id for dir in ignored_directories)
                    is_included = include_ignored_ids and id in include_ignored_ids

                    if is_ignored and not include_ignored and not is_included:
                        continue

                    suppliers.append(SupplierDto(
                        id=id,
                        name=name,
                        endpoint=endpoint,
                        is_deleted=False,
                    ))

                if results["next_page_url"] is None:
                    break

                url = URL(results["next_page_url"])
            except Exception as e:
                logger.warning(f"Could not retrieve suppliers from provider due to {e}")
                raise e

        return suppliers


    def get_one_supplier(self, supplier_id: str) -> SupplierDto:
        url = URL(f"{self.__supplier_provider_url}/Endpoint/{supplier_id}")
        try:
            response = self.__api_service.do_request("GET", url)
            response.raise_for_status()

            results: Dict[str, Any] = response.json()
            return SupplierDto(
                id=results["id"],
                name=results["name"],
                endpoint=results["endpoint"],
            )
        except Exception as e:
            logger.warning(f"Could not retrieve supplier {supplier_id} from provider due to {e}")
            raise e

    def check_if_supplier_is_deleted(self, supplier_id: str) -> bool:
        """
        Check if the supplier is deleted by checking the deleted_at field in the supplier provider.
        If the supplier provider is not reachable, we assume the supplier is not deleted.
        """
        url = URL(
            f"{self.__supplier_provider_url}/Endpoint/{supplier_id}/_history"
        )
        try:
            response = self.__api_service.do_request("GET", url)
            response.raise_for_status()
            results: Dict[str, Any] = response.json()
            if results["data"] and results["data"][0]["deleted_at"] is not None:
                return True
            return False
        except Exception:
            logger.warning(
                f"Could not check if supplier {supplier_id} is deleted due to provider unavailability"
            )
            return False
