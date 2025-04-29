from yarl import URL
from typing import Dict, List, Sequence, Any, Tuple
from app.models.supplier.dto import SupplierDto
from app.services_new.api.api_service import ApiService

class SupplierProvider:
    def __init__(self, supplier_urls: List[Tuple[str,str,str]]|None = None, supplier_provider_url: str|None = None) -> None:
        self._supplier_urls = supplier_urls
        self._supplier_provider_url = supplier_provider_url
        if self._supplier_urls is None and self._supplier_provider_url is None:
            raise ValueError("Either supplier_urls or supplier_provider_url must be provided")

class SuppliersApi(ApiService):
    def __init__(self, timeout: int, backoff: float, supplier_provider: SupplierProvider) -> None:
        super().__init__(timeout, backoff, 5)
        self.__supplier_provider_url = supplier_provider._supplier_provider_url
        self.__supplier_urls = supplier_provider._supplier_urls

    def get_one(self, supplier_id: str) -> SupplierDto | None:
        if self.__supplier_urls is not None:
            print(f"Using supplier_urls: {self.__supplier_urls}")
            for id, name, endpoint in self.__supplier_urls:
                if id == supplier_id:
                    return SupplierDto(
                        id=id,
                        name=name,
                        endpoint=endpoint,
                    )
            return None
        url = URL(f"{self.__supplier_provider_url}/api/supplier/{supplier_id}")
        try:
            response = self.do_request("GET", url)
            response.raise_for_status()

            results: Dict[str, Any] = response.json()
            return SupplierDto(
                id=results["id"],
                name=results["name"],
                endpoint=results["endpoint"],
            )
        except Exception:
            return None

    def get_all(self) -> Sequence[SupplierDto]:
        suppliers = []
        if self.__supplier_urls is not None:
            for id, name, endpoint in self.__supplier_urls:
                suppliers.append(
                    SupplierDto(
                        id=id,
                        name=name,
                        endpoint=endpoint,
                    )
                )
            return suppliers

        url = URL(f"{self.__supplier_provider_url}/api/suppliers")
        while True:
            response = self.do_request("GET", url)
            if response.status_code > 300:
                raise Exception(response.json())

            results: Dict[str, Any] = response.json()
            for result in results["data"]:
                suppliers.append(
                    SupplierDto(
                        id=result["id"],
                        name=result["name"],
                        endpoint=result["endpoint"],
                    )
                )

            if results["next_page_url"] is None:
                break

            url = URL(results["next_page_url"])

        return suppliers
