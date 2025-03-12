from yarl import URL
from typing import Dict, Sequence, Any
from app.models.supplier.dto import SupplierDto
from app.services_new.api.api_service import ApiService


class SuppliersApi(ApiService):
    def __init__(self, url: str, timeout: int, backoff: float) -> None:
        super().__init__(timeout, backoff, 5)
        self.__base_url = url

    def get_one(self, supplier_id: str) -> SupplierDto:
        url = URL(f"{self.__base_url}/api/supplier/{supplier_id}")
        response = self.do_request("GET", url)
        response.raise_for_status()

        results: Dict[str, Any] = response.json()
        return SupplierDto(
            id=results["id"],
            name=results["name"],
            endpoint=results["endpoint"],
        )

    def get_all(self) -> Sequence[SupplierDto]:
        suppliers = []

        url = URL(f"{self.__base_url}/api/suppliers")
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
