from datetime import datetime
from typing import Any, Dict
from fastapi.encoders import jsonable_encoder
import requests


class FhirRequestService:
    def __init__(self, timeout: int, backoff: float):
        self.timeout = timeout
        self.backoff = backoff

    def get_resource(
        self, resource_type: str, url: str, resource_id: str
    ) -> Dict[str, Any]:
        response = requests.get(
            f"{url}/{resource_type}/{resource_id}", timeout=self.timeout
        )
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return results

    def search_for_resource(
        self,
        resource_type: str,
        url: str,
        resource_params: dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        response = requests.get(
            f"{url}/{resource_type}/_search",
            params=resource_params,
            timeout=self.timeout,
        )
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return results

    def post_resource(
        self, resource_type: str, url: str, resource: dict[str, Any]
    ) -> Dict[str, Any]:
        response = requests.post(
            f"{url}/{resource_type}",
            json=jsonable_encoder(resource),
            timeout=self.timeout,
        )
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return results

    def get_resource_history(
        self,
        resource_type: str,
        url: str,
        resource_id: str | None = None,
        _since: datetime | None = None,
    ) -> Dict[str, Any]:
        url = (
            f"{url}/{resource_type}/{resource_id}/_history"
            if resource_id is not None
            else f"{url}/{resource_type}/_history"
        )
        response = requests.get(
            url,
            timeout=self.timeout,
            params={"_since": _since} if _since is not None else None,  # type: ignore
        )
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return results

    def put_resource(
        self, resource_type: str, url: str, resource_id: str, resource: dict[str, Any]
    ) -> Dict[str, Any]:
        response = requests.put(
            f"{url}/{resource_type}/{resource_id}",
            json=jsonable_encoder(resource),
            timeout=self.timeout,
        )
        if response.status_code > 300:
            raise Exception(response.json())

        results: Dict[str, Any] = response.json()
        return results

    def delete_resource(self, resource_type: str, url: str, resource_id: str) -> None:
        response = requests.delete(
            f"{url}/{resource_type}/{resource_id}",
            timeout=self.timeout,
        )
        if response.status_code > 300:
            raise Exception(response.json())
