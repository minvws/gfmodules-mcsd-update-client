from typing import Any, Dict, List
from fastapi.encoders import jsonable_encoder
import requests
from urllib.parse import urlparse, parse_qs

from app.models.fhir.r4.types import Bundle


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
        params: dict[str, str] | None = None,
    ) -> Bundle:
        new_url = (
            f"{url}/{resource_type}/{resource_id}/_history"
            if resource_id is not None
            else f"{url}/{resource_type}/_history"
        )

        response = requests.get(new_url, timeout=self.timeout, params=params)
        if response.status_code > 300:
            raise Exception(response.json())

        bundle: Bundle = Bundle(**response.json())

        if bundle.entry is None or len(bundle.entry) == 0:
            raise Exception(response.json())

        if bundle.link is not None and len(bundle.link) > 0:
            bundles: List[Bundle] = []
            for link in bundle.link:
                if link.relation == "next":
                    parsed_url = urlparse(str(link.url))
                    query_params = parse_qs(parsed_url.query)
                    params = query_params  # type: ignore
                    bundles.append(
                        self.get_resource_history(
                            resource_type, url, resource_id, params
                        )
                    )

            for extr_bundle in bundles:
                if extr_bundle.entry is None or len(extr_bundle.entry) == 0:
                    raise Exception(response.json())
                bundle.entry.extend(extr_bundle.entry)

        return bundle

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
