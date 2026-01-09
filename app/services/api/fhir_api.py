from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Dict, List

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from fhir.resources.R4B.domainresource import DomainResource
from requests import JSONDecodeError, Response
from yarl import URL

from app.models.fhir.types import BundleError
from app.services.api.api_service import HttpService
from app.services.api.authenticators.authenticator import Authenticator
from app.services.fhir.bundle.utils import filter_history_entries
from app.services.fhir.capability_statement_validator import is_capability_statement_valid
from app.services.fhir.fhir_service import FhirService
from app.services.fhir.utils import collect_errors

HTTP_ERR_MSG = "An error occurred while processing a FHIR request."
logger = logging.getLogger(__name__)


@dataclass
class FhirApiConfig:
    base_url: str
    auth: Authenticator
    timeout: int
    retries: int
    backoff: float
    mtls_cert: str | None
    mtls_key: str | None
    verify_ca: str | bool
    request_count: int
    fill_required_fields: bool
    # Optional HTTP client tuning (connection pooling).
    pool_connections: int = 10
    pool_maxsize: int = 10


class FhirApi(HttpService):
    def __init__(self, config: FhirApiConfig):
        super().__init__(
            base_url=config.base_url,
            timeout=config.timeout,
            backoff=config.backoff,
            retries=config.retries,
            authenticator=config.auth,
            verify_ca=config.verify_ca,
            mtls_cert=config.mtls_cert,
            mtls_key=config.mtls_key,
            pool_connections=config.pool_connections,
            pool_maxsize=config.pool_maxsize,
        )
        self.request_count = config.request_count
        self.__fhir_service = FhirService(config.fill_required_fields)

    @staticmethod
    def _safe_json(response: Response) -> Any:
        try:
            return response.json()
        except Exception:
            return response.text

    def post_bundle(self, bundle: Bundle) -> tuple[Bundle, list[BundleError]]:
        """
        Post a FHIR bundle to the server and return the response as a Bundle object.

        Returns:
            (bundle, bundle_errors)
        """
        try:
            response = self.do_request("POST", json=jsonable_encoder(bundle.model_dump(exclude_none=True)))
        except Exception as e:
            logger.exception("FHIR POST bundle failed. base_url=%s", self.base_url)
            raise HTTPException(status_code=500, detail=str(e))

        if response.status_code >= 400:
            logger.error(
                "FHIR POST bundle returned error. status=%s body=%s",
                response.status_code,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        data = self._safe_json(response)
        if isinstance(data, str):
            logger.error("Failed to decode JSON response for bundle: %s", data)
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        parsed_bundle = self.__fhir_service.create_bundle(data)
        bundle_errors = collect_errors(parsed_bundle)
        return parsed_bundle, bundle_errors

    def get_resource_history_by_id(self, resource_type: str, resource_id: str) -> List[BundleEntry]:
        """Fetch a single resource's instance history and return filtered entries."""
        response = self.do_request(method="GET", sub_route=f"{resource_type}/{resource_id}/_history")
        if response.status_code >= 400:
            if response.status_code == 400:
                try:
                    resource = self.get_resource_by_id(resource_type=resource_type, resource_id=resource_id)
                    entry = BundleEntry.model_construct()
                    entry.resource = resource  # type: ignore[assignment]
                    entry.request = BundleEntryRequest.model_construct(method="PUT", url=f"{resource_type}/{resource_id}")  # type: ignore[assignment]
                    return [entry]
                except HTTPException as e:
                    if e.status_code in (404, 410):
                        return []
                except Exception:
                    pass
            logger.error(
                "FHIR instance history fetch failed. status=%s resource=%s/%s body=%s",
                response.status_code,
                resource_type,
                resource_id,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        bundle = self.__fhir_service.create_bundle(response.json())
        return filter_history_entries(bundle.entry) if bundle.entry else []

    def search_resource(
        self, resource_type: str, params: dict[str, Any]
    ) -> tuple[URL | None, List[BundleEntry]]:
        """Search for resources of a given type with specified parameters."""
        response = self.do_request(method="GET", sub_route=f"{resource_type}/_search", params=params)
        if response.status_code >= 400:
            logger.error(
                "FHIR search error. status=%s resource_type=%s body=%s",
                response.status_code,
                resource_type,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_url: URL | None = None
        entries = page_bundle.entry if page_bundle.entry else []
        if page_bundle.link:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_url = self._resolve_next_url(link.url)  # type: ignore[attr-defined]
        return next_url, entries

    def search_resource_page(
        self, next_url: URL
    ) -> tuple[URL | None, List[BundleEntry]]:
        """Fetch the next page from a search bundle's 'next' link."""
        response = self.do_request_url(method="GET", url=next_url)
        if response.status_code >= 400:
            logger.error(
                "FHIR search next page error. status=%s url=%s body=%s",
                response.status_code,
                next_url,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_url_out: URL | None = None
        entries = page_bundle.entry if page_bundle.entry else []
        if page_bundle.link:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_url_out = self._resolve_next_url(link.url)  # type: ignore[attr-defined]
        return next_url_out, entries

    def _resolve_next_url(self, link_url: str) -> URL:
        candidate = URL(link_url)
        if candidate.is_absolute():
            return candidate

        base = URL(self.base_url.rstrip("/"))
        if link_url.startswith("?"):
            return base.with_query(candidate.query)

        if candidate.path.startswith("/"):
            return base.with_path(candidate.path).with_query(candidate.query)

        return base.with_path(
            f"{base.path.rstrip('/')}/{candidate.path.lstrip('/')}"
        ).with_query(candidate.query)

    def get_resource_by_id(self, resource_type: str, resource_id: str) -> DomainResource:
        response = self.do_request(method="GET", sub_route=f"{resource_type}/{resource_id}")

        if response.status_code == 410:
            logger.warning("Resource deleted. %s/%s", resource_type, resource_id)
            raise HTTPException(status_code=410, detail="Resource has been deleted.")

        if response.status_code >= 400:
            logger.error(
                "FHIR get by id error. status=%s resource=%s/%s body=%s",
                response.status_code,
                resource_type,
                resource_id,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        return self.__fhir_service.create_resource(response.json())

    def build_history_params(self, since: datetime | None = None) -> Dict[str, Any]:
        """Build the params based on request_count and the optional `_since` parameter."""
        params: Dict[str, int | str] = {"_count": str(self.request_count)}
        if since:
            params["_since"] = since.isoformat()
        return params

    def validate_capability_statement(self) -> bool:
        """Fetch and validate the FHIR CapabilityStatement from the server."""
        response = self.do_request("GET", sub_route="metadata")
        if response.status_code >= 400:
            logger.error(
                "FHIR CapabilityStatement fetch failed. status=%s body=%s",
                response.status_code,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        try:
            data = response.json()
        except JSONDecodeError:
            logger.error("Failed to decode CapabilityStatement JSON: %s", response.text)
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        return is_capability_statement_valid(data)

    @staticmethod
    def get_next_params(url: URL) -> Dict[str, Any]:
        """Extract query parameters from a URL."""
        return dict(url.query)

    def get_history_batch(
        self,
        resource_type: str,
        next_params: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, List[BundleEntry]]:
        """Fetch a batch of resource history entries."""
        response = self.do_request("GET", sub_route=f"{resource_type}/_history", params=next_params)
        if response.status_code >= 400:
            logger.error(
                "FHIR history fetch failed. status=%s resource_type=%s body=%s",
                response.status_code,
                resource_type,
                self._safe_json(response),
            )
            raise HTTPException(status_code=response.status_code, detail=HTTP_ERR_MSG)

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_params_out: Dict[str, Any] | None = None

        entries = filter_history_entries(page_bundle.entry) if page_bundle.entry else []
        if page_bundle.link:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_params_out = self.get_next_params(URL(link.url))  # type: ignore[attr-defined]

        return next_params_out, entries
