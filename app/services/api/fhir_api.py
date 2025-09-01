from datetime import datetime
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from typing import Any, Dict, List
import logging
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource
from yarl import URL
from app.services.fhir.bundle.bundle_utils import filter_history_entries
from app.services.api.api_service import HttpService
from app.services.api.authenticators.authenticator import Authenticator
from app.services.fhir.fhir_service import FhirService

from fhir.resources.R4B.bundle import Bundle

logger = logging.getLogger(__name__)

ERROR_SEVERITIES = {"error", "fatal"}


def _issues_has_error(outcome: dict[str, Any]) -> bool:
    """
    Determine if an issue has an error
    """
    issues = (outcome or {}).get("issue") or []
    return any((i.get("severity", "").lower() in ERROR_SEVERITIES) for i in issues if isinstance(i, dict))


def _collect_errors(bundle: dict[str, Any]) -> List[dict[str, Any]]:
    """
    Collect errors from bundle
    """
    errs = []
    for idx, e in enumerate(bundle.get("entry", []) or []):
        resp = e.get("response") or {}
        status_str = str(resp.get("status", "")).strip()
        status_code = None
        if status_str:
            try:
                status_code = int(status_str.split()[0])
            except ValueError:
                pass

        if status_code and status_code >= 400:
            errs.append({"entry": idx, "status": status_code, "reason": f"HTTP error: {status_code}"})
        elif _issues_has_error(resp.get("outcome")):  # type: ignore[arg-type]
            errs.append({"entry": idx, "status": status_code, "reason": "OperationOutcome error"})

        res = e.get("resource")
        if isinstance(res, dict) and res.get("resourceType") == "OperationOutcome" and _issues_has_error(res):
            errs.append({"entry": idx, "status": status_code, "reason": "Resource-level OperationOutcome error"})
    return errs


class FhirApi(HttpService):
    def __init__(
        self,
        base_url: str,
        timeout: int,
        backoff: float,
        auth: Authenticator,
        request_count: int,
        fill_required_fields: bool,
        retries: int,
        mtls_cert: str | None = None,
        mtls_key: str | None = None,
        mtls_ca: str | None = None,
    ):
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            backoff=backoff,
            retries=retries,
            authenticator=auth,
            mtls_ca=mtls_ca,
            mtls_cert=mtls_cert,
            mtls_key=mtls_key,
        )
        self.request_count = request_count
        self.__fhir_service = FhirService(fill_required_fields)

    def post_bundle(self, bundle: Bundle) -> Bundle:
        try:
            response = self.do_request(
                "POST", json=jsonable_encoder(bundle.model_dump())
            )
        except Exception as e:
            logging.error("PostBundle error: %s", e)
            raise HTTPException(status_code=500, detail=str(e))

        # Make sure we have a valid HTTP response status
        if response.status_code < 200 or response.status_code > 299:
            # See if we can get an Operation Outcome from the error response
            try:
                data = response.json()
            except Exception:
                # No json data found, just return generic error
                logging.error("PostBundle error: %s", response.text)
                raise HTTPException(status_code=500, detail="HTTP error")

            # Show details if we have a correct json output
            if isinstance(data, dict) and data.get("resourceType") == "OperationOutcome" and _issues_has_error(data):
                logging.error("PostBundle error: %s", data)
                raise HTTPException(status_code=response.status_code, detail=data)

            # Json data is found, but it doesn't seem like an operation outcome. Just return a global error
            logging.error("PostBundle error: %s", data)
            raise HTTPException(status_code=response .status_code, detail="HTTP error")


        # Sucecssful HTTP status. Check if we have a JSON body
        try:
            data = response.json()
        except Exception:
            logger.error("PostBundle error: %s", response.text)
            raise HTTPException(status_code=response.status_code, detail="HTTP error")

        # Check for errors, if any
        # if isinstance(data, dict) and data.get('resourceType') == "Bundle":
            # print(data)
            # entry_errs = _collect_errors(data)
            # if entry_errs:
            #     logging.error("Bundle error: %s", entry_errs)
            #     raise HTTPException(status_code=response.status_code, detail=entry_errs)

        return self.__fhir_service.create_bundle(data)

    def search_resource(
        self, resource_type: str, params: dict[str, Any]
    ) -> tuple[URL | None, List[BundleEntry]]:
        """
        Search for resources of a given type with specified parameters. Will return a tuple containing the next URL (if available)
        and a list of BundleEntry objects. If the next url is empty, there are no more pages to fetch
        """
        response = self.do_request(
            method="GET", sub_route=f"{resource_type}/_search", params=params
        )
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_url = None
        entries = page_bundle.entry if page_bundle.entry else []
        if page_bundle.link is not None and len(page_bundle.link) > 0:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_url = URL(link.url)  # type: ignore[attr-defined]

        return next_url, entries

    def get_resource_by_id(
        self, resource_type: str, resource_id: str
    ) -> DomainResource:
        response = self.do_request(
            method="GET", sub_route=f"{resource_type}/{resource_id}"
        )
        if response.status_code == 410:
            logger.warning(
                f"Resource {resource_type}/{resource_id} has been deleted from the server."
            )
            raise HTTPException(status_code=410, detail="Resource has been deleted.")

        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        data = response.json()
        return self.__fhir_service.create_resource(data)

    def build_history_params(self, since: datetime | None = None) -> Dict[str, Any]:
        """
        Builds the params based on request_count and _since parameter.
        """
        params: Dict[str, int | str] = {"_count": str(self.request_count)}
        if since:
            params["_since"] = since.isoformat()

        return params

    def get_next_params(self, url: URL) -> Dict[str, Any]:
        """
        Helper function to extract query parameter from URL.
        """
        query = url.query
        return dict(query)

    def get_history_batch(
        self,
        resource_type: str,
        next_params: Dict[str, Any] | None = None,
    ) -> tuple[Dict[str, Any] | None, List[BundleEntry]]:
        """
        Fetch a batch of resource history entries from the given URL. Will return a tuple containing the next parameter (if available)
        and a list of BundleEntry objects. If the next params is empty, there are no more pages to fetch
        """
        response = self.do_request(
            "GET", sub_route=f"{resource_type}/_history", params=next_params
        )
        if response.status_code > 300:
            logger.error(
                f"An error with status code {response.status_code} has occurred from server. See response:\n{response.json()}"
            )
            raise HTTPException(status_code=500, detail=response.json())

        page_bundle = self.__fhir_service.create_bundle(response.json())
        next_params = None
        entries = filter_history_entries(page_bundle.entry) if page_bundle.entry else []
        if page_bundle.link is not None and len(page_bundle.link) > 0:
            for link in page_bundle.link:
                if link.relation == "next":  # type: ignore[attr-defined]
                    next_params = self.get_next_params(URL(link.url))  # type: ignore[attr-defined]

        return next_params, entries
