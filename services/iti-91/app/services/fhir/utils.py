import logging
from typing import Any, Dict

from fhir.resources.R4B.bundle import Bundle, BundleEntryResponse
from fhir.resources.R4B.operationoutcome import OperationOutcome
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.identifier import Identifier

from app.models.fhir.types import (
    McsdResources,
    McsdResourcesWithRequiredFields,
    BundleError,
    ERROR_SEVERITIES,
)

logger = logging.getLogger(__name__)

ID_SYSTEM_URA = "http://fhir.nl/fhir/namingsystem/ura" # NOSONAR

def validate_resource_type(resource_type: str) -> bool:
    return any(resource_type in r.value for r in McsdResources)


def check_for_required_fields(resource_type: str) -> bool:
    return any(resource_type in r.value for r in McsdResourcesWithRequiredFields)


def get_resource_type(resource: Dict[str, Any]) -> str:
    res_type_key = "resource_type" if "resource_type" in resource else "resourceType"
    resource_type: str = resource[res_type_key]

    return resource_type


def collect_errors(bundle: Bundle) -> list[BundleError]:
    """
    Collect errors from bundle
    """
    errs: list[BundleError] = []
    if not bundle.entry:
        return errs

    for idx, entry in enumerate(bundle.entry):
        resp = entry.response  # type: ignore[attr-defined]
        if not resp:
            continue

        status_code = _parse_status_code(resp)
        if status_code and status_code < 400:
            continue

        errs.extend(_build_errors_from_response(idx, status_code, resp))

    return errs

def get_ura_from_organization(organization: Organization) -> str:
        """
        Extracts the URA from the Organization resource's identifier.
        """
        for identifier in organization.identifier or []:
            if isinstance(identifier, Identifier):
                if identifier.system.lower() == ID_SYSTEM_URA:  # type: ignore
                    if identifier.value is None:  # type: ignore
                        raise ValueError("Identifier value is None")
                    return str(identifier.value)  # type: ignore
        logger.error(f"Organization {organization.id} has no URA identifier")
        raise ValueError("No URA identifier found")



def _parse_status_code(resp: BundleEntryResponse) -> int | None:
    """Extract status code if possible, else None."""
    try:
        return int(str(resp.status).strip().split()[0])
    except (ValueError, AttributeError, IndexError):
        return None


def _build_errors_from_response(
    idx: int, status_code: int | None, resp: BundleEntryResponse
) -> list[BundleError]:
    """Extract BundleErrors from a response."""
    outcome = resp.outcome
    if not outcome:
        return []

    if not isinstance(outcome, OperationOutcome):
        raise ValueError("Outcome is not of type OperationOutcome")

    issues = outcome.issue

    return [
        BundleError(
            entry=idx,
            status=status_code or 0,
            code=str(issue.code) or "",
            severity=issue.severity,
            diagnostics=issue.diagnostics or "",
        )
        for issue in issues
        if issue.severity in ERROR_SEVERITIES
    ]
