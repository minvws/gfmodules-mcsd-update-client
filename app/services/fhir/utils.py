from typing import Any, Dict

from fhir.resources.R4B.bundle import Bundle, BundleEntryResponse

from app.models.fhir.types import McsdResources, McsdResourcesWithRequiredFields, BundleError, ERROR_SEVERITIES


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
        resp = entry.response # type: ignore[attr-defined]
        if not resp:
            continue

        status_code = _parse_status_code(resp)
        if status_code and status_code < 400:
            continue

        errs.extend(_build_errors_from_response(idx, status_code, resp))

    return errs


def _parse_status_code(resp: BundleEntryResponse) -> int | None:
    """Extract status code if possible, else None."""
    try:
        return int(str(resp.status).strip().split()[0])
    except (ValueError, AttributeError, IndexError):
        return None


def _build_errors_from_response(idx: int, status_code: int | None, resp: BundleEntryResponse) -> list[BundleError]:
    """Extract BundleErrors from a response."""
    outcome = resp.outcome
    if not outcome:
        return []

    return [
        BundleError(
            entry=idx,
            status=status_code,
            code=issue.get("code"),
            severity=issue.get("severity"),
            diagnostics=issue.get("diagnostics") or "",
        )
        for issue in outcome.get("issue", [])
        if issue.get("severity") in ERROR_SEVERITIES
    ]
