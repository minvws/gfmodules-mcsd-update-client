from typing import Any, Dict

from fhir.resources.R4B.bundle import Bundle

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
    for idx, e in enumerate(bundle.entry or []):
        if e is None or e.response is None:
            continue
        resp = e.response
        status_str = str(resp.status).strip()
        status_code = None
        if status_str:
            try:
                status_code = int(status_str.split()[0])
            except ValueError:
                pass

        if status_code and status_code < 400:
            continue

        resp = e.response or {}
        if not resp.outcome:
            continue
        for issue in resp.outcome.get("issue", []):
            if issue.get("severity") not in ERROR_SEVERITIES:
                continue

            errs.append(BundleError(
                entry=idx,
                status=status_code,
                code=issue.get("code"),
                severity=issue.get("severity"),
                diagnostics=issue.get("diagnostics") or ""
            ))
    return errs
