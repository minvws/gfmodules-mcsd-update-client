from typing import List
from fhir.resources.R4B.bundle import BundleEntry

from app.models.fhir.types import HttpValidVerbs

HTTP_VERBS: List[HttpValidVerbs] = [
    "GET",
    "POST",
    "PATCH",
    "POST",
    "PUT",
    "HEAD",
    "DELETE",
]


def get_resource_from_reference(reference: str) -> tuple[str | None, str | None]:
    """
    Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
    """
    parts = reference.split("/")
    if len(parts) < 2:
        return None, None

    return parts[-2], parts[-1]


def get_resource_type_and_id_from_entry(
    entry: BundleEntry,
) -> tuple[str, str]:
    """
    Retrieves the resource type and id from a Bundle entry
    """
    if entry.resource is not None:
        return entry.resource.get_resource_type(), entry.resource.id

    if entry.fullUrl is not None:
        return entry.fullUrl.split("/")[-2], entry.fullUrl.split("/")[-1]

    # On DELETE entries, there is no resource anymore. In that case we need to grab the resource type and id from the URL
    if entry.request is not None and entry.request.url is not None:
        return entry.request.url.split("/")[-2], entry.request.url.split("/")[-1]

    raise ValueError("resourceType and id are not available in Bundle")


def get_request_method_from_entry(entry: BundleEntry) -> HttpValidVerbs:
    """
    Retrieves the request method from a Bundle entry if a request is present
    """
    entry_request = entry.request
    if entry_request is None:
        raise ValueError("Entry request cannot be None")

    method: HttpValidVerbs | None = entry_request.method
    if method is None:
        raise ValueError("Entry request method cannot be None")

    if method not in HTTP_VERBS:
        raise ValueError("Unknown Http method}")

    return method


def filter_history_entries(entries: List[BundleEntry]) -> List[BundleEntry]:
    """
    Filters BundleEntries in a history Bundle and returns the latest for each ResourceType
    """
    results = []
    ids = []
    for entry in entries:
        _, id = get_resource_type_and_id_from_entry(entry)
        if id is None:
            continue

        if id in ids:
            continue

        ids.append(id)
        results.append(entry)

    return results
