from typing import List, Set
from fhir.resources.R4B.bundle import BundleEntry


def get_resource_type_and_id_from_entry(
    entry: BundleEntry,
) -> tuple[str, str]:
    """
    Retrieves the resource type and id from a Bundle entry
    """
    if entry.resource is not None:
        return entry.resource.__resource_type__, entry.resource.id

    if entry.fullUrl is not None:
        return entry.fullUrl.split("/")[-2], entry.fullUrl.split("/")[-1]

    # On DELETE entries, there is no resource anymore. In that case we need to grab the resource type and id from the URL
    if entry.request is not None and entry.request.url is not None:
        return entry.request.url.split("/")[-2], entry.request.url.split("/")[-1]

    raise ValueError("resourceType and id are not available in Bundle")


def get_request_method_from_entry(entry: BundleEntry) -> str:
    """
    Retrieves the request method from a Bundle entry if a request is present
    """
    entry_request = entry.request
    if entry_request is None:
        raise ValueError("Entry request cannot be None")

    method: str = entry_request.method
    if method is None:
        raise ValueError("Entry request method cannot be None")

    return method


# TODO: Add this to fhir service
def filter_history_entries(entries: List[BundleEntry]) -> List[BundleEntry]:
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


# TODO: this go in fhir service
def get_entries_id_and_type(entries: List[BundleEntry]) -> List[tuple[str, str]]:
    ids: Set[tuple[str, str]] = set()
    for e in entries:
        res_type, id = get_resource_type_and_id_from_entry(e)
        if (id, res_type) not in ids:
            ids.add((id, res_type))
    return list(ids)
