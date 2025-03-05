
from app.models.fhir.r4.types import Entry


def get_resource_from_reference(reference: str) -> tuple[str | None, str | None]:
    """
    Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
    """
    parts = reference.split("/")
    if len(parts) < 2:
        return None, None

    return parts[-2], parts[-1]


def get_resource_type_and_id_from_entry(
    entry: Entry,
) -> tuple[str | None, str | None]:
    """
    Retrieves the resource type and id from a Bundle entry
    """
    if entry.resource is not None:
        return entry.resource.resource_type, entry.resource.id

    if entry.fullUrl is not None:
        return entry.fullUrl.split("/")[-2], entry.fullUrl.split("/")[-1]

    # On DELETE entries, there is no resource anymore. In that case we need to grab the resource type and id from the URL
    if entry.request is not None and entry.request.url is not None:
        return entry.request.url.split("/")[-2], entry.request.url.split("/")[-1]

    return None, None


def get_request_method_from_entry(entry: Entry) -> str | None:
    """
    Retrieves the request method from a Bundle entry if a request is present
    """
    entry_request = entry.request
    if entry_request is None:
        return None

    method = entry_request.method
    if method is None:
        return None

    return method
