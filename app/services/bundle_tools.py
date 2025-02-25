import json
from typing import Dict, List, Any
from app.models.fhir.r4.types import Entry, Resource


def get_resource_from_reference(reference: str) -> tuple[str | None, str | None]:
    """
    Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
    """
    parts = reference.split("/")
    if len(parts) < 2:
        return None, None

    return parts[-2], parts[-1]


def get_resource_type_and_id_from_entry(entry: Entry) -> tuple[str | None, str | None]:
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


def get_references(
    data: dict[str, Any],
) -> List[Dict[str, str]]:
    refs: List[Dict[str, str]] = []
    """
    Takes a Dict FHIR Resource as an argument and returns a list of references.
    """
    for k, v in data.items():
        try:
            if "reference" in v:
                refs.append(v)
        except TypeError:
            continue

        if k == "qualification":
            refs.append(v)
            continue

        if isinstance(v, List):
            refs_list: List[dict[str, str]] = []
            checker = False
            for i in v:
                try:
                    if "reference" in i:
                        checker = True
                        refs_list.append(i)
                except TypeError:
                    continue

            if checker:
                refs.extend(refs_list)
    return refs


def get_unique_references(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Takes a Dict FHIR Resource as an argument and returns a list of unique references
    """
    refs = get_references(data)
    # transform to str and back to dict to get unique list of dicts
    unique_strs = list(set([json.dumps(i, sort_keys=True) for i in refs]))
    unique_refs = [json.loads(i) for i in unique_strs]

    return unique_refs


def namespace_resource_refs(data: Dict[str, Any], namespace: str) -> Resource:
    """
    Takes a Dict FHIR Resource and prefixes the references.
    eg {reference: "some_id} -> {"reference": "namespace/some_id"
    """
    for k, v in data.items():
        try:
            if "reference" in v:
                res_type, id = get_resource_from_reference(data[k]["reference"])
                new_id = f"{namespace}-{id}"
                data[k]["reference"] = f"{res_type}/{new_id}"
        except TypeError:
            continue

        if k == "qualification":  # Special practitioners case

            res_type, id = get_resource_from_reference(data[k]["reference"])
            new_id = f"{namespace}-{id}"
            data[k]["reference"] = f"{res_type}/{new_id}"
            continue

        if isinstance(v, List):
            for index, value in enumerate(v):
                try:
                    if "reference" in value:
                        res_type, id = get_resource_from_reference(
                            data[k][index]["reference"]
                        )
                        new_id = f"{namespace}-{id}"
                        data[k][index]["reference"] = f"{res_type}/{new_id}"
                except TypeError:
                    continue

    return Resource(**data)
