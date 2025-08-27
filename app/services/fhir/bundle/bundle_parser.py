from typing import Any, Dict, List
from fhir.resources.R4B.bundle import (
    Bundle,
    BundleEntry,
    BundleEntryRequest,
    BundleEntryResponse,
    BundleLink,
    BundleEntrySearch,
)

from app.models.fhir.types import BundleRequestParams
from app.services.fhir.model_factory import create_resource


def create_bundle_link(data: Dict[str, Any]) -> BundleLink:
    bundle_link = BundleLink.model_construct(**data)
    return bundle_link


def create_entry_request(data: Dict[str, Any]) -> BundleEntryRequest:
    entry_request = BundleEntryRequest.model_construct(**data)
    return entry_request


def create_entry_response(data: Dict[str, Any]) -> BundleEntryResponse:
    entry_response = BundleEntryResponse.model_construct(**data)
    return entry_response


def create_entry_search(data: Dict[str, Any]) -> BundleEntrySearch:
    entry_search = BundleEntrySearch.model_construct(**data)
    return entry_search


def create_bundle_entry(data: Dict[str, Any]) -> BundleEntry:
    bundle_entry = BundleEntry.model_construct()
    if "fullUrl" in data:
        bundle_entry.fullUrl = data["fullUrl"]

    if "link" in data:
        bundle_entry.link = []
        bundle_entry.link = [create_bundle_link(link) for link in data["link"]]

    if "request" in data:
        bundle_entry.request = create_entry_request(data["request"])    # type: ignore[assignment]

    if "resource" in data:
        if (
            "resourceType" in data["resource"]
            and data["resource"]["resourceType"] == "Bundle"
        ):
            bundle_entry.resource = create_bundle(data["resource"])     # type: ignore[assignment]
        else:
            bundle_entry.resource = create_resource(data["resource"])       # type: ignore[assignment]

    if "response" in data:
        bundle_entry.response = create_entry_response(data["response"])     # type: ignore[assignment]

    if "search" in data:
        bundle_entry.search = create_entry_search(data["search"])       # type: ignore[assignment]

    if "id" in data:
        bundle_entry.id = data["id"]

    return bundle_entry


def create_bundle(data: Dict[str, Any], strict: bool = False) -> Bundle:
    if strict:
        return Bundle.model_validate(data)

    bundle = Bundle.model_construct(type="")
    if "type" in data:
        bundle.type = data["type"]

    if "link" in data:
        bundle.link = []
        for link in data["link"]:
            bundle_link = create_bundle_link(link)
            bundle.link.append(bundle_link)

    if "entry" in data:
        bundle.entry = []
        for entry in data["entry"]:
            bundle.entry.append(create_bundle_entry(entry))

    if "total" in data:
        bundle.total = int(data["total"])
    return bundle


def create_bundle_request(data: list[BundleRequestParams]) -> Bundle:
    request_bundle = Bundle.model_construct(type="batch")
    request_bundle.entry = []
    for ref in data:
        bunlde_request = BundleEntryRequest.model_construct(
            method="GET", url=f"/{ref.resource_type}/{ref.id}/_history"
        )
        bundle_entry = BundleEntry.model_construct()
        bundle_entry.request = bunlde_request   # type: ignore[assignment]
        request_bundle.entry.append(bundle_entry)

    return request_bundle


def get_entries_from_bundle_of_bundles(data: Bundle) -> List[BundleEntry]:
    entries = data.entry if data.entry else []
    results: List[BundleEntry] = []

    for entry in entries:
        if isinstance(entry, BundleEntry) and isinstance(entry.resource, Bundle):   # type: ignore[attr-defined]
            nested_bundle = entry.resource  # type: ignore[attr-defined]
            nested_entries = nested_bundle.entry if nested_bundle.entry else []
            results.extend(nested_entries)
    return results
