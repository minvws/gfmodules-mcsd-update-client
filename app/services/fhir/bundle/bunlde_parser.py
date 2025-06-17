from typing import Any, Dict, List
from fhir.resources.R4B.bundle import (
    Bundle,
    BundleEntry,
    BundleEntryRequest,
    BundleEntryResponse,
    BundleLink,
    BundleEntrySearch,
)
from pydantic import ValidationError

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
        bundle_entry.request = create_entry_request(data["request"])

    if "resource" in data:
        if (
            "resourceType" in data["resource"]
            and data["resource"]["resourceType"] == "Bundle"
        ):
            bundle_entry.resource = create_bundle(data["resource"])
        else:
            bundle_entry.resource = create_resource(data["resource"])

    if "response" in data:
        bundle_entry.response = create_entry_response(data["response"])

    if "search" in data:
        bundle_entry.search = create_entry_search(data["search"])

    if "id" in data:
        bundle_entry.id = data["id"]

    return bundle_entry


def create_bundle(data: Dict[str, Any] | None = None, strict: bool = False) -> Bundle:
    try:
        if strict:
            return Bundle(**data) if data is not None else Bundle.model_construct()

        bundle = Bundle.model_construct(type="")
        if data is not None:
            if "type" in data:
                bundle.type = data["type"]

            if "link" in data:
                bundle.link = []
                for link in data["link"]:
                    bundle_link = create_bundle_link(link)
                    bundle.link.append(bundle_link)

            if "entry" in data:
                bundle.entry = list()
                for entry in data["entry"]:
                    bundle.entry.append(create_bundle_entry(entry))

            if "total" in data:
                bundle.total = int(data["total"])
        return bundle
    except ValidationError as e:
        raise e


def create_bundle_request(data: list[BundleRequestParams]) -> Bundle:
    request_bundle = Bundle.model_construct(type="batch")
    request_bundle.entry = []
    for ref in data:
        bunlde_request = BundleEntryRequest.model_construct(
            method="GET", url=f"/{ref.resource_type}/{ref.id}/_history"
        )
        bundle_entry = BundleEntry.model_construct()
        bundle_entry.request = bunlde_request
        request_bundle.entry.append(bundle_entry)

    return request_bundle


def get_entries_from_bundle_of_bundles(data: Bundle) -> List[BundleEntry]:
    entries = data.entry if data.entry else []
    results: List[BundleEntry] = []

    for entry in entries:
        if isinstance(entry, BundleEntry) and isinstance(entry.resource, Bundle):
            nested_bundle = entry.resource
            nested_entries = nested_bundle.entry if nested_bundle.entry else []
            results.extend(nested_entries)
    return results
