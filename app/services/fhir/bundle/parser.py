from typing import Any, Dict

from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from app.models.fhir.types import BundleRequestParams
from app.services.fhir.bundle.fillers import fill_bundle, fill_entry


def create_bundle_entry(
    data: Dict[str, Any], fill_required_fields: bool = False
) -> BundleEntry:
    """
    Creates a Fhir BundleEntry model. When 'fill_required_fields' is set to True
    all required fields are filled with placeholder data to bypass ValidationErrors
    """
    if fill_required_fields:
        filled_entry = fill_entry(data)
        return BundleEntry.model_validate(filled_entry)

    return BundleEntry.model_validate(data)


def create_bundle(data: Dict[str, Any], fill_required_fields: bool = False) -> Bundle:
    """
    Creates a Fhir Bundle model. When 'fill_required_fields' is set to True
    all required fields are filled with placeholder data to bypass ValidationErrors
    """
    if fill_required_fields:
        filled_data = fill_bundle(data)
        return Bundle.model_validate(filled_data)

    return Bundle.model_validate(data)


def create_request_bundle(data: list[BundleRequestParams]) -> Bundle:
    """
    Creates a Bundle with Requests in the entries. Used typically to
    create a 'batch' request for a known set of Resources.
    """
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
