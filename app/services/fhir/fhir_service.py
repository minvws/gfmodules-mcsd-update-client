from typing import Any, Dict, List
from fhir.resources.R4B.bundle import BundleEntry, Bundle
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from app.models.adjacency.node import NodeReference
from app.models.fhir.types import HttpValidVerbs
from app.services.fhir.bundle.bundle_utils import (
    filter_history_entries,
    get_request_method_from_entry,
    get_resource_type_and_id_from_entry,
)
from app.services.fhir.bundle.bunlde_parser import (
    create_bundle,
    create_bundle_request,
    get_entries_from_bundle_of_bundles,
)
from app.services.fhir.model_factory import create_resource
from app.services.fhir.references.reference_extractor import (
    get_references,
)
from app.services.fhir.references.reference_misc import split_reference
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)


class FhirService:
    def __init__(
        self,
        strict_validation: bool,
    ) -> None:
        self.strict_validation = strict_validation

    def create_resource(self, data: Dict[str, Any]) -> DomainResource:
        """
        Creates a defined FHIR mCSD resource model.
        """
        return create_resource(data, self.strict_validation)

    def get_references(self, data: DomainResource) -> List[Reference]:
        """
        Retrieves a unique list of References from an FHIR mCSD Resources.
        data returned only if they contain the property "reference" in them,
        also contained references are ignored.
        """
        return get_references(data)

    def namespace_resource_references(
        self, data: DomainResource, namespace: str
    ) -> DomainResource:
        """
        Adds namespace to the References in a FHIR mCSD Resource.
        """
        return namespace_resource_reference(data, namespace)

    def split_reference(self, data: Reference) -> tuple[str, str]:
        """
        Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
        """
        return split_reference(data)

    def create_bundle(self, data: Dict[str, Any] | None = None) -> Bundle:
        """
        Returns a Bundle instance from a Dict object
        """
        return create_bundle(data, self.strict_validation)

    def get_resource_type_and_id_from_entry(
        self, entry: BundleEntry
    ) -> tuple[str, str]:
        """
        Retrieves the resource type and id from a Bundle entry
        """
        return get_resource_type_and_id_from_entry(entry)

    def get_request_method_from_entry(self, entry: BundleEntry) -> HttpValidVerbs:
        """
        Retrieves the request method from a Bundle entry if a request is present
        """
        return get_request_method_from_entry(entry)

    def filter_history_entries(self, entries: List[BundleEntry]) -> List[BundleEntry]:
        """
        Filters BundleEntries in a history Bundle and returns the latest for each ResourceType
        """
        return filter_history_entries(entries)

    def create_bundle_request(self, data: List[NodeReference]) -> Bundle:
        """
        Takes a list of AdjacencyReference and creates a Bundle with GET as an HTTP request method
        """
        return create_bundle_request(data)

    def get_entries_from_bundle_of_bundles(self, data: Bundle) -> List[BundleEntry]:
        """
        Returns a flat list of entries from a Bundle of Bundles
        """
        return get_entries_from_bundle_of_bundles(data)
