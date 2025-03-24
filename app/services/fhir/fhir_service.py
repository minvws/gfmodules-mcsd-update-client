from typing import Any, Dict, List
from fhir.resources.R4B.bundle import BundleEntry
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from app.services.fhir.bundle.bundle_utils import (
    get_request_method_from_entry,
    get_resource_type_and_id_from_entry,
)
from app.services.fhir.model_factory import create_resource
from app.services.fhir.references.reference_extractor import (
    extract_references,
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

    def extract_references(self, data: Any) -> Reference | None:
        """
        Builds a Reference object only if the "reference" exists in the object
        """
        return extract_references(data)

    def get_references(self, data: DomainResource) -> List[Reference]:
        """
        Retrieves a unique list of References from an FHIR mCSD Resources.
        References are returned only if they contain the property "reference" in them.
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

    def get_resource_type_and_id_from_entry(
        self, entry: BundleEntry
    ) -> tuple[str, str]:
        """
        Retrieves the resource type and id from a Bundle entry
        """
        return get_resource_type_and_id_from_entry(entry)

    def get_request_method_from_entry(self, entry: BundleEntry) -> str:
        """
        Retrieves the request method from a Bundle entry if a request is present
        """
        return get_request_method_from_entry(entry)
