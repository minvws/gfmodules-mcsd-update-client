from typing import List
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference

from app.services.fhir.references.reference_extractor import get_references
from app.services.fhir.references.reference_namespacer import (
    namespace_resource_reference,
)


class ReferenceServices:

    @staticmethod
    def get_references(data: DomainResource) -> List[Reference]:
        return get_references(data)

    @staticmethod
    def namespace_practitioner_references(
        data: DomainResource, namespace: str
    ) -> DomainResource:
        return namespace_resource_reference(data, namespace)

    @staticmethod
    def split_resource_type(data: Reference) -> tuple[str, str]:
        """
        Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
        """
        ref = data.reference
        if ref is None:
            raise ValueError("Cannot parse an None Value")

        parts = ref.split("/")
        if len(parts) < 2:
            raise ValueError(
                "Invalid Reference type, this should be a valid relative or absolute URL"
            )

        return parts[-2], parts[-1]
