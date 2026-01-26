from typing import Any
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference

from app.services.fhir.bundle.utils import get_resource_from_reference
from app.services.fhir.id_utils import make_namespaced_fhir_id
from app.services.fhir.references.reference_extractor import (
    validate_reference,
)


def _namespace_reference(ref: Reference, namespace: str) -> Reference | None:
    """
    Finds a single reference in the data and converts it to namespaced references.
    """
    valid_reference = validate_reference(ref)
    if not valid_reference or ref.reference is None:
        return None

    res_type, _id = get_resource_from_reference(ref.reference)

    ref.reference = f"{res_type}/{make_namespaced_fhir_id(namespace, _id)}"
    return ref


def _namespace_in_value(value: Any, namespace: str) -> Any:
    """Recursively namespacing References"""
    # Handle Reference object
    if isinstance(value, Reference):
        return _namespace_reference(value, namespace) or value

    # Handle FHIR model objects
    if hasattr(value, "elements_sequence"):
        for attr in value.elements_sequence():
            attr_val = getattr(value, attr, None)
            if attr_val is not None:
                setattr(value, attr, _namespace_in_value(attr_val, namespace))
        return value

    # Handle lists
    if isinstance(value, list):
        return [_namespace_in_value(v, namespace) for v in value]

    return value


def namespace_resource_reference(
    data: DomainResource, namespace: str
) -> DomainResource:
    res = _namespace_in_value(data, namespace)
    if isinstance(res, DomainResource):
        return res
    raise ValueError("Expected DomainResource after namespacing")
