from typing import Any
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference

from app.services.fhir.bundle.bundle_utils import get_resource_from_reference
from app.services.fhir.references.reference_extractor import extract_reference


def _namespace_reference(data: Any, namespace: str) -> Reference | None:
    """
    Finds a single reference in the data and converts it to namespaced references.
    """
    ref = extract_reference(data)
    if ref is None or ref.reference is None:
        return None
    res_type, _id = get_resource_from_reference(ref.reference)
    if res_type is None or _id is None:
        return None

    ref.reference = f"{res_type}/{namespace}-{_id}"
    return ref


def _namespace_in_value(value: Any, namespace: str) -> Any:
    """Recursively namespacing References"""
    # Handle dict
    if isinstance(value, dict):
        if "reference" in value:
            return _namespace_reference(value, namespace) or value
        return {k: _namespace_in_value(v, namespace) for k, v in value.items()}

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
