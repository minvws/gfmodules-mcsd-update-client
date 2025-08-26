from typing import Any, List
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.practitioner import Practitioner, PractitionerQualification

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

def _namespace_practitioner_references(
    data: Practitioner, namespace: str
) -> Practitioner:
    """
    FIXME: This setup could be improved by filtering through BackboneElements
    """
    qualification = data.qualification
    if qualification is not None:
        for i, q in enumerate(qualification):
            if isinstance(q, PractitionerQualification):
                issuer = q.issuer   # type: ignore[attr-defined]
                if issuer is not None:
                    new_ref = _namespace_reference(issuer, namespace)
                    if new_ref is not None:
                        qualification[i].issuer = new_ref   # type: ignore[attr-defined]
            elif isinstance(q, dict) and q.get("issuer") is not None:
                issuer = q["issuer"]
                if issuer is not None:
                    new_ref = _namespace_reference(issuer, namespace)
                    if new_ref is not None:
                        qualification[i]["issuer"] = new_ref

        data.qualification = qualification
    return data

def _namespace_references(data: DomainResource, namespace: str) -> DomainResource:
    for attr in data.elements_sequence():  # type: ignore
        value = getattr(data, attr)
        if value is None:
            continue

        if isinstance(value, dict) and value.get("reference") is not None or isinstance(value, Reference):
            namespaced = _namespace_reference(value, namespace)
            if namespaced is not None:
                setattr(data, attr, namespaced)

        elif isinstance(value, list):
            setattr(data, attr, _namespace_list_references(value, namespace))

    return data

def _namespace_list_references(items: List[Any], namespace: str) -> List[Any]:
    for i, item in enumerate(items):
        if isinstance(item, dict) and item.get("reference") is not None or isinstance(item, Reference):
            namespaced = _namespace_reference(item, namespace)
            if namespaced is not None:
                items[i] = namespaced
    return items

def namespace_resource_reference(data: DomainResource, namespace: str) -> DomainResource:
    if isinstance(data, Practitioner):
        return _namespace_practitioner_references(data, namespace)
    return _namespace_references(data, namespace)
