import json
from typing import Any, List
from fhir.resources.R4B.backboneelement import BackboneElement
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from app.models.fhir.types import McsdResources


def _is_ref(data: Any) -> bool:
    """
    Helper function to validate if data is a valid Reference by checking against
    the class and Dict
    """
    if isinstance(data, Reference):
        return True

    if isinstance(data, dict) and "reference" in data.keys():
        return True

    return False


def _is_backbone_element(data: Any) -> bool:
    return isinstance(data, BackboneElement)


def _from_backbone_element(data: BackboneElement) -> List[Reference]:
    """
    Helpe function to extract references from Backbone Element.
    """
    fields = [getattr(data, name) for name in data.elements_sequence()]  # type: ignore
    fields = [field for field in fields if fields]

    refs = [extract_references(ref) for ref in fields if _is_ref(ref)]
    return [ref for ref in refs if ref]


def extract_model_references(model: DomainResource) -> List[Reference]:
    """
    Extracts references from an mCSD resource by going through the properties
    of the model.
    """
    # extract fields of the model and filter the None values
    fields = [getattr(model, field) for field in model.elements_sequence()]  # type: ignore
    fields = [field for field in fields if field]

    # get the refs on the root level and filter None
    root_refs = [extract_references(ref) for ref in fields if _is_ref(ref)]
    filtered_root_refs = [ref for ref in root_refs if ref]

    # extract sublists of the model and flatten it
    sub_lists = [sublist for sublist in fields if isinstance(sublist, list)]
    flattened_list = [element for sublist in sub_lists for element in sublist]

    # get the refs from the flattened sublists and filter nones
    sublist_refs = [
        extract_references(data) for data in flattened_list if _is_ref(data)
    ]
    filtered_sublist_refs = [ref for ref in sublist_refs if ref]

    # extract BackboneElements from the flattened sublists
    backbone_elements = [
        element for element in flattened_list if _is_backbone_element(element)
    ]
    nested_backbone_elements_refs = [
        _from_backbone_element(data) for data in backbone_elements
    ]
    # get the refs of those sublists
    flattened_backbone_elements_refs = [
        ref for sublist in nested_backbone_elements_refs for ref in sublist
    ]

    return [
        *filtered_root_refs,
        *filtered_sublist_refs,
        *flattened_backbone_elements_refs,
    ]


def extract_references(data: Any) -> Reference | None:
    """
    Helper function that extracts refs from a data by checking if the item is a Dict
    or an instance of the Reference. This function also ignores contained references.
    """
    if isinstance(data, dict):
        if "reference" in data and data["reference"][0] != "#":
            return Reference(**data)

    if isinstance(data, Reference):
        if data.reference is not None and not data.reference.startswith("#"):
            return data

    return None


def _make_unique(data: List[Reference]) -> List[Reference]:
    """
    Takes a list of References and deduplicates them based on their JSON representation.
    """
    unique_str = [*{*[json.dumps(d.model_dump()) for d in data]}]
    unique_dicts = [json.loads(i) for i in unique_str]

    return [Reference(**d) for d in unique_dicts]


def get_references(model: DomainResource) -> List[Reference]:
    """
    Takes a FHIR DomainResource as an argument and returns a list of References.
    """
    resource_type = model.get_resource_type()
    if not any(resource_type in r.value for r in McsdResources):
        raise ValueError(f"{resource_type} is not a valid mCSD Resource")

    refs = extract_model_references(model)
    return _make_unique(refs)
