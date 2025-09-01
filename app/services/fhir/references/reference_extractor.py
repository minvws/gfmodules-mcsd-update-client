import json
from typing import Iterable, List, TypeVar
from fhir.resources.R4B.backboneelement import BackboneElement
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from app.services.fhir.utils import validate_resource_type

_T = TypeVar("_T")


def filter_none(data: Iterable[_T | None]) -> Iterable[_T]:
    return [entry for entry in data if entry]


def flatten(sublists: Iterable[Iterable[_T]]) -> Iterable[_T]:
    """
    Helper function to flatten an Iterable of Iterables.
    eg: [[1,2],[3,4]] -> [1,2,3,4]
    """
    return [element for sublist in sublists for element in sublist]


def _from_backbone_element(data: BackboneElement) -> List[Reference]:
    """
    Helper function to extract references from Backbone Element.
    """
    fields = [getattr(data, name) for name in data.elements_sequence()]  # type: ignore
    fields = [field for field in fields if fields]

    refs = filter_none(
        [ref for ref in fields if isinstance(ref, Reference) if validate_reference(ref)]
    )
    return [*refs]


def from_domain_resource(model: DomainResource) -> List[Reference]:
    """
    Extracts references from an mCSD resource by going through the properties
    of the model.
    """
    # extract fields of the model and filter the None values
    fields = filter_none([getattr(model, field) for field in model.elements_sequence()])  # type: ignore

    # get the refs on the root level and filter None
    root_refs = filter_none(
        [ref for ref in fields if isinstance(ref, Reference) if validate_reference(ref)]
    )

    # extract sublists of the model and flatten it
    sub_lists = flatten([sublist for sublist in fields if isinstance(sublist, list)])

    # get the refs from the flattened sublists and filter nones
    sublist_refs = filter_none(
        [
            data
            for data in sub_lists
            if isinstance(data, Reference)
            if validate_reference(data)
        ]
    )

    # extract BackboneElements from the flattened sublists
    backbone_elements = [
        element for element in sub_lists if isinstance(element, BackboneElement)
    ]

    # extract sublists of refs from backbone elements and flatten them
    backbone_elements_refs = flatten(
        [_from_backbone_element(data) for data in backbone_elements]
    )

    return [
        *root_refs,
        *sublist_refs,
        *backbone_elements_refs,
    ]


def validate_reference(ref: Reference) -> bool:
    """
    Helper function that validates if a Reference is valid by checking if
    'reference' property exists and if is not contained (ie starting with
    '#').
    """
    if ref.reference is not None and not ref.reference.startswith("#"):
        return True

    return False


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
    valid_resource_type = validate_resource_type(resource_type)
    if not valid_resource_type:
        raise ValueError(f"{resource_type} is not a valid mCSD Resource")

    refs = from_domain_resource(model)
    return _make_unique(refs)
