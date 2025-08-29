import json
from typing import Any, Iterable, List, TypeVar
from fhir.resources.R4B.backboneelement import BackboneElement
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.reference import Reference
from app.models.fhir.types import McsdResources


_T = TypeVar("_T")


def filter_none(data: Iterable[_T | None]) -> Iterable[_T]:
    return [entry for entry in data if entry]


def flatten(sublists: Iterable[Iterable[_T]]) -> Iterable[_T]:
    """
    Helper function to flatten an Iterable of Iterables.
    eg: [[1,2],[3,4]] -> [1,2,3,4]
    """
    return [element for sublist in sublists for element in sublist]


def _is_ref(data: Any) -> bool:
    """
    Helper function to validate if data is a valid Reference by checking against
    the class and Dict.
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
    Helper function to extract references from Backbone Element.
    """
    fields = [getattr(data, name) for name in data.elements_sequence()]  # type: ignore
    fields = [field for field in fields if fields]

    refs = filter_none([extract_reference(ref) for ref in fields if _is_ref(ref)])
    return [*refs]


def from_domain_resource(model: DomainResource) -> List[Reference]:
    """
    Extracts references from an mCSD resource by going through the properties
    of the model.
    """
    # extract fields of the model and filter the None values
    fields = filter_none([getattr(model, field) for field in model.elements_sequence()])  # type: ignore

    # get the refs on the root level and filter None
    root_refs = filter_none([extract_reference(ref) for ref in fields if _is_ref(ref)])

    # extract sublists of the model and flatten it
    sub_lists = flatten([sublist for sublist in fields if isinstance(sublist, list)])

    # get the refs from the flattened sublists and filter nones
    sublist_refs = filter_none(
        [extract_reference(data) for data in sub_lists if _is_ref(data)]
    )

    # extract BackboneElements from the flattened sublists
    backbone_elements = [
        element for element in sub_lists if _is_backbone_element(element)
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


# TODO: deprecate this function once new namespace PR is merged
def extract_reference(data: Any) -> Reference | None:
    """
    Helper function that extracts refs from a data by checking if the item is a Dict
    or an instance of the Reference. This function also ignores contained references.

    Note: will be deprecated soon.
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

    refs = from_domain_resource(model)
    return _make_unique(refs)
