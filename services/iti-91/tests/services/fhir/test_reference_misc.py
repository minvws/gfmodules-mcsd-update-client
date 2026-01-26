from typing import Any

import pytest
from fhir.resources.R4B.reference import Reference

from app.models.adjacency.node import NodeReference
from app.services.fhir.references.reference_misc import build_node_reference


def test_build_node_reference_with_absolute_url() -> None:
    ref = Reference.model_construct(reference="https://example.com/fhir/Patient/123")
    node_ref = build_node_reference(ref, "https://example.com/fhir")

    assert isinstance(node_ref, NodeReference)
    assert node_ref.resource_type == "Patient"
    assert node_ref.id == "123"


def test_build_node_reference_with_relative_url() -> None:
    ref = Reference.model_construct(reference="Patient/456")
    node_ref = build_node_reference(ref, "https://example.com/fhir")

    assert node_ref.resource_type == "Patient"
    assert node_ref.id == "456"


def test_build_node_reference_invalid_reference_none() -> None:
    ref = Reference.model_construct(reference=None)
    with pytest.raises(ValueError, match="Invalid reference"):
        build_node_reference(ref, "https://example.com/fhir")


def test_build_node_reference_invalid_format(monkeypatch: Any, caplog: Any) -> None:
    caplog.set_level("ERROR")
    ref = Reference.model_construct(reference="InvalidReferenceString")

    with pytest.raises(ValueError, match="Invalid reference:"):
        build_node_reference(ref, "https://example.com/fhir")

    # Check error log entry
    assert "Failed to parse reference" in caplog.text


def test_build_node_reference_with_cross_host_absolute_url() -> None:
    ref = Reference.model_construct(reference="https://other.example.org/fhir/Organization/abc")
    node_ref = build_node_reference(ref, "https://example.com/fhir")

    assert isinstance(node_ref, NodeReference)
    assert node_ref.resource_type == "Organization"
    assert node_ref.id == "abc"


def test_build_node_reference_with_history_absolute_url() -> None:
    ref = Reference.model_construct(
        reference="https://other.example.org/fhir/Practitioner/123/_history/1"
    )
    node_ref = build_node_reference(ref, "https://example.com/fhir")

    assert isinstance(node_ref, NodeReference)
    assert node_ref.resource_type == "Practitioner"
    assert node_ref.id == "123"
