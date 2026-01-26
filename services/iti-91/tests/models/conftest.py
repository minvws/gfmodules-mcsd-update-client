from typing import Any, Dict, List
import pytest

from app.models.adjacency.node import Node, NodeReference
from app.services.fhir.bundle.parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from tests.utils.utils import create_mock_node


@pytest.fixture()
def mock_loc_node(
    mock_loc_bundle_entry: Dict[str, Any],
    location_node_references: List[NodeReference],
    fhir_service: FhirService,
) -> Node:
    entry = create_bundle_entry(mock_loc_bundle_entry)
    return create_mock_node(
        bundle_entry=entry,
        node_refs=location_node_references,
        fhir_service=fhir_service,
    )
