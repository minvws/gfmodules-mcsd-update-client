from typing import Any, Dict, List
import pytest

from app.models.adjacency.node import Node, NodeReference
from app.services.fhir.bundle.bunlde_parser import create_bundle_entry
from app.services.fhir.fhir_service import FhirService
from app.services.update.computation_service import ComputationService
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


@pytest.fixture()
def mock_node_org(
    mock_org_bundle_entry: Dict[str, Any],
    org_node_references: List[NodeReference],
    fhir_service: FhirService,
    computation_service: ComputationService,
) -> Node:
    entry = create_bundle_entry(mock_org_bundle_entry)
    return create_mock_node(
        bundle_entry=entry,
        node_refs=org_node_references,
        fhir_service=fhir_service,
        supplier_hash=computation_service.hash_supplier_entry(entry),
    )


@pytest.fixture()
def mock_node_ep(
    mock_ep_bundle_entry: Dict[str, Any],
    ep_node_references: List[NodeReference],
    fhir_service: FhirService,
    computation_service: ComputationService,
) -> Node:
    entry = create_bundle_entry(mock_ep_bundle_entry)
    return create_mock_node(
        bundle_entry=entry,
        node_refs=ep_node_references,
        fhir_service=fhir_service,
        supplier_hash=computation_service.hash_supplier_entry(entry),
    )
