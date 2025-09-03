
from fhir.resources.R4B.reference import Reference
import logging

from app.models.adjacency.node import NodeReference

logger = logging.getLogger(__name__)

def build_node_reference(data: Reference, base_url: str) -> NodeReference:
    """
    Converts a FHIR Reference to a NodeReference, making it relative to the given base URL if necessary.
    """
    ref = data.reference
    if ref is None:
        raise ValueError("Invalid reference (None)")

    if ref.startswith(base_url):
        ref = ref[len(base_url):].lstrip("/")

    if ref.startswith("https://") or ref.startswith("http://"):
        raise ValueError("Invalid absolute URL found")

    parts = ref.split("/")
    if len(parts) != 2:
        logger.error("Failed to parse reference: %s", ref)
        raise ValueError("Invalid reference: %s" % ref)

    return NodeReference(resource_type=parts[0], id=parts[1])
