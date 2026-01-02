
from fhir.resources.R4B.reference import Reference
import logging
from yarl import URL

from app.models.adjacency.node import NodeReference

logger = logging.getLogger(__name__)

def build_node_reference(data: Reference, base_url: str) -> NodeReference:
    """
    Converts a FHIR Reference to a NodeReference, making it relative to the given base URL if necessary.
    """
    ref = data.reference
    if ref is None:
        raise ValueError("Invalid reference (None)")

    base_url_norm = base_url.rstrip("/")
    if ref.startswith(base_url_norm):
        ref = ref[len(base_url_norm):].lstrip("/")

    if ref.startswith("https://") or ref.startswith("http://"): # NOSONAR
        try:
            url = URL(ref)
            parts = [p for p in url.path.split("/") if p]
            if "_history" in parts:
                i = parts.index("_history")
                if i >= 2:
                    return NodeReference(resource_type=parts[i - 2], id=parts[i - 1])
                raise ValueError("Invalid absolute URL found")
            if len(parts) >= 2:
                return NodeReference(resource_type=parts[-2], id=parts[-1])
            raise ValueError("Invalid absolute URL found")
        except Exception:
            raise ValueError("Invalid absolute URL found")

    parts = ref.split("/")
    if len(parts) != 2:
        logger.error("Failed to parse reference: %s", ref)
        raise ValueError("Invalid reference: %s" % ref)

    return NodeReference(resource_type=parts[0], id=parts[1])
