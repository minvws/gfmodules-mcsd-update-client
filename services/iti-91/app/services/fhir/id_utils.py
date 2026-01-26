import hashlib


def make_namespaced_fhir_id(namespace: str, resource_id: str) -> str:
    """Create a deterministic, FHIR-valid id for update-client resources.

    FHIR id constraints (R4): 1..64 chars of [A-Za-z0-9.-].

    We keep the existing `namespace-resource_id` format when it fits, and fall back
    to a deterministic SHA-256 hex digest when it does not.
    """
    candidate = f"{namespace}-{resource_id}"
    if len(candidate) <= 64:
        return candidate

    payload = f"{namespace}|{resource_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
