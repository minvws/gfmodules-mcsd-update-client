from enum import Enum
from urllib.parse import urlsplit

from fhir.resources.R4B.reference import Reference


def split_reference(data: Reference) -> tuple[str, str]:
    """
    Returns a split resource from a reference (e.g. "Patient/123" -> ("Patient", "123"))
    """
    ref = data.reference
    if ref is None:
        raise ValueError("Cannot parse an None Value")

    parts = ref.split("/")
    if len(parts) < 2:
        raise ValueError(
            "Invalid Reference type, this should be a valid relative or absolute URL"
        )

    return parts[-2], parts[-1]


class ReferenceType(Enum):
    RELATIVE = "relative"
    ABSOLUTE = "absolute"
    LOCAL = "local"
    LOGICAL = "logical"
    CANONICAL = "canonical"
    UNKNOWN = "unknown"

def get_reference_type(data: Reference) -> ReferenceType:
    ref = data.reference
    identifier = data.identifier

    # logical reference (identifier only, no `reference`)
    if identifier is not None and not ref:
        return ReferenceType.LOGICAL

    if ref:
        if ref.startswith("#"):
            return ReferenceType.LOCAL
        if ref.startswith("http://") or ref.startswith("https://"):
            # canonical references are also absolute, but have a specific pattern
            if "|" in ref:  # versioned canonical
                return ReferenceType.CANONICAL
            return ReferenceType.ABSOLUTE
        if "/" in ref:
            return ReferenceType.RELATIVE

    return ReferenceType.UNKNOWN


def is_same_origin(url1: str, url2: str) -> bool:
    """
    Returns true if the URLs have the same origin.
    See https://developer.mozilla.org/en-US/docs/Web/Security/Same-origin_policy
    """

    def origin_tuple(url: str) -> tuple[str, str, int]|None:
        parts = urlsplit(url)
        if not (parts.scheme and parts.netloc):
            return None

        scheme = parts.scheme.lower()
        host = (parts.hostname or "").lower()
        port = parts.port
        if port is None:
            port = 80 if scheme == "http" else 443

        return (scheme, host, port)

    print("Checking origin for URLs:", url1, url2)
    return origin_tuple(url1) == origin_tuple(url2)
