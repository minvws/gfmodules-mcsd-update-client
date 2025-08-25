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

