from dataclasses import dataclass
from typing import Literal
from enum import Enum
from pydantic import BaseModel

HttpValidVerbs = Literal["GET", "POST", "PATCH", "POST", "PUT", "HEAD", "DELETE"]

ERROR_SEVERITIES = {"error", "fatal"}

@dataclass
class BundleError:
    # Index in the bundle for this error
    entry: int
    # Status code of the error (http status code)
    status: int
    # Severity of the error
    severity: str
    # Error code
    code: str
    # Any diagnostics
    diagnostics: str|None


class BundleRequestParams(BaseModel):
    id: str
    resource_type: str


class McsdResources(Enum):
    ORGANIZATION_AFFILIATION = "OrganizationAffiliation"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTHCARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
    PRACTITIONER = "Practitioner"
    ORGANIZATION = "Organization"
    ENDPOINT = "Endpoint"


class McsdResourcesWithRequiredFields(Enum):
    ENDPOINT = "Endpoint"
    PRACTITIONER = "Practitioner"
    PRACTITIONER_ROLE = "PractitionerRole"
    HEALTHCARE_SERVICE = "HealthcareService"
    LOCATION = "Location"
