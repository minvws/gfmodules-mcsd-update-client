from typing import Literal
from enum import Enum
from pydantic import BaseModel

HttpValidVerbs = Literal["GET", "POST", "PATCH", "POST", "PUT", "HEAD", "DELETE"]


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
