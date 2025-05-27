from typing import Dict, List, Tuple
from typing import Any

import requests
from yarl import Query

from app.services.api.authenticators.authenticator import Authenticator
from app.services.api.fhir_api import FhirApi
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.identifier import Identifier
import logging

from app.services.fhir.fhir_service import FhirService

logger = logging.getLogger(__name__)

class AuthenticSourceService:
    def __init__(self, authentic_sources: Dict[str, Any], authenticator: Authenticator) -> None:
        self.authentic_source = authentic_sources
        self.authenticator = authenticator

    def get_authentic_organizations(self, ura: str) -> List[Tuple[str, Organization]]:
        organizations: List[Tuple[str, Organization]] = []
        for source_name in self.authentic_source:
            try:
                organization = self.retrieve_authentic_organization(ura, source_name)
                organizations.append((source_name, organization))
            except ValueError as e:
                logger.error(f"Error retrieving organization for ura {ura} from source {source_name}: {e}")
                raise e

        return organizations

    def retrieve_authentic_organization(self, ura: str, source_name: str) -> Organization:
        #################################### Replace this part with search_resource in FhirApi
        source = self.authentic_source.get(source_name)
        
        ura_system = self.authentic_source.get("CIBG").get("identifier_system")
        url = f"{source.get('url')}/Organization/_search"
        params = [
            ("identifier", f"{ura_system}|{ura}"),
            ("identifier", f"{source.get('identifier_system')}|")
        ]
        response = requests.get(url, params=params)
        entries = FhirService(False).create_bundle(response.json()).entry
        ##################################### Replace this part with search_resource in FhirApi
        if not entries:
            raise ValueError(f"No organization found for ura {ura} in source {source_name}")
        if not isinstance(entries[0].resource, Organization):
            raise ValueError(f"Resource found for ura {ura} in source {source_name} is not an Organization")
        if len(entries) > 1:
            for entry in entries:
                try:
                    self.get_identifier_from_organization(entry.resource, source.get("identifier_system"))
                    return entry.resource
                except ValueError as e:
                    logger.error(f"Error retrieving identifier for ura {ura} from entry {entry}: {e}")
            raise ValueError(f"Multiple organizations found for ura {ura} and system {source.get('identifier_system')} in source {source_name}")
        return entries[0].resource

    def get_identifier_from_organization(self, organization: Organization, identifier_system: str) -> Identifier:
        for identifier in organization.identifier:
            if identifier["system"] == identifier_system:
                return identifier
        print(f"Identifier system {identifier_system} not found in organization {organization.model_dump()}")
        raise ValueError(f"No identifier found in the organization for system {identifier_system}")

    def merge_authentic_sources_with_supplier_org(self, supplier_org: Organization) -> Organization:
        # Retrieve the authentic organization
        try:
            ura = self.get_identifier_from_organization(supplier_org, self.authentic_source.get("CIBG").get("identifier_system"))["value"]
            authentic_orgs = self.get_authentic_organizations(ura)
            for source_name, authentic_org in authentic_orgs:
                match source_name:
                    case "VEKTIS":
                        supplier_org = self.merge_vektis(supplier_org, authentic_org)
                    case "CIBG":
                        supplier_org = self.merge_cibg(supplier_org, authentic_org)
                    case "KVK":
                        supplier_org = self.merge_kvk(supplier_org, authentic_org)
                    case "BIG":
                        supplier_org = self.merge_big(supplier_org, authentic_org)
            return supplier_org
        except Exception as e:
            logger.error(e)
            raise e
            return supplier_org

    def merge_vektis(self, supplier_org: Organization, vektis_org: Organization) -> Organization:
        # Merge logic for Vektis
        supplier_org = supplier_org.model_copy()
        vektis_system = self.authentic_source.get("VEKTIS").get("identifier_system")
        # if system from vektis_org.identifier is already in supplier_org remove that identifier
        
        supplier_org = FhirService(strict_validation=False).create_resource(supplier_org.model_dump())
        supplier_org.identifier = [
            identifier for identifier in supplier_org.identifier
            if identifier["system"] != vektis_system
        ]
        # Add the Vektis identifier to the supplier organization
        supplier_org.identifier.append(
            self.get_identifier_from_organization(vektis_org, vektis_system)
        )
        # Merge other fields as needed
        if vektis_org.type:
            supplier_org.type = vektis_org.type
        return supplier_org
    
    def merge_cibg(self, supplier_org: Organization, cibg_org: Organization) -> Organization:
        # Merge logic for CIBG
        supplier_org = supplier_org.model_copy()
        cibg_system = self.authentic_source.get("CIBG").get("identifier_system")
        # if system from cibg_org.identifier is already in supplier_org remove that identifier
        supplier_org = FhirService(strict_validation=False).create_resource(supplier_org.model_dump())
        supplier_org.identifier = [
            identifier for identifier in supplier_org.identifier
            if identifier["system"] != cibg_system
        ]
        # Add the CIBG identifier to the supplier organization
        supplier_org.identifier.append(
            self.get_identifier_from_organization(cibg_org, cibg_system)
        )
        # Merge other fields as needed
        if cibg_org.name:
            supplier_org.name = cibg_org.name
        if cibg_org.endpoint[0]:
            endpoint = cibg_org.endpoint[0].copy()
            endpoint_ref = endpoint.get("reference")
            if endpoint_ref and not endpoint_ref.startswith("http"):
                # Make the reference absolute using the CIBG base URL
                base_url = self.authentic_source.get("CIBG").get("url").rstrip("/")
                no_slash = endpoint_ref.lstrip("/")  # Ensure no leading slash
                endpoint_ref = f"{base_url}/{no_slash}"
                endpoint["reference"] = endpoint_ref
            supplier_org.endpoint.append(endpoint)
        return supplier_org

    def merge_kvk(self, supplier_org: Organization, kvk_org: Organization) -> Organization:
        # Merge logic for KVK
        supplier_org = supplier_org.model_copy()

        kvk_system = self.authentic_source.get("KVK").get("identifier_system")
        # if system from kvk_org.identifier is already in supplier_org remove that identifier
        supplier_org = FhirService(strict_validation=False).create_resource(supplier_org.model_dump())
        supplier_org.identifier = [
            identifier for identifier in supplier_org.identifier
            if identifier["system"] != kvk_system
        ]
        # Add the KVK identifier to the supplier organization
        supplier_org.identifier.append(
            self.get_identifier_from_organization(kvk_org, kvk_system)
        )
        return supplier_org

    def merge_big(self, supplier_org: Organization, big_org: Organization) -> Organization:
        supplier_org = supplier_org.model_copy()
        # Merge logic for BIG
        big_system = self.authentic_source.get("BIG").get("identifier_system")
        # if system from big_org.identifier is already in supplier_org remove that identifier
        supplier_org = FhirService(strict_validation=False).create_resource(supplier_org.model_dump())
        supplier_org.identifier = [
            identifier for identifier in supplier_org.identifier
            if identifier["system"] != big_system
        ]
        # Add the BIG identifier to the supplier organization
        supplier_org.identifier.append(
            self.get_identifier_from_organization(big_org, big_system)
        )
        return supplier_org