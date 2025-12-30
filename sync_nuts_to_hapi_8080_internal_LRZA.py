#!/usr/bin/env python3
"""
Simple script to sync mCSD resources from Nuts LRZA endpoint to a local HAPI FHIR server.
Fetches resources from Nuts and POSTs them to the local HAPI server.
"""

import requests
import json
import sys
from typing import Optional
from fhir.resources.R4B.bundle import Bundle, BundleEntry, BundleEntryRequest
from uuid import uuid4

# Configuration
NUTS_ENDPOINT = "https://knooppunt-test.nuts-services.nl/lrza/mcsd"
# HAPI_ENDPOINT = "http://hapi-update-client:8080/fhir"
HAPI_ENDPOINT = "http://localhost:8080/fhir"
BATCH_SIZE = 100

# mCSD resource types to sync
MCSD_RESOURCE_TYPES = [
    "Organization",
    "Endpoint",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
    "HealthcareService",
    "Location",
]


def fetch_resources_from_nuts(resource_type: str, since: Optional[str] = None) -> list:
    """Fetch resources from Nuts LRZA endpoint"""
    print(f"\n📥 Fetching {resource_type} resources from Nuts...")
    
    url = f"{NUTS_ENDPOINT}/{resource_type}/_history"
    params = {"_count": 100}
    if since:
        params["_since"] = since
    
    resources = []
    page_count = 0
    
    try:
        while url:
            page_count += 1
            print(f"  - Fetching page {page_count}...")
            response = requests.get(url, params=params if page_count == 1 else None, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract resources from bundle entries
            for entry in data.get("entry", []):
                resource = entry.get("resource")
                if resource:
                    resources.append(resource)
            
            # Check for next page
            next_link = None
            for link in data.get("link", []):
                if link.get("relation") == "next":
                    next_link = link.get("url")
                    break
            
            if next_link:
                url = next_link
                params = None  # Don't use params for paginated URLs
            else:
                url = None
                
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error fetching {resource_type}: {e}")
        return []
    
    print(f"  ✅ Fetched {len(resources)} {resource_type} resources")
    return resources


def create_transaction_bundle(resources: list) -> dict:
    """Create a FHIR transaction bundle from resources"""
    entries = []
    
    for resource in resources:
        entry = {
            "fullUrl": f"urn:uuid:{uuid4()}",
            "resource": resource,
            "request": {
                "method": "PUT",
                "url": f"{resource.get('resourceType', 'Resource')}/{resource.get('id', 'unknown')}"
            }
        }
        entries.append(entry)
    
    bundle = {
        "resourceType": "Bundle",
        "type": "transaction",
        "id": str(uuid4()),
        "entry": entries
    }
    
    return bundle


def post_bundle_to_hapi(bundle: dict) -> bool:
    """POST bundle to HAPI directory"""
    try:
        print(f"  📤 Posting {len(bundle['entry'])} resources to HAPI...")
        response = requests.post(
            HAPI_ENDPOINT,
            json=bundle,
            headers={"Content-Type": "application/fhir+json"},
            timeout=60
        )
        
        if response.status_code >= 400:
            print(f"  ❌ Error posting to HAPI: HTTP {response.status_code}")
            print(f"     Response: {response.text[:200]}")
            return False
        
        print(f"  ✅ Successfully posted {len(bundle['entry'])} resources to HAPI")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error posting to HAPI: {e}")
        return False


def post_resources_to_hapi(resources: list, resource_type: str) -> int:
    """POST resources individually to HAPI FHIR server"""
    synced_count = 0
    
    for resource in resources:
        try:
            resource_id = resource.get("id")
            url = f"{HAPI_ENDPOINT}/{resource_type}/{resource_id}"
            
            response = requests.put(
                url,
                json=resource,
                headers={"Content-Type": "application/fhir+json"},
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                synced_count += 1
            else:
                print(f"    ⚠️  Failed to POST {resource_type}/{resource_id}: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"    ⚠️  Error posting {resource_type}: {e}")
    
    return synced_count


def sync_all():
    """Sync all resource types from Nuts to HAPI"""
    print("🔄 Starting mCSD sync from Nuts LRZA to local HAPI...")
    print(f"   Source: {NUTS_ENDPOINT}")
    print(f"   Target: {HAPI_ENDPOINT}")
    
    # Use predefined mCSD resource types
    print(f"   Resources to sync: {', '.join(MCSD_RESOURCE_TYPES)}\n")
    
    total_synced = 0
    failed_types = []
    
    for resource_type in MCSD_RESOURCE_TYPES:
        try:
            # Fetch from Nuts
            resources = fetch_resources_from_nuts(resource_type)
            
            if not resources:
                print(f"  ⚠️  No {resource_type} resources found")
                continue
            
            # POST resources individually
            synced_count = post_resources_to_hapi(resources, resource_type)
            print(f"  ✅ Synced {synced_count}/{len(resources)} {resource_type} resources")
            total_synced += synced_count
                    
        except Exception as e:
            print(f"  ❌ Error processing {resource_type}: {e}")
            failed_types.append(resource_type)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"✅ Sync completed!")
    print(f"   Total resources synced: {total_synced}")
    if failed_types:
        print(f"   ⚠️  Failed resource types: {', '.join(failed_types)}")
    print(f"{'='*60}\n")
    
    return len(failed_types) == 0


if __name__ == "__main__":
    try:
        success = sync_all()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Sync cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        sys.exit(1)
