from typing import Optional
import requests
import fhir.resources.bundle as bundle_module
from uuid import uuid4
import json

NUTS_LRZA = "https://knooppunt-test.nuts-services.nl/lrza/mcsd"


def fetch_endpoints_from_nuts(since: Optional[str] = None) -> list:
    """Fetch Endpoint resources from Nuts LRZA endpoint"""
    print(f"\n📥 Fetching Endpoint resources from Nuts...")
    
    url = f"{NUTS_LRZA}/Endpoint/"
    params = {"_count": 100}
    if since:
        params["_since"] = since

    endpoints = []
    page_count = 0

    try:
        while url:
            page_count += 1
            print(f"  - Fetching page {page_count}...")

            response = requests.get(url, params=params if page_count == 1 else None, timeout=30)
            response.raise_for_status()

            
            data = response.json()
            
            # Extract Endpoint resources from bundle entries
            for entry in data.get("entry", []):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == "Endpoint":
                    endpoints.append(resource)
            
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
        print(f"  ❌ Error fetching Endpoint resources: {e}")
        return []

    print(f"✅ Fetched {len(endpoints)} Endpoint resources from Nuts.")
    return endpoints

if __name__ == "__main__":
    endpoints = fetch_endpoints_from_nuts()

    # create the output JSON structure
    directory_dict: dict = {"directories": []}

    # don't add already existing endpoints, track with a set
    existing_endpoint_addresses = set()

    for i, ep in enumerate(endpoints):
        if ep.get("address") in existing_endpoint_addresses:
            continue

        managingOrganization = ep.get('managingOrganization', {})
        directory_dict["directories"].append({
            "id": managingOrganization.get("display", f"{i:03d}-Org"),
            "ura": f"{i:011d}",  # Placeholder URA
            "endpoint_address": ep.get("address")
        })
        existing_endpoint_addresses.add(ep.get("address"))
    
    # print the output JSON structure
    print(json.dumps(directory_dict, indent=2))




