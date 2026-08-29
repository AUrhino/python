"""
LogicMonitor API - Update Device Properties from CSV
----------------------------------------------------
Reads a CSV file and updates/adds LogicMonitor custom properties on devices.

Expected CSV format:
deviceId,properties
123,"key:value,key2:value2"
456,"location:Sydney,owner:Ryan,environment:prod"

Optional identifiers supported:
- deviceId          preferred
- deviceName        resolved from LM device name
- displayName       resolved from LM displayName
- deviceDisplayName resolved from LM displayName

Requirements:
- Python 3.x
- requests, python-dotenv, tabulate
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage:
python update_device_properties_from_csv.py --csv devices.csv
python update_device_properties_from_csv.py --csv devices.csv --dry-run

Author: Ryan Gillan / LogicMonitor Support helper
"""

import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from tabulate import tabulate


# Load environment variables
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")

if not ACCESS_KEY or not ACCESS_ID or not COMPANY:
    raise SystemExit(
        "Missing one or more required environment variables: "
        "ACCESS_ID, ACCESS_KEY, COMPANY"
    )

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> dict:
    """
    Generate LogicMonitor LMv1 authentication headers.

    Important:
    - The data string must exactly match the body sent in the request.
    - The resource_path should not include the base URL.
    """
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb.upper() + epoch + data + resource_path

    hmac_hash = hmac.new(
        ACCESS_KEY.encode("utf-8"),
        msg=request_vars.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    signature = base64.b64encode(hmac_hash.encode("utf-8")).decode("utf-8")
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"

    return {
        "Content-Type": "application/json",
        "Authorization": auth,
    }


def api_request(
    method: str,
    resource_path: str,
    body: Optional[dict] = None,
    params: Optional[dict] = None,
) -> requests.Response:
    """
    Perform an authenticated LogicMonitor API request.
    """
    method = method.upper()
    data = json.dumps(body, separators=(",", ":")) if body is not None else ""

    headers = generate_auth_headers(method, resource_path, data)
    url = BASE_URL + resource_path

    return requests.request(
        method=method,
        url=url,
        headers=headers,
        data=data if data else None,
        params=params,
        timeout=60,
    )


def parse_properties(raw_properties: str) -> Dict[str, str]:
    """
    Parse key:value,key2:value2 into a dictionary.

    Notes:
    - Splits each property on the first colon only.
    - Empty keys are skipped.
    - Values may contain additional colons.
    - Commas are treated as property separators.
    """
    properties = {}

    if not raw_properties:
        return properties

    for pair in raw_properties.split(","):
        pair = pair.strip()
        if not pair:
            continue

        if ":" not in pair:
            print(f"Skipping invalid property pair without colon: {pair}")
            continue

        key, value = pair.split(":", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            print(f"Skipping property with empty key: {pair}")
            continue

        # Avoid attempting to overwrite read-only LM discovered properties.
        if key.startswith("system.") or key.startswith("auto."):
            print(f"Skipping read-only/discovered property: {key}")
            continue

        properties[key] = value

    return properties


def get_all_devices() -> List[dict]:
    """
    Retrieve all devices from LogicMonitor.

    Used only when CSV rows do not include deviceId and need name/displayName lookup.
    """
    devices = []
    offset = 0
    size = 1000

    while True:
        response = api_request(
            "GET",
            "/device/devices",
            params={
                "size": size,
                "offset": offset,
                "fields": "id,name,displayName",
            },
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to retrieve devices: {response.status_code} - {response.text}"
            )

        payload = response.json()
        data = payload.get("data", {})
        items = data.get("items", [])

        devices.extend(items)

        total = data.get("total", 0)
        if offset + size >= total or not items:
            break

        offset += size

    return devices


def build_device_lookup(devices: List[dict]) -> Dict[str, int]:
    """
    Build lookup map for device name and displayName to device ID.
    """
    lookup = {}

    for device in devices:
        device_id = device.get("id")
        name = device.get("name")
        display_name = device.get("displayName")

        if device_id:
            if name:
                lookup[f"name:{name}"] = device_id
            if display_name:
                lookup[f"displayName:{display_name}"] = device_id

    return lookup


def resolve_device_id(row: dict, device_lookup: Optional[Dict[str, int]]) -> Optional[int]:
    """
    Resolve a device ID from CSV row.
    """
    device_id = (
        row.get("deviceId")
        or row.get("id")
        or row.get("device_id")
        or row.get("Device ID")
    )

    if device_id:
        try:
            return int(str(device_id).strip())
        except ValueError:
            print(f"Invalid deviceId value: {device_id}")
            return None

    if not device_lookup:
        return None

    device_name = row.get("deviceName") or row.get("name")
    display_name = row.get("displayName") or row.get("deviceDisplayName")

    if device_name:
        return device_lookup.get(f"name:{device_name.strip()}")

    if display_name:
        return device_lookup.get(f"displayName:{display_name.strip()}")

    return None


def upsert_device_property(
    device_id: int,
    prop_name: str,
    prop_value: str,
    dry_run: bool = False,
) -> Tuple[str, str]:
    """
    Update an existing device property.
    If the property is not found, add it.

    Returns:
    - status string
    - response/error text
    """
    body = {
        "name": prop_name,
        "value": prop_value,
    }

    encoded_name = quote(prop_name, safe="")
    update_path = f"/device/devices/{device_id}/properties/{encoded_name}"

    if dry_run:
        return "DRY-RUN", f"Would set {prop_name}={prop_value}"

    update_response = api_request("PUT", update_path, body=body)

    if update_response.status_code in (200, 201):
        return "UPDATED", update_response.text

    # If property does not exist, add it.
    if update_response.status_code == 404:
        add_path = f"/device/devices/{device_id}/properties"
        add_response = api_request("POST", add_path, body=body)

        if add_response.status_code in (200, 201):
            return "ADDED", add_response.text

        return "ADD_FAILED", f"{add_response.status_code} - {add_response.text}"

    return "UPDATE_FAILED", f"{update_response.status_code} - {update_response.text}"


def process_csv(csv_path: str, dry_run: bool = False) -> List[List[str]]:
    """
    Process CSV rows and update device properties.
    """
    results = []

    with open(csv_path, mode="r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)

        if not reader.fieldnames:
            raise ValueError("CSV file has no headers.")

        has_device_id = any(
            header in reader.fieldnames
            for header in ["deviceId", "id", "device_id", "Device ID"]
        )

        device_lookup = None
        if not has_device_id:
            print("No deviceId column found. Building lookup from device name/displayName...")
            devices = get_all_devices()
            device_lookup = build_device_lookup(devices)

        for row_number, row in enumerate(reader, start=2):
            device_id = resolve_device_id(row, device_lookup)

            raw_properties = (
                row.get("properties")
                or row.get("Properties")
                or row.get("customProperties")
                or row.get("custom_properties")
            )

            if not device_id:
                results.append([row_number, "N/A", "N/A", "N/A", "SKIPPED", "Could not resolve device"])
                continue

            if not raw_properties:
                results.append([row_number, device_id, "N/A", "N/A", "SKIPPED", "No properties column/value"])
                continue

            properties = parse_properties(raw_properties)

            if not properties:
                results.append([row_number, device_id, "N/A", "N/A", "SKIPPED", "No valid properties parsed"])
                continue

            for prop_name, prop_value in properties.items():
                status, message = upsert_device_property(
                    device_id=device_id,
                    prop_name=prop_name,
                    prop_value=prop_value,
                    dry_run=dry_run,
                )

                results.append([
                    row_number,
                    device_id,
                    prop_name,
                    prop_value,
                    status,
                    message[:250],
                ])

    return results


def display_results(results: List[List[str]]) -> None:
    """
    Display results in a formatted table and save raw output.
    """
    headers = ["CSV Row", "Device ID", "Property", "Value", "Status", "Message"]

    print("\n" + "=" * 80)
    print("LogicMonitor Device Property Update Results")
    print("=" * 80)
    print(tabulate(results, headers=headers, tablefmt="grid"))

    os.makedirs("output", exist_ok=True)
    output_path = "output/update_device_properties_results.json"

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(
            [
                {
                    "csvRow": row[0],
                    "deviceId": row[1],
                    "property": row[2],
                    "value": row[3],
                    "status": row[4],
                    "message": row[5],
                }
                for row in results
            ],
            file,
            indent=4,
        )

    print(f"\nSaved results to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update LogicMonitor device custom properties from a CSV file."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file. Expected columns: deviceId, properties",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse CSV and show intended changes without calling the API.",
    )

    args = parser.parse_args()

    update_results = process_csv(args.csv, dry_run=args.dry_run)
    display_results(update_results)
