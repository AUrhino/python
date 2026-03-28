"""
LogicMonitor API - Get ConfigSource Config Data By Group
-------------------------------------------------------
This script retrieves devices from a LogicMonitor device group, finds
device datasources of type "CS" (ConfigSource), retrieves datasource
instances, then retrieves config details for each instance.

For each matching config record it returns:
- total
- version
- pollTimestamp (converted to local time)
- whether config data was found

Requirements:
- Python 3.x
- requests
- tabulate
- python-dotenv

Environment variables (.env):
    ACCESS_ID=your_access_id
    ACCESS_KEY=your_access_key
    COMPANY=your_company_name

Examples:
    # Lookup by group ID
    python3 Get-LMGroupConfigSources.py --group_id 12435

    # Lookup by full group path
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/Big W/ACT/191-Canberra Airport"

    # Filter instance names using comma-separated partial matches
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup"

    # Same filter with group name
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/ACME/Store 1" --instance_name_filter "running, startup"

    # Export results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --csv output/configs.csv

    # Export filtered results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup" --csv output/configs.csv

    # Enable debug output
    python3 Get-LMGroupConfigSources.py --group_id 12435 --debug

Instance name filter behavior:
    --instance_name_filter "running, startup"

This is a case-insensitive substring match:
    "running" -> matches "Running-Config"
    "startup" -> matches "Startup-Config"
"""

import argparse
import base64
import csv
import hashlib
import hmac
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
DEBUG = False
SESSION = requests.Session()


def validate_env() -> None:
    missing = [
        name for name, value in {
            "ACCESS_ID": ACCESS_ID,
            "ACCESS_KEY": ACCESS_KEY,
            "COMPANY": COMPANY,
        }.items() if not value
    ]

    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def debug_print(message: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {message}")


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> Dict[str, str]:
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb + epoch + data + resource_path
    hmac_hash = hmac.new(
        ACCESS_KEY.encode(),
        msg=request_vars.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()
    signature = base64.b64encode(hmac_hash.encode()).decode()
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"

    return {
        "Content-Type": "application/json",
        "Authorization": auth
    }


def api_get(resource_path: str) -> Dict:
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    debug_print(f"API endpoint: GET {resource_path}")
    debug_print(f"Full URL: {url}")

    try:
        response = SESSION.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return {}

    debug_print(f"Response status: {response.status_code}")

    if response.status_code == 200:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                debug_print(f"Response keys: {list(payload.keys())}")
                if "data" in payload:
                    data = payload.get("data")
                    debug_print(f"Response data type: {type(data).__name__}")
                    if isinstance(data, dict):
                        debug_print(f"Response data keys: {list(data.keys())}")
                        if "items" in data and isinstance(data.get("items"), list):
                            debug_print(f"Response data items count: {len(data.get('items', []))}")
                    elif isinstance(data, list):
                        debug_print(f"Response data list count: {len(data)}")
                if "items" in payload and isinstance(payload.get("items"), list):
                    debug_print(f"Top-level items count: {len(payload.get('items', []))}")
            return payload
        except ValueError:
            print("Error: Response was not valid JSON.")
            return {}

    print(f"Error: {response.status_code} - {response.text}")
    return {}


def extract_items_and_total(response: Dict) -> Tuple[List[Dict], Optional[int]]:
    """
    Support multiple LM response shapes:
    1) {"items":[...], "total":N}
    2) {"data":{"items":[...], "total":N}}
    3) {"data":[...]}
    4) {"data":{"data":{"items":[...], "total":N}}}
    """
    if not isinstance(response, dict):
        return [], None

    if isinstance(response.get("items"), list):
        return response.get("items", []), response.get("total")

    data = response.get("data")

    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data.get("items", []), data.get("total")

        nested = data.get("data")
        if isinstance(nested, dict) and isinstance(nested.get("items"), list):
            return nested.get("items", []), nested.get("total")

    if isinstance(data, list):
        return data, len(data)

    return [], None


def get_paginated_results(resource_base: str, page_size: int = 1000) -> List[Dict]:
    all_items: List[Dict] = []
    offset = 0

    while True:
        separator = "&" if "?" in resource_base else "?"
        resource_path = f"{resource_base}{separator}size={page_size}&offset={offset}"
        response = api_get(resource_path)
        items, total = extract_items_and_total(response)

        debug_print(f"Parsed items from {resource_path}: {len(items)}")
        debug_print(f"Parsed total from {resource_path}: {total}")

        if not items:
            break

        all_items.extend(items)

        if total is not None and len(all_items) >= total:
            break

        if len(items) < page_size:
            break

        offset += page_size

    return all_items


def get_unpaged_results(resource_path: str) -> List[Dict]:
    response = api_get(resource_path)
    items, total = extract_items_and_total(response)
    debug_print(f"Parsed items from {resource_path}: {len(items)}")
    debug_print(f"Parsed total from {resource_path}: {total}")
    return items


def get_results_prefer_unpaged(resource_base: str) -> List[Dict]:
    items = get_unpaged_results(resource_base)
    if items:
        return items

    debug_print(f"Unpaged call returned no items for {resource_base}. Trying paginated fallback.")
    return get_paginated_results(resource_base)


def get_group_by_id(group_id: int) -> Dict:
    response = api_get(f"/device/groups/{group_id}")

    if isinstance(response, dict) and response.get("id") is not None:
        return response

    data = response.get("data", {})
    if isinstance(data, dict) and data.get("id") is not None:
        return data

    return {}


def normalize_group_value(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).strip().strip("/")


def escape_filter_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_group_filter_path(field: str, value: str) -> str:
    filter_value = escape_filter_value(value)
    filter_expr = f'{field}:"{filter_value}"'
    encoded_filter = quote(filter_expr, safe="")
    return f"/device/groups?filter={encoded_filter}"


def get_groups_by_filter(field: str, value: str) -> List[Dict]:
    resource_base = build_group_filter_path(field, value)
    return get_results_prefer_unpaged(resource_base)


def dedupe_groups(groups: List[Dict]) -> List[Dict]:
    seen = set()
    deduped = []

    for group in groups:
        if not isinstance(group, dict):
            continue
        group_id = group.get("id")
        if group_id in seen:
            continue
        seen.add(group_id)
        deduped.append(group)

    return deduped


def show_group_candidates(groups: List[Dict]) -> None:
    table = []
    for group in groups:
        table.append([
            group.get("id"),
            group.get("name"),
            group.get("fullPath"),
        ])

    print(tabulate(table, headers=["Group ID", "Name", "Full Path"], tablefmt="grid"))


def find_group_by_name(group_name: str) -> Dict:
    wanted_fullpath = normalize_group_value(group_name)
    wanted_leaf = wanted_fullpath.split("/")[-1]

    fullpath_matches = dedupe_groups(get_groups_by_filter("fullPath", wanted_fullpath))
    if len(fullpath_matches) == 1:
        return fullpath_matches[0]
    if len(fullpath_matches) > 1:
        print(f"Multiple groups matched fullPath '{group_name}'.")
        show_group_candidates(fullpath_matches)
        return {}

    name_matches = dedupe_groups(get_groups_by_filter("name", wanted_fullpath))
    if len(name_matches) == 1:
        return name_matches[0]

    if wanted_leaf != wanted_fullpath:
        leaf_matches = dedupe_groups(get_groups_by_filter("name", wanted_leaf))
    else:
        leaf_matches = name_matches

    if len(leaf_matches) == 1:
        return leaf_matches[0]

    if len(leaf_matches) > 1:
        exact_fullpath_match = []
        for group in leaf_matches:
            group_id = group.get("id")
            if group_id is None:
                continue
            detail = get_group_by_id(group_id)
            full_path = normalize_group_value(detail.get("fullPath") or group.get("fullPath"))
            if full_path == wanted_fullpath:
                exact_fullpath_match.append(detail if detail else group)

        exact_fullpath_match = dedupe_groups(exact_fullpath_match)
        if len(exact_fullpath_match) == 1:
            return exact_fullpath_match[0]

        print(f"Multiple groups matched '{group_name}'. Possible matches:")
        show_group_candidates(leaf_matches)
        return {}

    print(f"No group found with name/fullPath '{group_name}'.")
    return {}


def get_devices_in_group(group_id: int) -> List[Dict]:
    return get_results_prefer_unpaged(f"/device/groups/{group_id}/devices")


def get_device_datasources(device_id: int) -> List[Dict]:
    return get_results_prefer_unpaged(f"/device/devices/{device_id}/devicedatasources")


def get_datasource_instances(device_id: int, device_datasource_id: int) -> List[Dict]:
    return get_results_prefer_unpaged(
        f"/device/devices/{device_id}/devicedatasources/{device_datasource_id}/instances"
    )


def get_instance_config_items(device_id: int, device_datasource_id: int, instance_id: int) -> Tuple[List[Dict], Optional[int]]:
    base = f"/device/devices/{device_id}/devicedatasources/{device_datasource_id}/instances/{instance_id}/config"

    response = api_get(base)
    items, total = extract_items_and_total(response)
    debug_print(f"Parsed config items from {base}: {len(items)}")
    debug_print(f"Parsed config total from {base}: {total}")

    if items:
        return items, total

    debug_print("Unpaged config call returned no items. Trying paginated fallback.")
    response = api_get(f"{base}?size=1000&offset=0")
    items, total = extract_items_and_total(response)
    debug_print(f"Parsed config items from paginated config call: {len(items)}")
    debug_print(f"Parsed config total from paginated config call: {total}")
    return items, total


def parse_instance_name_filters(filter_text: Optional[str]) -> List[str]:
    """
    Parse a comma-separated filter string into lowercase substrings.

    Example:
        "running, startup" -> ["running", "startup"]
    """
    if not filter_text:
        return []

    filters = [part.strip().lower() for part in filter_text.split(",") if part.strip()]
    return filters


def instance_name_matches(instance_name: str, filters: List[str]) -> bool:
    """
    Case-insensitive substring matching.

    Example:
        filters = ["running", "startup"]
        "Running-Config" -> True
        "Startup-Config" -> True
        "Candidate-Config" -> False
    """
    if not filters:
        return True

    candidate = (instance_name or "").lower()
    return any(filter_value in candidate for filter_value in filters)


def epoch_ms_to_local_string(epoch_ms: Optional[int]) -> str:
    if epoch_ms in (None, ""):
        return ""

    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000).astimezone()
        tz_name = dt.tzname() or ""
        return dt.strftime("%Y-%m-%d %H:%M:%S") + (f" {tz_name}" if tz_name else "")
    except Exception:
        return str(epoch_ms)


def format_config_found(config_value: Any) -> str:
    if isinstance(config_value, str):
        return "Config data found" if config_value.strip() else ""
    if config_value:
        return "Config data found"
    return ""


def display_table(data: List[List], headers: List[str], title: str = "") -> None:
    if title:
        print("\n" + "=" * 120)
        print(title)
        print("=" * 120)
    print(tabulate(data, headers=headers, tablefmt="grid"))


def write_csv(filepath: str, headers: List[str], rows: List[List]) -> None:
    output_dir = os.path.dirname(filepath)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV exported: {filepath}")


def collect_group_config_data(group_id: int, group_name: str, instance_name_filters: List[str]) -> List[List]:
    rows: List[List] = []

    devices = get_devices_in_group(group_id)
    if not devices:
        print("No devices found in the group.")
        return rows

    print(f"\nFound {len(devices)} device(s) in group '{group_name}' (ID: {group_id})")
    if instance_name_filters:
        print(f"Instance name filter(s): {', '.join(instance_name_filters)}")

    for device in devices:
        if not isinstance(device, dict):
            continue

        device_id = device.get("id")
        device_display_name = device.get("displayName") or device.get("name") or str(device_id)

        if device_id is None:
            continue

        debug_print(f"Processing device {device_display_name} ({device_id})")

        datasources = get_device_datasources(device_id)
        cs_datasources = [
            ds for ds in datasources
            if isinstance(ds, dict) and ds.get("dataSourceType") == "CS"
        ]

        debug_print(f"Found {len(cs_datasources)} ConfigSource datasource(s) on device {device_id}")

        for ds in cs_datasources:
            device_datasource_id = ds.get("id")
            datasource_name = ds.get("dataSourceName") or ds.get("dataSourceDisplayName") or ""

            if device_datasource_id is None:
                continue

            instances = get_datasource_instances(device_id, device_datasource_id)
            debug_print(
                f"Found {len(instances)} instance(s) for deviceDataSourceId {device_datasource_id}"
            )

            filtered_instances = []
            for instance in instances:
                if not isinstance(instance, dict):
                    continue
                instance_name = instance.get("displayName") or instance.get("name") or ""
                if instance_name_matches(instance_name, instance_name_filters):
                    filtered_instances.append(instance)

            debug_print(
                f"Matched {len(filtered_instances)} instance(s) after instance name filtering "
                f"for deviceDataSourceId {device_datasource_id}"
            )

            for instance in filtered_instances:
                instance_id = instance.get("id")
                instance_name = instance.get("displayName") or instance.get("name") or ""

                if instance_id is None:
                    continue

                items, total = get_instance_config_items(device_id, device_datasource_id, instance_id)

                if not items:
                    rows.append([
                        group_id,
                        group_name,
                        device_id,
                        device_display_name,
                        device_datasource_id,
                        datasource_name,
                        instance_id,
                        instance_name,
                        total if total is not None else 0,
                        "",
                        "",
                        ""
                    ])
                    continue

                for item in items:
                    version = item.get("version", "")
                    poll_timestamp = item.get("pollTimestamp")
                    config_found = format_config_found(item.get("config"))

                    rows.append([
                        group_id,
                        group_name,
                        device_id,
                        device_display_name,
                        device_datasource_id,
                        datasource_name,
                        instance_id,
                        item.get("instanceName") or instance_name,
                        total if total is not None else "",
                        version,
                        epoch_ms_to_local_string(poll_timestamp),
                        config_found
                    ])

    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retrieve ConfigSource config data for devices in a LogicMonitor group."
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--group_id",
        type=int,
        help="LogicMonitor device group ID. Example: --group_id 12435"
    )
    group.add_argument(
        "--group_name",
        type=str,
        help='LogicMonitor device group name or full path. Example: --group_name "Australia/Stores/Big W/ACT/191-Canberra Airport"'
    )

    parser.add_argument(
        "--instance_name_filter",
        type=str,
        help='Comma-separated instance name filters. Example: --instance_name_filter "running, startup"'
    )

    parser.add_argument(
        "--csv",
        type=str,
        help='Export results to CSV. Example: --csv output/configs.csv'
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output."
    )

    return parser.parse_args()


def main() -> None:
    global DEBUG
    args = parse_args()
    DEBUG = args.debug

    validate_env()

    if args.group_id is not None:
        group = get_group_by_id(args.group_id)
        if not group or not isinstance(group, dict) or group.get("id") is None:
            print(f"Unable to find group ID {args.group_id}.")
            sys.exit(1)
    else:
        group = find_group_by_name(args.group_name)
        if not group:
            sys.exit(1)

    group_id = group.get("id")
    group_name = group.get("fullPath") or group.get("name") or str(group_id)
    instance_name_filters = parse_instance_name_filters(args.instance_name_filter)

    rows = collect_group_config_data(group_id, group_name, instance_name_filters)

    if not rows:
        print("\nNo ConfigSource config data found.")
        return

    headers = [
        "Group ID",
        "Group Name",
        "Device ID",
        "Device Name",
        "DeviceDataSource ID",
        "DataSource Name",
        "Instance ID",
        "Instance Name",
        "Total",
        "Version",
        "Poll Timestamp (Local)",
        "Config Status"
    ]

    display_table(rows, headers, "LogicMonitor ConfigSource Config Data")

    if args.csv:
        write_csv(args.csv, headers, rows)


if __name__ == "__main__":
    main()
