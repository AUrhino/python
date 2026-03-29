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

Optional features:
- Filter datasource names with comma-separated values, wildcards, and exclusions
- Filter instance names with comma-separated partial matches
- Include selected device properties from:
  - customProperties
  - systemProperties
  - autoProperties
  - inheritedProperties
- Show script version info

Requirements:
- Python 3.x
- requests
- tabulate
- python-dotenv

Environment variables (.env):
    ACCESS_ID=your_access_id
    ACCESS_KEY=your_access_key
    COMPANY=your_company_name

Workflow summary:
    1. Resolve the target group from --group_id or --group_name
    2. Retrieve devices in the group
    3. For each device:
         a. Retrieve device datasources
         b. Keep only datasources where dataSourceType == "CS"
         c. Apply optional datasource filters from --filterDS
         d. Retrieve datasource instances
         e. Apply optional instance filters from --instance_name_filter
         f. Retrieve config data for each matching instance
    4. Optionally append requested device properties
    5. Print the table and optionally export CSV

Important implementation note:
    Some LogicMonitor endpoints in certain portals return usable data only
    when called without pagination parameters. For that reason, this script
    generally prefers the unpaged endpoint first, then falls back to the
    paginated form when necessary.

Examples:
    # Show script version
    python3 Get-LMGroupConfigSources.py --version

    # Lookup by group ID
    python3 Get-LMGroupConfigSources.py --group_id 12435

    # Lookup by full group path
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/Store 1"

    # Filter datasource name exact match
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH_Exec_Standard"

    # Filter datasource name with wildcard
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*"

    # Exclude a datasource name
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "!Cisco_IOS"

    # Match a datasource name starting with a literal !
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "\\!Cisco_IOS"

    # Include wildcard and exclude one datasource
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*,!Cisco_IOS"

    # Filter instance names using comma-separated partial matches
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup"

    # Same filter with group name
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/Stores/Store 1" --instance_name_filter "running, startup"

    # Include device properties
    python3 Get-LMGroupConfigSources.py --group_id 12435 --include_properties "system.staticgroup,snmp.community"

    # Filter datasource, instance names and include device properties
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*" --instance_name_filter "running, startup" --include_properties "system.staticgroup,snmp.community"

    # Export results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --csv output/configs.csv

    # Enable debug output
    python3 Get-LMGroupConfigSources.py --group_id 12435 --debug

Datasource filter behavior:
    --filterDS "SSH_Exec_Standard"
    --filterDS "SSH*"
    --filterDS "!Cisco_IOS"
    --filterDS "\\!Cisco_IOS"
    --filterDS "SSH*,!Cisco_IOS"

Rules:
    - leading !  => exclude pattern
    - leading \! => literal ! in the pattern
    - * is supported as a wildcard

Instance name filter behavior:
    --instance_name_filter "running, startup"

This is a case-insensitive substring match:
    "running" -> matches "Running-Config"
    "startup" -> matches "Startup-Config"

Included properties behavior:
    --include_properties "system.staticgroup,snmp.community"

For each requested property, the script checks these arrays on the device:
    - customProperties
    - systemProperties
    - autoProperties
    - inheritedProperties

If a property is not found, the output cell is left blank.
"""

import argparse
import base64
import csv
import fnmatch
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

# Load environment variables from .env if present.
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
DEBUG = False
SESSION = requests.Session()

SCRIPT_VERSION = "1.0.2"
SCRIPT_DATE    = "2026-03-29"
SCRIPT_NAME    = "Get-LMGroupConfigSources.py"
SCRIPT_AUTHOR  = "Community project written by Ryan Gillan"
SCRIPT_URL     = "https://github.com/AUrhino/python/tree/main/LMConfigs"


def validate_env() -> None:
    """
    Validate required LogicMonitor credentials from environment variables.

    Required variables:
        ACCESS_ID
        ACCESS_KEY
        COMPANY

    Exits:
        sys.exit(1) if any required value is missing.
    """
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
    """
    Print a message only when --debug is enabled.

    Args:
        message: Debug text to print.
    """
    if DEBUG:
        print(f"[DEBUG] {message}")


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> Dict[str, str]:
    """
    Build LMv1 authentication headers for a LogicMonitor REST request.

    LogicMonitor LMv1 signing format:
        signature input = HTTP_VERB + EPOCH_MS + BODY + RESOURCE_PATH

    Args:
        http_verb: Request method such as GET.
        resource_path: REST path only, for example /device/groups/123.
        data: Request body as a string. Empty for GET requests.

    Returns:
        Headers dictionary containing Content-Type and Authorization.
    """
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
    """
    Perform a GET request against the LogicMonitor REST API.

    This helper handles:
    - LMv1 auth header creation
    - JSON decoding
    - optional debug logging for response shape

    Args:
        resource_path: REST path only, not the full portal URL.

    Returns:
        Parsed JSON dictionary on success.
        Empty dict on failure.
    """
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
    Extract list items and total count from several LogicMonitor response shapes.

    Why this exists:
        LM endpoints are not always consistent across portals or endpoints.
        Some responses are flat, some are wrapped under data, and some return
        a list directly under data.

    Supported shapes:
        1) {"items":[...], "total":N}
        2) {"data":{"items":[...], "total":N}}
        3) {"data":[...]}
        4) {"data":{"data":{"items":[...], "total":N}}}

    Args:
        response: Parsed JSON response.

    Returns:
        Tuple of:
            - list of item dictionaries
            - total count if available, else None
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
    """
    Retrieve items from a paginated LogicMonitor endpoint.

    This helper appends:
        ?size=<page_size>&offset=<offset>

    and continues until:
    - the endpoint returns no items
    - the returned total has been reached
    - the last page is smaller than page_size

    Args:
        resource_base: REST path without size/offset.
        page_size: Number of records per page.

    Returns:
        Combined list of all retrieved items.
    """
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
    """
    Retrieve items from an unpaged LogicMonitor list endpoint.

    This helper is preferred first because some LM portals return valid data
    from the plain endpoint but return no usable items when size/offset is
    appended.

    Args:
        resource_path: REST path without pagination parameters.

    Returns:
        List of item dictionaries.
    """
    response = api_get(resource_path)
    items, total = extract_items_and_total(response)
    debug_print(f"Parsed items from {resource_path}: {len(items)}")
    debug_print(f"Parsed total from {resource_path}: {total}")
    return items


def get_results_prefer_unpaged(resource_base: str) -> List[Dict]:
    """
    Retrieve list results from a LogicMonitor endpoint.

    Strategy:
        1. Try the plain endpoint first
        2. If no items are returned, try paginated retrieval

    Why this helper exists:
        In some LM portals, the plain endpoint returns valid data while the
        paginated form with ?size=...&offset=... may return no usable items.

    Args:
        resource_base: REST path without the portal base URL.

    Returns:
        A list of dictionaries representing the LM objects returned by the
        endpoint. Returns an empty list if nothing is found.
    """
    items = get_unpaged_results(resource_base)
    if items:
        return items

    debug_print(f"Unpaged call returned no items for {resource_base}. Trying paginated fallback.")
    return get_paginated_results(resource_base)


def get_group_by_id(group_id: int) -> Dict:
    """
    Retrieve a device group by numeric group ID.

    Args:
        group_id: LogicMonitor device group ID.

    Returns:
        Group dictionary if found, otherwise {}.
    """
    response = api_get(f"/device/groups/{group_id}")

    if isinstance(response, dict) and response.get("id") is not None:
        return response

    data = response.get("data", {})
    if isinstance(data, dict) and data.get("id") is not None:
        return data

    return {}


def get_device_by_id(device_id: int) -> Dict:
    """
    Retrieve full device details for a single device ID.

    This is used mainly when the device list payload does not contain the
    property arrays needed for --include_properties.

    Args:
        device_id: LogicMonitor device ID.

    Returns:
        Device dictionary if found, otherwise {}.
    """
    response = api_get(f"/device/devices/{device_id}")

    if isinstance(response, dict) and response.get("id") is not None:
        return response

    data = response.get("data", {})
    if isinstance(data, dict) and data.get("id") is not None:
        return data

    return {}


def normalize_group_value(value: Optional[str]) -> str:
    """
    Normalize a user-supplied group name or full path.

    Normalization:
    - convert None to empty string
    - trim whitespace
    - remove leading and trailing slashes

    Args:
        value: Group name or fullPath.

    Returns:
        Normalized string.
    """
    if not value:
        return ""
    return str(value).strip().strip("/")


def escape_filter_value(value: str) -> str:
    """
    Escape special characters for LM filter expressions.

    Escapes:
    - backslash
    - double quote

    Args:
        value: Raw filter value.

    Returns:
        Escaped string for inclusion in filter query text.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_group_filter_path(field: str, value: str) -> str:
    """
    Build a /device/groups filter query using one group field.

    Example:
        field="fullPath"
        value="Australia/Stores/Store 1"

    Produces:
        /device/groups?filter=fullPath%3A%22Australia%2FStores%2FStore%201%22

    Args:
        field: Group field name such as name or fullPath.
        value: Desired field value.

    Returns:
        REST path with encoded filter query.
    """
    filter_value = escape_filter_value(value)
    filter_expr = f'{field}:"{filter_value}"'
    encoded_filter = quote(filter_expr, safe="")
    return f"/device/groups?filter={encoded_filter}"


def get_groups_by_filter(field: str, value: str) -> List[Dict]:
    """
    Query groups using a LogicMonitor filter expression.

    Args:
        field: Group field such as name or fullPath.
        value: Match value.

    Returns:
        List of matching group dictionaries.
    """
    resource_base = build_group_filter_path(field, value)
    return get_results_prefer_unpaged(resource_base)


def dedupe_groups(groups: List[Dict]) -> List[Dict]:
    """
    Remove duplicate groups by group ID while preserving order.

    Args:
        groups: List of group dictionaries.

    Returns:
        Deduplicated list.
    """
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
    """
    Print possible group matches in table form.

    Used when --group_name resolves to multiple candidate groups.

    Args:
        groups: Candidate group dictionaries.
    """
    table = []
    for group in groups:
        table.append([
            group.get("id"),
            group.get("name"),
            group.get("fullPath"),
        ])

    print(tabulate(table, headers=["Group ID", "Name", "Full Path"], tablefmt="grid"))


def find_group_by_name(group_name: str) -> Dict:
    """
    Resolve a device group from a user-supplied name or full path.

    Search strategy:
        1. Exact fullPath match
        2. Exact name match
        3. If input looks like a path, try the leaf name
        4. If multiple leaf matches are found, fetch each group by ID and
           compare fullPath exactly

    This approach exists because users may provide either:
    - the exact group full path
    - the group name only
    - a leaf name that is ambiguous without fullPath confirmation

    Args:
        group_name: Group name or full path from --group_name.

    Returns:
        Matching group dictionary if uniquely resolved, otherwise {}.
    """
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
    """
    Retrieve devices that belong to a specific group.

    Args:
        group_id: LogicMonitor device group ID.

    Returns:
        List of device dictionaries.
    """
    return get_results_prefer_unpaged(f"/device/groups/{group_id}/devices")


def get_device_datasources(device_id: int) -> List[Dict]:
    """
    Retrieve device datasources for one device.

    Args:
        device_id: LogicMonitor device ID.

    Returns:
        List of device datasource dictionaries.
    """
    return get_results_prefer_unpaged(f"/device/devices/{device_id}/devicedatasources")


def get_datasource_instances(device_id: int, device_datasource_id: int) -> List[Dict]:
    """
    Retrieve instances for one device datasource.

    Args:
        device_id: LogicMonitor device ID.
        device_datasource_id: Device datasource ID.

    Returns:
        List of datasource instance dictionaries.
    """
    return get_results_prefer_unpaged(
        f"/device/devices/{device_id}/devicedatasources/{device_datasource_id}/instances"
    )


def get_instance_config_items(device_id: int, device_datasource_id: int, instance_id: int) -> Tuple[List[Dict], Optional[int]]:
    """
    Retrieve config records for a specific datasource instance.

    Strategy:
        1. Try the plain endpoint first
        2. If it returns no items, try the paginated form

    Args:
        device_id: LogicMonitor device ID.
        device_datasource_id: Device datasource ID.
        instance_id: Datasource instance ID.

    Returns:
        Tuple of:
            - list of config item dictionaries
            - total count if available
    """
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
    Parse a comma-separated instance filter string.

    Matching style:
        Case-insensitive substring match.

    Example:
        "running, startup" -> ["running", "startup"]

    Args:
        filter_text: Raw text from --instance_name_filter.

    Returns:
        List of normalized lowercase filter tokens.
    """
    if not filter_text:
        return []
    return [part.strip().lower() for part in filter_text.split(",") if part.strip()]


def instance_name_matches(instance_name: str, filters: List[str]) -> bool:
    """
    Evaluate whether an instance name matches the requested instance filters.

    Rules:
        - If no filters are provided, every instance is allowed
        - Matching is case-insensitive substring matching

    Examples:
        filters=["running", "startup"]
        "Running-Config" -> True
        "Startup-Config" -> True
        "Candidate-Config" -> False

    Args:
        instance_name: Datasource instance display name or name.
        filters: Lowercase substring filters.

    Returns:
        True if allowed, else False.
    """
    if not filters:
        return True
    candidate = (instance_name or "").lower()
    return any(filter_value in candidate for filter_value in filters)


def parse_include_properties(include_properties_text: Optional[str]) -> List[str]:
    """
    Parse a comma-separated property list from --include_properties.

    Duplicate property names are removed while preserving order.

    Example:
        "system.staticgroup,snmp.community" ->
            ["system.staticgroup", "snmp.community"]

    Args:
        include_properties_text: Raw CLI value.

    Returns:
        Ordered list of unique property names.
    """
    if not include_properties_text:
        return []

    props = []
    seen = set()

    for part in include_properties_text.split(","):
        value = part.strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        props.append(value)

    return props


def parse_ds_filters(filter_text: Optional[str]) -> Tuple[List[str], List[str]]:
    """
    Parse datasource filter text into include and exclude pattern lists.

    Supported syntax:
        SSH_Exec_Standard   Exact or wildcard include pattern
        SSH*                Wildcard include pattern
        !Cisco_IOS          Exclude pattern
        \\!Cisco_IOS         Literal datasource name starting with '!'

    Matching rules:
        1. Exclusions are evaluated first
        2. If include patterns exist, the datasource must match at least one
        3. If no include patterns exist, any datasource not excluded is allowed

    Notes:
        - Matching is case-insensitive
        - Wildcards are handled by fnmatch
        - Only a leading exclamation mark has special meaning

    Args:
        filter_text: Comma-separated filter expression from --filterDS.

    Returns:
        Tuple of:
            - include_patterns
            - exclude_patterns

    Examples:
        "SSH*" -> (["ssh*"], [])
        "!Cisco_IOS" -> ([], ["cisco_ios"])
        "SSH*,!Cisco_IOS" -> (["ssh*"], ["cisco_ios"])
        "\\!Cisco_IOS" -> (["!cisco_ios"], [])
    """
    include_patterns: List[str] = []
    exclude_patterns: List[str] = []

    if not filter_text:
        return include_patterns, exclude_patterns

    for raw_part in filter_text.split(","):
        part = raw_part.strip()
        if not part:
            continue

        if part.startswith("\\!"):
            pattern = part[1:].strip()
            if pattern:
                include_patterns.append(pattern.lower())
            continue

        if part.startswith("!"):
            pattern = part[1:].strip()
            if pattern:
                exclude_patterns.append(pattern.lower())
            continue

        include_patterns.append(part.lower())

    return include_patterns, exclude_patterns


def matches_pattern_case_insensitive(value: str, pattern: str) -> bool:
    """
    Compare a value against a wildcard pattern case-insensitively.

    Args:
        value: Candidate text.
        pattern: Wildcard pattern, for example SSH*.

    Returns:
        True if matched, otherwise False.
    """
    return fnmatch.fnmatch((value or "").lower(), pattern.lower())


def datasource_name_matches(datasource_name: str, include_patterns: List[str], exclude_patterns: List[str]) -> bool:
    """
    Evaluate datasource name filtering rules.

    Rules:
        - Excludes are applied first
        - If include patterns exist, datasource must match at least one include
        - If no include patterns exist, any datasource not excluded is allowed

    Examples:
        include=["ssh*"], exclude=[]:
            SSH_Exec_Standard -> True
            Cisco_IOS -> False

        include=[], exclude=["cisco_ios"]:
            Cisco_IOS -> False
            SSH_Exec_Standard -> True

    Args:
        datasource_name: Datasource name to test.
        include_patterns: Include patterns from --filterDS.
        exclude_patterns: Exclude patterns from --filterDS.

    Returns:
        True if datasource passes filters, else False.
    """
    candidate = (datasource_name or "").lower()

    if any(matches_pattern_case_insensitive(candidate, pattern) for pattern in exclude_patterns):
        return False

    if include_patterns:
        return any(matches_pattern_case_insensitive(candidate, pattern) for pattern in include_patterns)

    return True


def get_property_map_from_array(properties: Any) -> Dict[str, str]:
    """
    Convert a LogicMonitor property array into a simple name->value map.

    Expected LM property array shape:
        [
            {"name": "snmp.community", "value": "public"},
            {"name": "system.staticgroup", "value": "true"}
        ]

    Args:
        properties: List of property dictionaries.

    Returns:
        Dictionary of property names to string values.
    """
    result: Dict[str, str] = {}

    if not isinstance(properties, list):
        return result

    for item in properties:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if name is None:
            continue
        result[str(name)] = "" if value is None else str(value)

    return result


def ensure_device_property_sets(device: Dict) -> Dict:
    """
    Ensure a device dictionary contains the four property arrays used by
    --include_properties.

    Why this exists:
        Devices returned from /device/groups/{id}/devices may not always include
        customProperties, systemProperties, autoProperties, and inheritedProperties.
        If any are missing, this function retrieves the full device payload.

    Required arrays:
        - customProperties
        - systemProperties
        - autoProperties
        - inheritedProperties

    Args:
        device: Device dictionary, usually from the group devices endpoint.

    Returns:
        Original or enriched device dictionary.
    """
    required_keys = ["customProperties", "systemProperties", "autoProperties", "inheritedProperties"]

    if all(key in device for key in required_keys):
        return device

    device_id = device.get("id")
    if device_id is None:
        return device

    debug_print(f"Device {device_id} missing one or more property arrays. Fetching full device details.")
    full_device = get_device_by_id(device_id)
    if not full_device:
        return device

    merged = dict(device)
    for key in required_keys:
        if key in full_device:
            merged[key] = full_device.get(key)

    return merged


def get_device_property_value(device: Dict, property_name: str) -> str:
    """
    Look up one property across all supported property arrays.

    Search order:
        1. customProperties
        2. systemProperties
        3. autoProperties
        4. inheritedProperties

    Returns the first match found.

    Args:
        device: Device dictionary containing property arrays.
        property_name: Property to retrieve.

    Returns:
        Property value as a string, or empty string if not found.
    """
    property_sets = [
        get_property_map_from_array(device.get("customProperties")),
        get_property_map_from_array(device.get("systemProperties")),
        get_property_map_from_array(device.get("autoProperties")),
        get_property_map_from_array(device.get("inheritedProperties")),
    ]

    for prop_map in property_sets:
        if property_name in prop_map:
            return prop_map[property_name]

    return ""


def epoch_ms_to_local_string(epoch_ms: Optional[int]) -> str:
    """
    Convert a Unix epoch timestamp in milliseconds to local time text.

    Args:
        epoch_ms: Epoch milliseconds from LM.

    Returns:
        Formatted local datetime string such as:
            2026-03-29 14:30:00 AEDT
        Returns empty string for missing values.
        Returns raw value as string if conversion fails.
    """
    if epoch_ms in (None, ""):
        return ""

    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000).astimezone()
        tz_name = dt.tzname() or ""
        return dt.strftime("%Y-%m-%d %H:%M:%S") + (f" {tz_name}" if tz_name else "")
    except Exception:
        return str(epoch_ms)


def format_config_found(config_value: Any) -> str:
    """
    Convert a config payload into a friendly status string.

    Rules:
        - non-empty string -> "Config data found"
        - truthy non-string -> "Config data found"
        - empty or missing -> ""

    Args:
        config_value: Value of the config field from LM.

    Returns:
        Friendly status string for table output.
    """
    if isinstance(config_value, str):
        return "Config data found" if config_value.strip() else ""
    if config_value:
        return "Config data found"
    return ""


def display_table(data: List[List], headers: List[str], title: str = "") -> None:
    """
    Print result rows using tabulate.

    Args:
        data: Table rows.
        headers: Column headers.
        title: Optional title block shown before the table.
    """
    if title:
        print("\n" + "=" * 140)
        print(title)
        print("=" * 140)
    print(tabulate(data, headers=headers, tablefmt="grid"))


def write_csv(filepath: str, headers: List[str], rows: List[List]) -> None:
    """
    Export result rows to a CSV file.

    Creates the destination directory if it does not already exist.

    Args:
        filepath: Output CSV path.
        headers: CSV column headers.
        rows: CSV row data.
    """
    output_dir = os.path.dirname(filepath)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"CSV exported: {filepath}")


def collect_group_config_data(
    group_id: int,
    group_name: str,
    ds_include_patterns: List[str],
    ds_exclude_patterns: List[str],
    instance_name_filters: List[str],
    included_properties: List[str]
) -> List[List]:
    """
    Collect output rows for all matching config instances in a group.

    High-level flow:
        1. Get devices in the group
        2. For each device:
             - ensure device property arrays exist if needed
             - get device datasources
             - keep only ConfigSource datasources (dataSourceType == "CS")
             - apply datasource filters
             - get datasource instances
             - apply instance name filters
             - retrieve config data per instance
        3. Build rows for console/CSV output

    Output row fields:
        Group ID
        Group Name
        Device ID
        Device Name
        DeviceDataSource ID
        DataSource Name
        Instance ID
        Instance Name
        Total
        Version
        Poll Timestamp (Local)
        Config Status
        [optional included properties...]

    Args:
        group_id: Target device group ID.
        group_name: Friendly group name/full path for output.
        ds_include_patterns: Parsed include datasource filters.
        ds_exclude_patterns: Parsed exclude datasource filters.
        instance_name_filters: Parsed instance substring filters.
        included_properties: Device properties to append as extra columns.

    Returns:
        List of output rows.
    """
    rows: List[List] = []

    devices = get_devices_in_group(group_id)
    if not devices:
        print("No devices found in the group.")
        return rows

    print(f"\nFound {len(devices)} device(s) in group '{group_name}' (ID: {group_id})")
    if ds_include_patterns or ds_exclude_patterns:
        ds_filter_display = ds_include_patterns + [f"!{p}" for p in ds_exclude_patterns]
        print(f"Datasource filter(s): {', '.join(ds_filter_display)}")
    if instance_name_filters:
        print(f"Instance name filter(s): {', '.join(instance_name_filters)}")
    if included_properties:
        print(f"Included propertie(s): {', '.join(included_properties)}")

    for device in devices:
        if not isinstance(device, dict):
            continue

        device = ensure_device_property_sets(device)

        device_id = device.get("id")
        device_display_name = device.get("displayName") or device.get("name") or str(device_id)

        if device_id is None:
            continue

        property_values = [get_device_property_value(device, prop_name) for prop_name in included_properties]

        debug_print(f"Processing device {device_display_name} ({device_id})")

        datasources = get_device_datasources(device_id)
        cs_datasources = [
            ds for ds in datasources
            if isinstance(ds, dict) and ds.get("dataSourceType") == "CS"
        ]

        filtered_datasources = []
        for ds in cs_datasources:
            datasource_name = ds.get("dataSourceName") or ds.get("dataSourceDisplayName") or ""
            if datasource_name_matches(datasource_name, ds_include_patterns, ds_exclude_patterns):
                filtered_datasources.append(ds)

        debug_print(f"Found {len(cs_datasources)} ConfigSource datasource(s) on device {device_id}")
        debug_print(f"Matched {len(filtered_datasources)} datasource(s) after datasource filtering on device {device_id}")

        for ds in filtered_datasources:
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
                    base_row = [
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
                    ]
                    rows.append(base_row + property_values)
                    continue

                for item in items:
                    version = item.get("version", "")
                    poll_timestamp = item.get("pollTimestamp")
                    config_found = format_config_found(item.get("config"))

                    base_row = [
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
                    ]
                    rows.append(base_row + property_values)

    return rows


def parse_args() -> argparse.Namespace:
    """
    Define and parse command-line arguments.

    Returns:
        argparse.Namespace containing CLI values.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve ConfigSource config data for devices in a LogicMonitor group."
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"{SCRIPT_VERSION} | {SCRIPT_NAME} | {SCRIPT_DATE} | {SCRIPT_AUTHOR} | {SCRIPT_URL}"
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
        help='LogicMonitor device group name or full path. Example: --group_name "Australia/Stores/Stores/Store 1"'
    )

    parser.add_argument(
        "--filterDS",
        type=str,
        help='Comma-separated datasource filters. Supports wildcard * and exclusion with !. Escape literal ! with \\!. Examples: --filterDS "SSH_Exec_Standard", --filterDS "SSH*,!Cisco_IOS", --filterDS "\\!Cisco_IOS"'
    )

    parser.add_argument(
        "--instance_name_filter",
        type=str,
        help='Comma-separated instance name filters. Example: --instance_name_filter "running, startup"'
    )

    parser.add_argument(
        "--include_properties",
        type=str,
        help='Comma-separated device properties to include. Example: --include_properties "system.staticgroup,snmp.community"'
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
    """
    Main program entry point.

    Steps:
        1. Parse CLI arguments
        2. Validate required environment variables
        3. Resolve the target group
        4. Parse optional filters
        5. Collect group config rows
        6. Print results and optionally export CSV
    """
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
    ds_include_patterns, ds_exclude_patterns = parse_ds_filters(args.filterDS)
    instance_name_filters = parse_instance_name_filters(args.instance_name_filter)
    included_properties = parse_include_properties(args.include_properties)

    rows = collect_group_config_data(
        group_id,
        group_name,
        ds_include_patterns,
        ds_exclude_patterns,
        instance_name_filters,
        included_properties
    )

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
    ] + included_properties

    display_table(rows, headers, "LogicMonitor ConfigSource Config Data")

    if args.csv:
        write_csv(args.csv, headers, rows)


if __name__ == "__main__":
    main()
