"""
LogicMonitor API - Get Devices and Related Data
------------------------------------------------
This script retrieves devices from a LogicMonitor account, displays them in a table,
and optionally allows the user to fetch associated DataSources and their instances.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv, re
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage examples:
python Get_LMDevices_plus_more.py --help
python Get_LMDevices_plus_more.py --autoproperties true --systemproperties true
python Get_LMDevices_plus_more.py --customproperties true --showsecrets
python Get_LMDevices_plus_more.py --group 123 --autoproperties true --systemproperties true --debug
python Get_LMDevices_plus_more.py --group "Production/Linux Servers"
python Get_LMDevices_plus_more.py --autoproperties true --customproperties true --inheritedproperties true --systemproperties true

Author: Ryan Gillan
"""

import argparse
import os
import sys
import time
import hmac
import hashlib
import base64
import json
import re
import requests
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"

PROPERTY_GROUPS = {
    "autoProperties": "Auto Properties",
    "customProperties": "Custom Properties",
    "inheritedProperties": "Inherited Properties",
    "systemProperties": "System Properties",
}

PROPERTY_COLUMN_PREFIXES = {
    "autoProperties": "autoproperties",
    "customProperties": "customproperties",
    "inheritedProperties": "inheritedproperties",
    "systemProperties": "systemproperties",
}

SENSITIVE_PROPERTY_TOKENS = (
    "accesskey",
    "access_key",
    "apikey",
    "api_key",
    "secret",
    "password",
    "passwd",
    "token",
    "privatekey",
    "private_key",
)


def str_to_bool(value):
    """
    Convert a command-line true/false string to a boolean.
    """
    if isinstance(value, bool):
        return value

    normalized_value = value.strip().lower()
    if normalized_value in ("true", "t", "yes", "y", "1"):
        return True
    if normalized_value in ("false", "f", "no", "n", "0"):
        return False

    raise argparse.ArgumentTypeError("Expected true or false.")


def parse_args():
    """
    Parse command-line arguments that control which device property groups are requested.

    If the script is run without command-line arguments, print the help output and exit
    before making any API calls.
    """
    parser = argparse.ArgumentParser(
        description="Get LogicMonitor devices, DataSources, DataSource instances, and optional device properties."
    )
    parser.add_argument(
        "--autoproperties",
        "--auto-properties",
        dest="autoproperties",
        type=str_to_bool,
        default=False,
        metavar="true|false",
        help="Include device autoProperties and append them as autoproperties_<property_name> columns in the device table.",
    )
    parser.add_argument(
        "--customproperties",
        "--custom-properties",
        dest="customproperties",
        type=str_to_bool,
        default=False,
        metavar="true|false",
        help="Include device customProperties and append them as customproperties_<property_name> columns in the device table.",
    )
    parser.add_argument(
        "--inheritedproperties",
        "--inherited-properties",
        dest="inheritedproperties",
        type=str_to_bool,
        default=False,
        metavar="true|false",
        help="Include device inheritedProperties and append them as inheritedproperties_<property_name> columns in the device table.",
    )
    parser.add_argument(
        "--systemproperties",
        "--system-properties",
        dest="systemproperties",
        type=str_to_bool,
        default=False,
        metavar="true|false",
        help="Include device systemProperties and append them as systemproperties_<property_name> columns in the device table.",
    )
    parser.add_argument(
        "--group",
        "--device-group",
        dest="device_group",
        default=None,
        metavar="GROUP_ID|NAME|FULL_PATH",
        help=(
            "Only return devices that are direct members of the specified LogicMonitor "
            "device group. Accepts a numeric group ID, exact/partial group name, or full path."
        ),
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Print each GET request URL before it is sent. Authorization headers are not printed.",
    )
    parser.add_argument(
        "--showsecrets",
        "--show-secrets",
        dest="show_secrets",
        action="store_true",
        help=(
            "Display sensitive-looking property values in console output. By default, values for "
            "property names containing key/password/secret/token terms are masked in the console. "
            "Raw JSON exports are not masked."
        ),
    )
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    return parser.parse_args()


def validate_required_environment():
    """
    Validate that the required .env values are set before making API calls.
    """
    missing = [name for name, value in {
        "ACCESS_ID": ACCESS_ID,
        "ACCESS_KEY": ACCESS_KEY,
        "COMPANY": COMPANY,
    }.items() if not value]

    if missing:
        print(f"Missing required environment variable(s): {', '.join(missing)}")
        print("Create or update your .env file with ACCESS_ID, ACCESS_KEY, and COMPANY.")
        sys.exit(1)


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> dict:
    """
    Generate LogicMonitor API authentication headers.

    Query parameters must not be included in resource_path when generating the LMv1 signature.
    """
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb + epoch + data + resource_path
    hmac_hash = hmac.new(ACCESS_KEY.encode(), msg=request_vars.encode(), digestmod=hashlib.sha256).hexdigest()
    signature = base64.b64encode(hmac_hash.encode()).decode()
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"
    return {"Content-Type": "application/json", "Authorization": auth}


def api_get(resource_path: str, params: dict = None, debug: bool = False) -> dict:
    """
    Perform a GET request to the LogicMonitor API.

    The resource path is signed without query parameters. Query parameters are passed
    separately through requests so the generated LMv1 signature remains valid.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    prepared_request = requests.Request("GET", url, params=params).prepare()
    if debug:
        print(f"[DEBUG] GET {prepared_request.url}")

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return {}


def display_table(data: list, headers: list, title: str = ""):
    """
    Display data in a formatted table.
    """
    if title:
        print("\n" + "=" * 50)
        print(title)
        print("=" * 50)
    print(tabulate(data, headers=headers, tablefmt="grid"))


def fetch_paginated_items(
    resource_path: str,
    params: dict = None,
    page_size: int = 1000,
    debug: bool = False,
) -> list:
    """
    Fetch all items from a paginated LogicMonitor GET endpoint.

    LogicMonitor GET endpoints commonly support size and offset query parameters. This helper
    keeps fetching until a page returns fewer than page_size items.
    """
    all_items = []
    offset = 0
    base_params = dict(params or {})

    while True:
        request_params = dict(base_params)
        request_params["size"] = page_size
        request_params["offset"] = offset

        response = api_get(resource_path, params=request_params, debug=debug)
        data = response.get("data", {})
        items = data.get("items", [])

        if not isinstance(items, list):
            print("Invalid or missing items in API response.")
            return all_items

        all_items.extend(items)

        if len(items) < page_size:
            break

        offset += page_size

    return all_items


def build_device_query_params(
    include_auto_properties: bool = False,
    include_custom_properties: bool = False,
    include_inherited_properties: bool = False,
    include_system_properties: bool = False,
) -> dict:
    """
    Build device query parameters based on requested property groups.

    The fields query parameter is used so LogicMonitor returns the selected property arrays
    alongside the standard device fields used in the table.
    """
    base_fields = [
        "id",
        "name",
        "displayName",
        "autoBalancedCollectorGroupId",
        "disableAlerting",
        "link",
        "hostStatus",
        "enableNetflow",
        "currentCollectorId",
        "preferredCollectorGroupName",
        "preferredCollectorId",
    ]

    optional_property_fields = get_requested_property_groups(
        include_auto_properties=include_auto_properties,
        include_custom_properties=include_custom_properties,
        include_inherited_properties=include_inherited_properties,
        include_system_properties=include_system_properties,
    )

    if not optional_property_fields:
        return {}

    return {"fields": ",".join(base_fields + optional_property_fields)}


def get_requested_property_groups(
    include_auto_properties: bool = False,
    include_custom_properties: bool = False,
    include_inherited_properties: bool = False,
    include_system_properties: bool = False,
) -> list:
    """
    Return the LogicMonitor property-group field names requested by CLI flags.
    """
    requested = []
    if include_auto_properties:
        requested.append("autoProperties")
    if include_custom_properties:
        requested.append("customProperties")
    if include_inherited_properties:
        requested.append("inheritedProperties")
    if include_system_properties:
        requested.append("systemProperties")
    return requested


def normalize_group_value(value: str) -> str:
    """
    Normalize group search text for matching user input against name and fullPath.
    """
    return value.strip().strip("/").lower()


def get_all_device_groups(debug: bool = False) -> list:
    """
    Fetch all device groups with the fields needed to resolve --group values.
    """
    return fetch_paginated_items(
        "/device/groups",
        params={"fields": "id,name,fullPath,numOfHosts,numOfDirectDevices"},
        debug=debug,
    )


def resolve_device_group(device_group: str, debug: bool = False):
    """
    Resolve a --group value to a LogicMonitor device group ID.

    Numeric values are treated as group IDs. Non-numeric values are resolved by checking
    fullPath and name against all device groups. Exact matches are preferred; if there is
    only one partial match, that match is used.
    """
    if not device_group:
        return None, None

    group_value = device_group.strip()
    if group_value.isdigit():
        return group_value, f"group ID {group_value}"

    print(f"\nResolving device group: {group_value}")
    groups = get_all_device_groups(debug=debug)
    if not groups:
        print("No device groups were returned from the API.")
        return None, None

    normalized_input = normalize_group_value(group_value)

    def group_name(group):
        return str(group.get("name", "") or "")

    def group_path(group):
        return str(group.get("fullPath", "") or "")

    def normalized_name(group):
        return normalize_group_value(group_name(group))

    def normalized_path(group):
        return normalize_group_value(group_path(group))

    exact_full_path_matches = [group for group in groups if normalized_path(group) == normalized_input]
    exact_name_matches = [group for group in groups if normalized_name(group) == normalized_input]
    partial_matches = [
        group
        for group in groups
        if normalized_input in normalized_path(group) or normalized_input in normalized_name(group)
    ]

    if exact_full_path_matches:
        matches = exact_full_path_matches
        match_type = "full path"
    elif exact_name_matches:
        matches = exact_name_matches
        match_type = "name"
    else:
        matches = partial_matches
        match_type = "partial name/full path"

    if not matches:
        print(f"No device group matched: {group_value}")
        return None, None

    if len(matches) > 1:
        table_data = [
            [
                group.get("id"),
                group.get("name"),
                group.get("fullPath"),
                group.get("numOfHosts"),
                group.get("numOfDirectDevices"),
            ]
            for group in matches[:25]
        ]
        display_table(
            table_data,
            ["Group ID", "Name", "Full Path", "Total Devices", "Direct Devices"],
            f"Multiple device groups matched by {match_type}. Re-run with --group <Group ID>.",
        )
        if len(matches) > 25:
            print(f"Showing first 25 of {len(matches)} matches.")
        return None, None

    selected_group = matches[0]
    group_id = selected_group.get("id")
    group_label = selected_group.get("fullPath") or selected_group.get("name") or group_id
    print(f"Matched device group: {group_label} (ID: {group_id})")
    return group_id, group_label


def property_list_to_dict(property_list: list) -> dict:
    """
    Convert a LogicMonitor property list from [{"name": "...", "value": "..."}] to a dict.
    """
    if not isinstance(property_list, list):
        return {}

    return {
        prop.get("name"): prop.get("value")
        for prop in property_list
        if isinstance(prop, dict) and prop.get("name") is not None
    }


def should_mask_property(property_name: str) -> bool:
    """
    Decide whether a property value should be masked in console output.
    """
    normalized_name = str(property_name or "").replace(".", "").replace("-", "").lower()
    return any(token in normalized_name for token in SENSITIVE_PROPERTY_TOKENS)


def mask_property_value(property_name: str, property_value, show_secrets: bool = False):
    """
    Mask sensitive-looking property values unless --showsecrets is set.
    """
    if show_secrets or property_value in (None, ""):
        return property_value

    if should_mask_property(property_name):
        return "********"

    return property_value



def sanitize_property_column_name(property_group: str, property_name: str) -> str:
    """
    Build a safe property table column name such as customproperties_location.

    The property group is lowercased and property names are lowercased with characters
    outside A-Z, 0-9, and underscore converted to underscores.
    """
    prefix = PROPERTY_COLUMN_PREFIXES.get(property_group, str(property_group).lower())
    normalized_property_name = re.sub(r"[^0-9A-Za-z_]+", "_", str(property_name or "").strip().lower())
    normalized_property_name = re.sub(r"_+", "_", normalized_property_name).strip("_")

    if not normalized_property_name:
        normalized_property_name = "unnamed_property"

    return f"{prefix}_{normalized_property_name}"


def build_property_table_columns(devices: list, requested_property_groups: list) -> list:
    """
    Build dynamic property columns for the main device table.

    Returns a list of tuples in this form:
    (property_group, property_name, column_header)

    Property names are collected in first-seen order across returned devices. If two property
    names normalize to the same column header, a numeric suffix is appended to keep headers unique.
    """
    columns = []
    seen_property_keys = set()
    used_headers = {}

    for property_group in requested_property_groups:
        for device in devices:
            if not isinstance(device, dict):
                continue

            properties = device.get(property_group, [])
            if not isinstance(properties, list):
                continue

            for prop in properties:
                if not isinstance(prop, dict):
                    continue

                property_name = prop.get("name")
                if property_name is None:
                    continue

                property_key = (property_group, property_name)
                if property_key in seen_property_keys:
                    continue

                seen_property_keys.add(property_key)

                base_header = sanitize_property_column_name(property_group, property_name)
                header = base_header

                if header in used_headers:
                    used_headers[base_header] = used_headers.get(base_header, 1) + 1
                    header = f"{base_header}_{used_headers[base_header]}"
                else:
                    used_headers[base_header] = 1

                columns.append((property_group, property_name, header))

    return columns


def display_requested_properties(devices: list, requested_property_groups: list, show_secrets: bool = False):
    """
    Display requested property groups in readable name/value tables after the device summary.

    Raw JSON exports retain the original unmasked property values returned by the API. Console
    output masks sensitive-looking values unless --showsecrets is used.
    """
    if not requested_property_groups:
        return

    for device in devices:
        if not isinstance(device, dict):
            continue

        device_id = device.get("id")
        device_label = device.get("displayName") or device.get("name") or device_id

        print("\n" + "#" * 80)
        print(f"Requested properties for device: {device_label} (ID: {device_id})")
        print("#" * 80)

        for property_group in requested_property_groups:
            properties = device.get(property_group)

            if properties is None:
                print(f"\n{PROPERTY_GROUPS[property_group]} ({property_group}) was not returned by the API.")
                continue

            if not properties:
                print(f"\n{PROPERTY_GROUPS[property_group]} ({property_group}): []")
                continue

            property_rows = []
            for prop in properties:
                if not isinstance(prop, dict):
                    continue
                name = prop.get("name")
                value = mask_property_value(name, prop.get("value"), show_secrets=show_secrets)
                property_rows.append([name, value])

            display_table(
                property_rows,
                ["Property Name", "Property Value"],
                f"{PROPERTY_GROUPS[property_group]} ({property_group})",
            )


def get_devices(
    include_auto_properties: bool = False,
    include_custom_properties: bool = False,
    include_inherited_properties: bool = False,
    include_system_properties: bool = False,
    device_group: str = None,
    debug: bool = False,
    show_secrets: bool = False,
):
    """
    Fetch and display devices.

    If device_group is provided, only direct device members of that group are returned.
    """
    requested_property_groups = get_requested_property_groups(
        include_auto_properties=include_auto_properties,
        include_custom_properties=include_custom_properties,
        include_inherited_properties=include_inherited_properties,
        include_system_properties=include_system_properties,
    )

    query_params = build_device_query_params(
        include_auto_properties=include_auto_properties,
        include_custom_properties=include_custom_properties,
        include_inherited_properties=include_inherited_properties,
        include_system_properties=include_system_properties,
    )

    group_label = None
    if device_group:
        group_id, group_label = resolve_device_group(device_group, debug=debug)
        if not group_id:
            print("Exiting because the device group could not be resolved to one group.")
            return []
        resource_path = f"/device/groups/{group_id}/devices"
        print(f"\nFetching devices that are direct members of group: {group_label}")
    else:
        resource_path = "/device/devices"
        print("\nFetching devices...")

    if query_params:
        print(f"Using query parameters: {query_params}")

    devices = fetch_paginated_items(resource_path, params=query_params, debug=debug)

    if not devices:
        print("No devices found.")
        return []

    # Save raw JSON so any included property groups are retained for review/export.
    os.makedirs("output", exist_ok=True)
    output_file = "output/getDevicesByGroup.json" if device_group else "output/getDevices.json"
    with open(output_file, "w") as file:
        json.dump(devices, file, indent=4)

    property_table_columns = build_property_table_columns(devices, requested_property_groups)
    table_data = []

    for device in devices:
        if not isinstance(device, dict):
            continue
        device_id = device.get("id")
        name = device.get("name")
        display_name = device.get("displayName")

        # Extract properties safely. These lists are only populated when included
        # in the API response.
        auto_props = property_list_to_dict(device.get("autoProperties", []))
        sys_props = property_list_to_dict(device.get("systemProperties", []))

        manufacturer = auto_props.get("auto.endpoint.manufacturer")
        sysinfo = sys_props.get("system.sysinfo")
        description = auto_props.get("auto.entphysical.descr") or sys_props.get("system.description")

        auto_balanced_collector_group_id = device.get("autoBalancedCollectorGroupId")
        disable_alerting = device.get("disableAlerting")
        link = device.get("link")
        host_status = device.get("hostStatus")
        enable_netflow = device.get("enableNetflow")
        current_collector_id = device.get("currentCollectorId")
        preferred_collector_group_name = device.get("preferredCollectorGroupName")
        preferred_collector_id = device.get("preferredCollectorId")

        row = [
            device_id,
            name,
            display_name,
            manufacturer,
            sysinfo,
            description,
            auto_balanced_collector_group_id,
            disable_alerting,
            link,
            host_status,
            enable_netflow,
            current_collector_id,
            preferred_collector_group_name,
            preferred_collector_id,
        ]

        property_dicts_by_group = {
            property_group: property_list_to_dict(device.get(property_group, []))
            for property_group in requested_property_groups
        }

        for property_group, property_name, _column_header in property_table_columns:
            value = property_dicts_by_group.get(property_group, {}).get(property_name)
            row.append(mask_property_value(property_name, value, show_secrets=show_secrets))

        table_data.append(row)

    headers = [
        "Device ID",
        "Name",
        "Display Name",
        "Manufacturer",
        "Sysinfo",
        "Description",
        "Auto Balanced Collector Group ID",
        "Disable Alerting",
        "Link",
        "Host Status",
        "Enable NetFlow",
        "Current Collector ID",
        "Preferred Collector Group Name",
        "Preferred Collector ID",
    ] + [column_header for _property_group, _property_name, column_header in property_table_columns]

    title = f"LogicMonitor Devices - Group: {group_label}" if group_label else "LogicMonitor Devices"
    display_table(table_data, headers, title)

    if property_table_columns:
        print(f"Added {len(property_table_columns)} requested property column(s) to the device table.")

    print(f"\nSaved raw device JSON to {output_file}")
    print(f"Returned {len(devices)} device(s).")

    missing_property_groups = [
        property_group
        for property_group in requested_property_groups
        if all(property_group not in device for device in devices if isinstance(device, dict))
    ]
    if missing_property_groups:
        print(
            "\nWarning: The API response did not include the following requested property group(s): "
            + ", ".join(missing_property_groups)
        )
        print("Use --debug to confirm the fields query parameter in the request URL.")


    return devices


def get_device_datasources(device_id: str, debug: bool = False):
    """
    Fetch and display DataSources for a given device.
    """
    print(f"\nFetching DataSources for device ID: {device_id}")
    response = api_get(f"/device/devices/{device_id}/devicedatasources", debug=debug)

    items = response.get("data", {}).get("items", [])
    if not items:
        print("No DataSources found.")
        return []

    output_data = []
    for item in items:
        if not isinstance(item, dict):
            continue
        graphs = [graph.get("id") for graph in item.get("graphs", []) if isinstance(graph, dict)]
        output_data.append([
            item.get("dataSourceId"),
            item.get("dataSourceName"),
            item.get("deviceName"),
            item.get("deviceDisplayName"),
            graphs
        ])

    headers = ["DataSource ID", "Name", "Device Name", "Display Name", "Graph IDs"]
    display_table(output_data, headers, "Device DataSources")

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/getDeviceDataSources.json", "w") as file:
        json.dump(items, file, indent=4)

    return items


def get_datasource_instances(device_id: str, datasource_id: str, debug: bool = False):
    """
    Fetch and display instances for a given DataSource.
    """
    print(f"\nFetching instances for DataSource ID: {datasource_id}")
    response = api_get(
        f"/device/devices/{device_id}/devicedatasources/{datasource_id}/instances",
        debug=debug,
    )

    items = response.get("data", {}).get("items", [])
    if not items:
        print("No instances found.")
        return

    table_data = []
    for item in items:
        if not isinstance(item, dict):
            continue
        table_data.append([
            item.get("deviceDataSourceId"),
            item.get("name"),
            item.get("deviceDisplayName"),
            item.get("id"),
            item.get("dataSourceId")
        ])

    headers = ["Device DataSource ID", "Name", "Device Display Name", "Instance ID", "DataSource ID"]
    display_table(table_data, headers, "DataSource Instances")

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/output_getDatasourceInstances.json", "w") as file:
        json.dump(items, file, indent=4)


if __name__ == "__main__":
    args = parse_args()
    validate_required_environment()

    devices = get_devices(
        include_auto_properties=args.autoproperties,
        include_custom_properties=args.customproperties,
        include_inherited_properties=args.inheritedproperties,
        include_system_properties=args.systemproperties,
        device_group=args.device_group,
        debug=args.debug,
        show_secrets=args.show_secrets,
    )

    if devices and input("\nWould you like to capture datasources and instances for a device? (y/n): ").lower() == "y":
        device_id = input("Enter the device ID: ")
        datasources = get_device_datasources(device_id, debug=args.debug)

        if datasources and input("\nProceed with getting DataSource instances? (y/n): ").lower() == "y":
            datasource_id = input("Enter the DataSource ID: ")
            get_datasource_instances(device_id, datasource_id, debug=args.debug)
        else:
            print("Exiting without fetching instances.")
    else:
        print("Exiting without further actions.")
