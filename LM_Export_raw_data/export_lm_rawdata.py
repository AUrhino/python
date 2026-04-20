"""
LogicMonitor API - Export Raw Module Data
-----------------------------------------
This script follows the same auth and structure style as Get-LMCollectors_v2.py,
but makes the target LM module (datasource) an argument.

What it does:
1. Find the target device group by fullPath.
2. Optionally include descendant subgroups.
3. Get all devices in scope.
4. For each device, find the requested module/datasource assignment.
5. For each datasource instance, pull raw data for either:
      - explicitly requested datapoints, or
      - all datapoints discovered from the datasource definition.
6. Export the raw timeseries to CSV.

Supported examples:
- Show matching inventory:
    python3 export_lm_rawdata.py --show-all --module "SNMP_Network_Interfaces"

- Export all datapoints for a module:
    python3 export_lm_rawdata.py --csv snmp_interfaces.csv --module "SNMP_Network_Interfaces"

- Export only selected datapoints:
    python3 export_lm_rawdata.py --csv util.csv --module "SNMP_Network_Interfaces" \
        --datapoints InUtilizationPercent OutUtilizationPercent

- Export discovered datapoints matching a regex:
    python3 export_lm_rawdata.py --csv temp.csv --module "Some_Module" \
        --datapoint-regex "temp|humidity"

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name


Written by Ryan Gillan
"""

import argparse
import base64
import csv
import hashlib
import hmac
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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

DEFAULT_GROUP_PATH = "All devices"
DEFAULT_DAYS = 7
DEFAULT_CHUNK_HOURS = 6


def validate_env() -> None:
    """
    Validate required environment variables.
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
    Print debug messages when debug mode is enabled.
    """
    if DEBUG:
        print(f"[DEBUG] {message}")


def generate_auth_headers(
    http_verb: str,
    resource_path: str,
    data: str = "",
    api_version: Optional[str] = None,
) -> Dict[str, str]:
    """
    Generate LogicMonitor API authentication headers.

    Preserves the working auth style from the example script.
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

    headers = {
        "Authorization": auth,
        "Accept": "application/json",
    }

    if http_verb.upper() != "GET":
        headers["Content-Type"] = "application/json"

    if api_version:
        headers["X-Version"] = str(api_version)

    return headers


def debug_headers(headers: Dict[str, str]) -> None:
    """
    Print headers safely for debug.
    """
    if not DEBUG:
        return

    safe_headers = dict(headers)
    if "Authorization" in safe_headers:
        auth_value = safe_headers["Authorization"]
        if len(auth_value) > 24:
            safe_headers["Authorization"] = auth_value[:20] + "...[redacted]"
        else:
            safe_headers["Authorization"] = "[redacted]"

    debug_print(f"Request headers: {safe_headers}")


def do_get(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    api_version: Optional[str] = None,
) -> requests.Response:
    """
    Perform the raw GET request and return the response object.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path, api_version=api_version)

    debug_print(f"API endpoint: GET {resource_path}")
    debug_print(f"Full URL: {url}")
    if params:
        debug_print(f"Query params: {params}")
    debug_headers(headers)

    response = requests.get(url, headers=headers, params=params, timeout=60)

    debug_print(f"Final requested URL: {response.request.url}")
    debug_print(f"HTTP status: {response.status_code} {response.reason}")
    if response.status_code != 200:
        debug_print(f"Response body: {response.text[:2000]}")

    return response


def api_get(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    api_version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Perform a GET request to the LogicMonitor API.
    """
    try:
        response = do_get(resource_path, params=params, api_version=api_version)
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return {}

    if response.status_code == 200:
        try:
            payload = response.json()
            debug_print(
                f"Response JSON keys: {list(payload.keys()) if isinstance(payload, dict) else 'non-dict response'}"
            )
            return payload
        except ValueError:
            print("Error: Response was not valid JSON.")
            return {}

    print(f"Error: HTTP {response.status_code} {response.reason}")
    if response.text:
        print(response.text)
    return {}


def display_table(data: List[List[Any]], headers: List[str], title: str = "") -> None:
    """
    Display data in a formatted ASCII table.
    """
    if title:
        print("\n" + "=" * 80)
        print(title)
        print("=" * 80)

    print(tabulate(data, headers=headers, tablefmt="grid"))


def write_csv(filename: str, headers: List[str], rows: List[List[Any]]) -> None:
    """
    Write rows to CSV.
    """
    with open(filename, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"\nCSV written to: {filename}")


def pick(obj: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Return the first matching key from a dict.
    """
    for key in keys:
        if isinstance(obj, dict) and key in obj:
            return obj[key]
    return default


def extract_items_and_total(response: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Support both top-level and nested pagination response formats.
    """
    if not isinstance(response, dict):
        return [], None

    if isinstance(response.get("items"), list):
        items = response.get("items", [])
        total = response.get("total")
        return items, total

    data = response.get("data", {})
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        items = data.get("items", [])
        total = data.get("total")
        return items, total

    if isinstance(response.get("data"), list):
        items = response.get("data", [])
        total = response.get("total")
        return items, total

    return [], None


def extract_data_object(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract nested data object when present.
    """
    if not isinstance(response, dict):
        return {}

    data = response.get("data")
    if isinstance(data, dict):
        return data

    return response


def paginate(
    resource_path: str,
    fields: Optional[Sequence[str]] = None,
    extra_params: Optional[Dict[str, Any]] = None,
    page_size: int = 1000,
    api_version: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Generic pagination helper.
    """
    offset = 0
    while True:
        params: Dict[str, Any] = {
            "size": page_size,
            "offset": offset,
        }

        if fields:
            params["fields"] = ",".join(fields)

        if extra_params:
            params.update(extra_params)

        response = api_get(resource_path, params=params, api_version=api_version)
        items, _ = extract_items_and_total(response)

        if not items:
            break

        for item in items:
            yield item

        if len(items) < page_size:
            break

        offset += page_size


def epoch_to_iso_utc(value: Any) -> str:
    """
    Convert epoch seconds or milliseconds to ISO UTC.
    """
    if value in (None, ""):
        return ""

    try:
        ts = float(value)
    except (TypeError, ValueError):
        return ""

    if ts > 10_000_000_000:
        ts = ts / 1000.0

    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def normalize_instance_series(
    response: Dict[str, Any],
    requested_datapoints: Sequence[str],
) -> List[Dict[str, Any]]:
    """
    Normalize LogicMonitor instance data into row-oriented records.

    Expected shapes commonly look like:
    - {data:{time:[...], values:[[...],[...]]}}
    - {data:{time:[...], values:[[dp1, dp2], [dp1, dp2], ...]}}
    """
    payload = extract_data_object(response)
    times = pick(payload, "time", default=[]) or []
    values = pick(payload, "values", default=[]) or []
    err_msg = pick(payload, "errMsg", "err_msg", default="")

    if err_msg:
        debug_print(f"Data endpoint returned errMsg: {err_msg}")

    if not isinstance(times, list) or not times:
        return []

    series_by_dp: Dict[str, List[Any]] = {}

    if isinstance(values, list) and values:
        # Shape 1: values[datapoint_index][time_index]
        if (
            len(values) == len(requested_datapoints)
            and all(isinstance(row, list) for row in values)
            and all(len(row) == len(times) for row in values)
        ):
            for dp, row in zip(requested_datapoints, values):
                series_by_dp[dp] = row

        # Shape 2: values[time_index][datapoint_index]
        elif (
            len(values) == len(times)
            and all(isinstance(row, list) for row in values)
        ):
            for dp_index, dp in enumerate(requested_datapoints):
                series_by_dp[dp] = [
                    row[dp_index] if dp_index < len(row) else None for row in values
                ]

        # Shape 3: single datapoint flat list
        elif len(requested_datapoints) == 1 and len(values) == len(times):
            series_by_dp[requested_datapoints[0]] = values

    for dp in requested_datapoints:
        series_by_dp.setdefault(dp, [None] * len(times))

    rows: List[Dict[str, Any]] = []
    for index, ts in enumerate(times):
        row = {
            "timestamp_epoch": ts,
            "timestamp_utc": epoch_to_iso_utc(ts),
        }
        for dp in requested_datapoints:
            row[dp] = series_by_dp[dp][index] if index < len(series_by_dp[dp]) else None
        rows.append(row)

    return rows


def get_all_groups() -> List[Dict[str, Any]]:
    """
    Fetch all device groups.
    """
    return list(
        paginate(
            "/device/groups",
            fields=["id", "name", "fullPath", "parentId"],
            page_size=1000,
        )
    )


def find_group_by_full_path(groups: List[Dict[str, Any]], group_path: str) -> Optional[Dict[str, Any]]:
    """
    Find a device group by exact fullPath, then case-insensitive fullPath.
    """
    for group in groups:
        full_path = pick(group, "fullPath", "full_path")
        if full_path == group_path:
            return group

    for group in groups:
        full_path = pick(group, "fullPath", "full_path", default="")
        if str(full_path).casefold() == group_path.casefold():
            return group

    return None


def get_groups_in_scope(groups: List[Dict[str, Any]], root_group_path: str, include_subgroups: bool) -> List[Dict[str, Any]]:
    """
    Return the matched group and optionally all descendants by fullPath prefix.
    """
    if not include_subgroups:
        return [
            group for group in groups
            if pick(group, "fullPath", "full_path") == root_group_path
        ]

    prefix = root_group_path.rstrip("/") + "/"
    return [
        group for group in groups
        if pick(group, "fullPath", "full_path") == root_group_path
        or str(pick(group, "fullPath", "full_path", default="")).startswith(prefix)
    ]


def get_devices_for_groups(groups_in_scope: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Get all devices for the provided group list.
    Deduplicate by device ID.
    """
    devices_by_id: Dict[Any, Dict[str, Any]] = {}

    for group in groups_in_scope:
        group_id = pick(group, "id")
        if group_id is None:
            continue

        for device in paginate(
            f"/device/groups/{group_id}/devices",
            fields=["id", "displayName", "name"],
            page_size=1000,
        ):
            device_id = pick(device, "id")
            if device_id is not None and device_id not in devices_by_id:
                devices_by_id[device_id] = device

    devices = list(devices_by_id.values())
    devices.sort(key=lambda d: str(pick(d, "displayName", "display_name", "name", default="")).casefold())
    return devices


def get_matching_devicedatasources(device_id: Any, module_name: str) -> List[Dict[str, Any]]:
    """
    Get matching device-datasource assignments for a device.

    module_name may be the datasource internal name or display name.
    """
    matches: List[Dict[str, Any]] = []

    for device_ds in paginate(
        f"/device/devices/{device_id}/devicedatasources",
        fields=[
            "id",
            "dataSourceId",
            "dataSourceName",
            "dataSourceDisplayName",
            "instanceNumber",
        ],
        page_size=1000,
    ):
        ds_name = str(pick(device_ds, "dataSourceName", "data_source_name", default=""))
        ds_display = str(pick(device_ds, "dataSourceDisplayName", "data_source_display_name", default=""))

        if ds_name.casefold() == module_name.casefold() or ds_display.casefold() == module_name.casefold():
            matches.append(device_ds)

    return matches


def get_instances(device_id: Any, device_ds_id: Any) -> List[Dict[str, Any]]:
    """
    Get all instances for a device datasource assignment.
    """
    instances = list(
        paginate(
            f"/device/devices/{device_id}/devicedatasources/{device_ds_id}/instances",
            fields=["id", "displayName", "name", "wildValue", "wildValue2"],
            page_size=1000,
        )
    )
    instances.sort(key=lambda i: str(pick(i, "displayName", "display_name", "name", default="")).casefold())
    return instances


def get_datasource_definition(datasource_id: Any) -> Dict[str, Any]:
    """
    Get datasource definition by datasource ID so datapoints can be discovered.
    """
    response = api_get(
        f"/setting/datasources/{datasource_id}",
        params={"fields": "id,name,displayName,dataPoints"},
    )
    return extract_data_object(response)


def extract_datapoint_names(datasource_definition: Dict[str, Any]) -> List[str]:
    """
    Extract datasource datapoint names from a datasource definition.
    """
    datapoints = pick(datasource_definition, "dataPoints", "data_points", default=[]) or []
    names: List[str] = []

    if isinstance(datapoints, list):
        for dp in datapoints:
            if not isinstance(dp, dict):
                continue
            name = pick(dp, "name", default=None)
            if name:
                names.append(str(name))

    # preserve order, remove duplicates
    deduped: List[str] = []
    seen = set()
    for name in names:
        if name not in seen:
            seen.add(name)
            deduped.append(name)

    return deduped


def filter_datapoints_by_regex(datapoints: Sequence[str], regex_pattern: Optional[str]) -> List[str]:
    """
    Optionally filter datapoint names by regex.
    """
    if not regex_pattern:
        return list(datapoints)

    compiled = re.compile(regex_pattern)
    return [name for name in datapoints if compiled.search(name)]


def resolve_datapoints(
    inventory: List[Dict[str, Any]],
    explicit_datapoints: Optional[Sequence[str]],
    datapoint_regex: Optional[str],
) -> List[str]:
    """
    Resolve which datapoints to export.

    Priority:
    1. Explicit --datapoints
    2. Discover all datapoints from datasource definition, optionally filtered by regex
    """
    if explicit_datapoints:
        resolved = [str(dp) for dp in explicit_datapoints if str(dp).strip()]
        if datapoint_regex:
            resolved = filter_datapoints_by_regex(resolved, datapoint_regex)
        return resolved

    first_data_source_id = None
    for item in inventory:
        first_data_source_id = item.get("dataSourceId")
        if first_data_source_id is not None:
            break

    if first_data_source_id is None:
        return []

    datasource_definition = get_datasource_definition(first_data_source_id)
    discovered = extract_datapoint_names(datasource_definition)
    discovered = filter_datapoints_by_regex(discovered, datapoint_regex)
    return discovered


def get_instance_data(
    device_id: Any,
    device_ds_id: Any,
    instance_id: Any,
    datapoints: Sequence[str],
    start_epoch: int,
    end_epoch: int,
    chunk_hours: int,
) -> List[Dict[str, Any]]:
    """
    Fetch timeseries for one instance in chunks.
    """
    rows: List[Dict[str, Any]] = []
    chunk_seconds = chunk_hours * 3600
    chunk_start = start_epoch

    while chunk_start < end_epoch:
        chunk_end = min(chunk_start + chunk_seconds - 1, end_epoch)

        params: Dict[str, Any] = {
            "start": chunk_start,
            "end": chunk_end,
            "format": "json",
        }

        if datapoints:
            params["datapoints"] = ",".join(datapoints)

        response = api_get(
            f"/device/devices/{device_id}/devicedatasources/{device_ds_id}/instances/{instance_id}/data",
            params=params,
        )

        rows.extend(normalize_instance_series(response, datapoints))
        chunk_start = chunk_end + 1

    return rows


def build_inventory_rows(matches: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Build summary table rows for matched datasource instances.
    """
    rows: List[List[Any]] = []

    for item in matches:
        rows.append([
            item.get("groupFullPath"),
            item.get("deviceId"),
            item.get("deviceDisplayName"),
            item.get("deviceName"),
            item.get("deviceDatasourceId"),
            item.get("dataSourceId"),
            item.get("moduleName"),
            item.get("moduleDisplayName"),
            item.get("instanceId"),
            item.get("instanceDisplayName"),
            item.get("instanceName"),
            item.get("wildValue"),
            item.get("wildValue2"),
        ])

    return rows


def get_inventory_headers() -> List[str]:
    return [
        "Group Full Path",
        "Device ID",
        "Device Display Name",
        "Device Name",
        "DeviceDatasource ID",
        "DataSource ID",
        "Module Name",
        "Module Display Name",
        "Instance ID",
        "Instance Display Name",
        "Instance Name",
        "WildValue",
        "WildValue2",
    ]


def get_export_headers(datapoints: Sequence[str]) -> List[str]:
    return [
        "Group Full Path",
        "Device ID",
        "Device Display Name",
        "Device Name",
        "DeviceDatasource ID",
        "DataSource ID",
        "Module Name",
        "Module Display Name",
        "Instance ID",
        "Instance Display Name",
        "Instance Name",
        "WildValue",
        "WildValue2",
        "Timestamp Epoch",
        "Timestamp UTC",
        *datapoints,
    ]


def collect_inventory(
    group_path: str,
    module_name: str,
    include_subgroups: bool,
) -> List[Dict[str, Any]]:
    """
    Build an inventory of matching devices/datasources/instances.
    """
    print("\nLoading groups...")
    all_groups = get_all_groups()

    target_group = find_group_by_full_path(all_groups, group_path)
    if not target_group:
        print(f'Group not found: "{group_path}"')
        return []

    resolved_group_path = str(pick(target_group, "fullPath", "full_path"))
    debug_print(f"Resolved group path: {resolved_group_path}")

    groups_in_scope = get_groups_in_scope(all_groups, resolved_group_path, include_subgroups)
    print(f"Groups in scope: {len(groups_in_scope)}")

    devices = get_devices_for_groups(groups_in_scope)
    print(f"Devices in scope: {len(devices)}")

    inventory: List[Dict[str, Any]] = []

    for device in devices:
        device_id = pick(device, "id")
        device_display_name = pick(device, "displayName", "display_name", default="")
        device_name = pick(device, "name", default="")

        print(f"Processing device: {device_display_name or device_name} ({device_id})")

        matching_datasources = get_matching_devicedatasources(device_id, module_name)
        if not matching_datasources:
            continue

        for device_ds in matching_datasources:
            device_ds_id = pick(device_ds, "id")
            data_source_id = pick(device_ds, "dataSourceId", "data_source_id")
            ds_name = pick(device_ds, "dataSourceName", "data_source_name", default="")
            ds_display = pick(device_ds, "dataSourceDisplayName", "data_source_display_name", default="")

            instances = get_instances(device_id, device_ds_id)
            for instance in instances:
                inventory.append({
                    "groupFullPath": resolved_group_path,
                    "deviceId": device_id,
                    "deviceDisplayName": device_display_name,
                    "deviceName": device_name,
                    "deviceDatasourceId": device_ds_id,
                    "dataSourceId": data_source_id,
                    "moduleName": ds_name,
                    "moduleDisplayName": ds_display,
                    "instanceId": pick(instance, "id"),
                    "instanceDisplayName": pick(instance, "displayName", "display_name", default=""),
                    "instanceName": pick(instance, "name", default=""),
                    "wildValue": pick(instance, "wildValue", "wild_value", default=""),
                    "wildValue2": pick(instance, "wildValue2", "wild_value2", default=""),
                })

    return inventory


def show_all_inventory(
    group_path: str,
    module_name: str,
    include_subgroups: bool,
) -> None:
    """
    Show inventory of matched datasource instances.
    """
    inventory = collect_inventory(
        group_path=group_path,
        module_name=module_name,
        include_subgroups=include_subgroups,
    )

    if not inventory:
        print("No matching datasource instances found.")
        return

    headers = get_inventory_headers()
    rows = build_inventory_rows(inventory)
    display_table(rows, headers, f'LogicMonitor Raw Module Export - Matching Inventory ({module_name})')
    print(f"\nMatched datasource instances: {len(inventory)}")


def export_raw_module_data_csv(
    csv_filename: str,
    group_path: str,
    module_name: str,
    explicit_datapoints: Optional[Sequence[str]],
    datapoint_regex: Optional[str],
    days: int,
    include_subgroups: bool,
    chunk_hours: int,
) -> None:
    """
    Export raw timeseries data to CSV.
    """
    inventory = collect_inventory(
        group_path=group_path,
        module_name=module_name,
        include_subgroups=include_subgroups,
    )

    if not inventory:
        print("No matching datasource instances found.")
        return

    resolved_datapoints = resolve_datapoints(
        inventory=inventory,
        explicit_datapoints=explicit_datapoints,
        datapoint_regex=datapoint_regex,
    )

    if not resolved_datapoints:
        print("No datapoints resolved. Use --datapoints explicitly or adjust --datapoint-regex.")
        return

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)
    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())

    print(f"\nResolved datapoints ({len(resolved_datapoints)}):")
    print(", ".join(resolved_datapoints))

    export_headers = get_export_headers(resolved_datapoints)
    export_rows: List[List[Any]] = []

    print(f"\nCollecting {days} days of data...")
    print(f"Time window UTC: {start_dt.isoformat()} -> {end_dt.isoformat()}")

    total_instances = len(inventory)
    processed_instances = 0
    total_points = 0

    for item in inventory:
        processed_instances += 1

        print(
            f"[{processed_instances}/{total_instances}] "
            f"{item['deviceDisplayName']} :: {item['instanceDisplayName'] or item['instanceName']}"
        )

        series_rows = get_instance_data(
            device_id=item["deviceId"],
            device_ds_id=item["deviceDatasourceId"],
            instance_id=item["instanceId"],
            datapoints=resolved_datapoints,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            chunk_hours=chunk_hours,
        )

        for series_row in series_rows:
            export_rows.append([
                item["groupFullPath"],
                item["deviceId"],
                item["deviceDisplayName"],
                item["deviceName"],
                item["deviceDatasourceId"],
                item["dataSourceId"],
                item["moduleName"],
                item["moduleDisplayName"],
                item["instanceId"],
                item["instanceDisplayName"],
                item["instanceName"],
                item["wildValue"],
                item["wildValue2"],
                series_row.get("timestamp_epoch"),
                series_row.get("timestamp_utc"),
                *[series_row.get(dp) for dp in resolved_datapoints],
            ])

        total_points += len(series_rows)

    write_csv(csv_filename, export_headers, export_rows)

    summary_headers = ["Module", "Matched Instances", "Resolved Datapoints", "Exported Rows", "Days", "Chunk Hours"]
    summary_rows = [[module_name, total_instances, len(resolved_datapoints), total_points, days, chunk_hours]]
    display_table(summary_rows, summary_headers, "Export Summary")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Export LogicMonitor raw instance data for a given module/datasource."
    )

    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Display matched datasource instances in an ASCII table."
    )

    parser.add_argument(
        "--csv",
        help="Write raw datasource timeseries to CSV."
    )

    parser.add_argument(
        "--group-path",
        default=DEFAULT_GROUP_PATH,
        help=f'Device group fullPath (default: "{DEFAULT_GROUP_PATH}")'
    )

    parser.add_argument(
        "--module",
        required=True,
        help='Datasource/module name or display name, eg "SNMP_Network_Interfaces".'
    )

    parser.add_argument(
        "--datapoints",
        nargs="+",
        help="Optional explicit datapoints to export. If omitted, all module datapoints are discovered automatically."
    )

    parser.add_argument(
        "--datapoint-regex",
        help="Optional regex to filter discovered datapoints or explicit datapoints."
    )

    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days of history to collect (default: {DEFAULT_DAYS})."
    )

    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=DEFAULT_CHUNK_HOURS,
        help=f"Hours per API data request chunk (default: {DEFAULT_CHUNK_HOURS})."
    )

    parser.add_argument(
        "--no-subgroups",
        action="store_true",
        help="Do not include descendant subgroups under the target group."
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output and print API endpoints, headers, and HTTP codes."
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if not args.show_all and not args.csv:
        parser.print_help()
        sys.exit(0)

    return args


if __name__ == "__main__":
    args = parse_args()
    DEBUG = args.debug
    validate_env()

    include_subgroups = not args.no_subgroups

    if args.show_all:
        show_all_inventory(
            group_path=args.group_path,
            module_name=args.module,
            include_subgroups=include_subgroups,
        )

    if args.csv:
        export_raw_module_data_csv(
            csv_filename=args.csv,
            group_path=args.group_path,
            module_name=args.module,
            explicit_datapoints=args.datapoints,
            datapoint_regex=args.datapoint_regex,
            days=args.days,
            include_subgroups=include_subgroups,
            chunk_hours=args.chunk_hours,
        )

# eof
