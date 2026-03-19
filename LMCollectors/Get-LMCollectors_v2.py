"""
LogicMonitor API - Get Collectors
---------------------------------
This script retrieves LogicMonitor collectors and displays them in an ASCII table.

Behavior:
- --show-all uses the working list endpoint from the original script.
- --detailed uses the working list endpoint to get IDs, then loops over:
    /setting/collector/collectors/{id}
  with X-Version: 3 to retrieve wrapperConf, collectorConf, and other detailed fields.
- --csv writes detailed output to a CSV file when used with --detailed.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage:
- Show help:
    python3 Get-LMCollectors.py

- Show collector summary:
    python3 Get-LMCollectors.py --show-all

- Show detailed collector view:
    python3 Get-LMCollectors.py --detailed

- Show detailed collector view and export to CSV:
    python3 Get-LMCollectors.py --detailed --csv collectors_detailed.csv

- Enable debug output:
    python3 Get-LMCollectors.py --detailed --debug
"""

import argparse
import base64
import csv
import hashlib
import hmac
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

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

    Preserves the working auth style from the original script and
    optionally adds X-Version for collector-style endpoints.
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

    response = requests.get(url, headers=headers, params=params, timeout=30)

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

    print(f"\nDetailed CSV written to: {filename}")


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


def extract_single_item(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract a single object from a detail response.
    """
    if not isinstance(response, dict):
        return None

    data = response.get("data")
    if isinstance(data, dict):
        return data

    if "id" in response:
        return response

    return None


def normalize_config_blob(config_text: Optional[str]) -> str:
    """
    Normalize config text so both escaped and real newlines parse correctly.

    Handles:
    - '\\r\\n'
    - '\\n'
    - '\r\n'
    - '\r'
    """
    if not config_text:
        return ""

    normalized = str(config_text)
    normalized = normalized.replace("\\r\\n", "\n")
    normalized = normalized.replace("\\n", "\n")
    normalized = normalized.replace("\r\n", "\n")
    normalized = normalized.replace("\r", "\n")
    return normalized


def parse_config_block(config_text: Optional[str]) -> Dict[str, str]:
    """
    Parse key=value lines into a dictionary.
    """
    parsed: Dict[str, str] = {}
    normalized = normalize_config_blob(config_text)

    if not normalized:
        return parsed

    for line in normalized.split("\n"):
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()

    return parsed


def get_wrapper_conf_value(collector: Dict[str, Any], key: str) -> Optional[str]:
    """
    Safely retrieve a specific key from wrapperConf.
    """
    wrapper_conf = collector.get("wrapperConf", "")
    parsed_conf = parse_config_block(wrapper_conf)
    return parsed_conf.get(key)


def get_collector_conf_value(collector: Dict[str, Any], key: str) -> Optional[str]:
    """
    Safely retrieve a specific key from collectorConf.
    """
    collector_conf = collector.get("collectorConf", "")
    parsed_conf = parse_config_block(collector_conf)
    return parsed_conf.get(key)


def get_java_size(java_maxmemory: Optional[str]) -> str:
    """
    Map Java Max Memory to a JavaSize label.
    """
    if java_maxmemory is None:
        return "Unknown"

    try:
        value = int(str(java_maxmemory).strip())
    except (TypeError, ValueError):
        return "Unknown"

    java_size_map = {
        1024: "Small",
        2048: "Medium",
        4096: "Large",
        8192: "XL",
        16384: "XXL",
    }

    return java_size_map.get(value, "Unknown")


def get_collector_est_size(number_of_instances: Any) -> str:
    """
    Map numberOfInstances to a CollectorEstSize label
    using the exact threshold logic requested.
    """
    try:
        value = int(number_of_instances)
    except (TypeError, ValueError):
        return "UNKNOWN"

    if value < 15000:
        return "Small"
    if value < 50000 and value > 15000:
        return "Medium"
    if value < 200000 and value > 50000:
        return "Large"
    if value < 400000 and value > 200000:
        return "XL"
    if value < 750000 and value > 400000:
        return "XXL"
    return "UNKNOWN"


def get_collectors_page(size: int = 1000, offset: int = 0) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Fetch collectors using the original working endpoint.
    """
    resource_path = "/setting/collectors"
    response = api_get(resource_path)

    items, total = extract_items_and_total(response)

    debug_print(f"Parsed items: {len(items)}")
    debug_print(f"Parsed total: {total}")

    return items, total


def get_all_collectors(page_size: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch all collectors.
    """
    page_items, _ = get_collectors_page(size=page_size, offset=0)
    collectors = page_items if page_items else []
    collectors.sort(key=lambda c: (c.get("id") is None, c.get("id")))
    return collectors


def get_collector_detail(collector_id: Any) -> Optional[Dict[str, Any]]:
    """
    Fetch a collector from the collector endpoint:
    /setting/collector/collectors/{id}

    Uses X-Version: 3.
    """
    resource_path = f"/setting/collector/collectors/{collector_id}"
    response = api_get(resource_path, api_version="3")
    item = extract_single_item(response)

    if not item:
        debug_print(f"No detail payload returned for collector ID {collector_id}")
        return None

    wrapper_conf = item.get("wrapperConf", "")
    collector_conf = item.get("collectorConf", "")

    if wrapper_conf:
        debug_print(f"wrapperConf found for collector ID {collector_id}")
    else:
        debug_print(f"wrapperConf missing or empty for collector ID {collector_id}")

    if collector_conf:
        debug_print(f"collectorConf found for collector ID {collector_id}")
    else:
        debug_print(f"collectorConf missing or empty for collector ID {collector_id}")

    return item


def get_all_collectors_detailed() -> List[Dict[str, Any]]:
    """
    Fetch collector list, then loop over the collector endpoint for each ID.
    """
    collectors = get_all_collectors()
    detailed_collectors: List[Dict[str, Any]] = []

    for collector in collectors:
        collector_id = collector.get("id")

        if collector_id is None:
            detailed_collectors.append(collector)
            continue

        detail = get_collector_detail(collector_id)

        if detail:
            merged = dict(collector)
            merged.update(detail)
            detailed_collectors.append(merged)
        else:
            detailed_collectors.append(collector)

    detailed_collectors.sort(key=lambda c: (c.get("id") is None, c.get("id")))
    return detailed_collectors


def build_summary_rows(collectors: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Build summary table rows for collectors.
    """
    rows = []
    for collector in collectors:
        if not isinstance(collector, dict):
            continue

        rows.append([
            collector.get("id"),
            collector.get("hostname"),
            collector.get("description"),
            collector.get("collectorSize"),
            collector.get("arch"),
            collector.get("build"),
            collector.get("isDown"),
            collector.get("status"),
            collector.get("collectorGroupName"),
        ])
    return rows


def build_detailed_rows(collectors: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Build detailed table rows for collectors.

    Important:
    - Java Init/Max Memory come from wrapperConf
    - JavaSize is derived from Java Max Memory
    - CollectorEstSize is derived from numberOfInstances
    - The rest of the custom collector settings come from collectorConf
    """
    rows = []
    for collector in collectors:
        if not isinstance(collector, dict):
            continue

        java_init_memory = get_wrapper_conf_value(collector, "wrapper.java.initmemory")
        java_max_memory = get_wrapper_conf_value(collector, "wrapper.java.maxmemory")
        java_size = get_java_size(java_max_memory)

        number_of_instances = collector.get("numberOfInstances")
        collector_est_size = get_collector_est_size(number_of_instances)

        rows.append([
            collector.get("id"),
            collector.get("hostname"),
            collector.get("description"),
            collector.get("collectorSize"),
            collector.get("arch"),
            collector.get("build"),
            collector.get("isDown"),
            collector.get("status"),
            collector.get("collectorGroupName"),
            number_of_instances,
            collector_est_size,
            collector.get("numberOfHosts"),
            collector.get("numberOfSDTs"),
            collector.get("numberOfWebsites"),
            collector.get("enableFailBack"),
            collector.get("inSDT"),
            collector.get("isAdminAccount"),
            java_init_memory,
            java_max_memory,
            java_size,
            get_collector_conf_value(collector, "remotesession.disable"),
            get_collector_conf_value(collector, "collector.batchscript.threadpool"),
            get_collector_conf_value(collector, "configcollector.script.threadpool"),
            get_collector_conf_value(collector, "collector.snmp.threadpool"),
            get_collector_conf_value(collector, "collector.wmi.threadpool"),
            get_collector_conf_value(collector, "proxy.enable"),
            get_collector_conf_value(collector, "proxy.host"),
            get_collector_conf_value(collector, "proxy.port"),
            get_collector_conf_value(collector, "proxy.user"),
            get_collector_conf_value(collector, "proxy.pass"),
            get_collector_conf_value(collector, "collector.script.cache.isPersistence"),
            get_collector_conf_value(collector, "collector.script.cache.switch.to.secondary"),
        ])
    return rows


def get_summary_headers() -> List[str]:
    return [
        "ID",
        "Hostname",
        "Description",
        "Collector Size",
        "Arch",
        "Build",
        "Is Down",
        "Status",
        "Collector Group Name",
    ]


def get_detailed_headers() -> List[str]:
    return [
        "ID",
        "Hostname",
        "Description",
        "Collector Size",
        "Arch",
        "Build",
        "Is Down",
        "Status",
        "Collector Group Name",
        "Number Of Instances",
        "CollectorEstSize",
        "Number Of Hosts",
        "Number Of SDTs",
        "Number Of Websites",
        "Enable FailBack",
        "In SDT",
        "Is Admin Account",
        "Java Init Memory",
        "Java Max Memory",
        "JavaSize",
        "Remote Session Disable",
        "Batchscript Threadpool",
        "ConfigCollector Script Threadpool",
        "SNMP Threadpool",
        "WMI Threadpool",
        "Proxy Enable",
        "Proxy Host",
        "Proxy Port",
        "Proxy User",
        "Proxy Password",
        "Script Cache Persistence",
        "Script Cache Switch To Secondary",
    ]


def show_all_collectors() -> None:
    """
    Fetch and display collector summary view.
    """
    print("\nFetching collectors...")
    collectors = get_all_collectors()

    if not collectors:
        print("No collectors found.")
        return

    headers = get_summary_headers()
    rows = build_summary_rows(collectors)
    display_table(rows, headers, "LogicMonitor Collectors")


def show_detailed_collectors(csv_filename: Optional[str] = None) -> None:
    """
    Fetch and display detailed collector view.
    Optionally write the detailed rows to a CSV file.
    """
    print("\nFetching collectors (detailed)...")
    collectors = get_all_collectors_detailed()

    if not collectors:
        print("No collectors found.")
        return

    headers = get_detailed_headers()
    rows = build_detailed_rows(collectors)

    display_table(rows, headers, "LogicMonitor Collectors - Detailed")

    if csv_filename:
        write_csv(csv_filename, headers, rows)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve LogicMonitor collectors via the LogicMonitor REST API."
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--show-all",
        action="store_true",
        help="Display collector summary in an ASCII table."
    )
    group.add_argument(
        "--detailed",
        action="store_true",
        help="Display detailed collector information in an ASCII table."
    )

    parser.add_argument(
        "--csv",
        help="Write detailed collector output to a CSV file. Use with --detailed."
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

    if not (args.show_all or args.detailed):
        parser.print_help()
        sys.exit(0)

    if args.csv and not args.detailed:
        print("Error: --csv can only be used with --detailed")
        sys.exit(1)

    return args


if __name__ == "__main__":
    args = parse_args()
    DEBUG = args.debug
    validate_env()

    if args.show_all:
        show_all_collectors()
    elif args.detailed:
        show_detailed_collectors(csv_filename=args.csv)

# eof