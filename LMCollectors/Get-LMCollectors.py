"""
LogicMonitor API - Get Collectors
---------------------------------
This script retrieves LogicMonitor collectors and displays them in an ASCII table.

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

- Enable debug output:
    python3 Get-LMCollectors.py --show-all --debug
"""

import argparse
import base64
import hashlib
import hmac
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

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


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> Dict[str, str]:
    """
    Generate LogicMonitor API authentication headers.
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


def do_get(resource_path: str) -> requests.Response:
    """
    Perform the raw GET request and return the response object.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    debug_print(f"API endpoint: GET {resource_path}")
    debug_print(f"Full URL: {url}")
    debug_headers(headers)

    response = requests.get(url, headers=headers, timeout=30)

    debug_print(f"HTTP status: {response.status_code} {response.reason}")
    if response.status_code != 200:
        debug_print(f"Response body: {response.text[:1000]}")

    return response


def api_get(resource_path: str) -> Dict:
    """
    Perform a GET request to the LogicMonitor API.
    """
    try:
        response = do_get(resource_path)
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


def display_table(data: List[List], headers: List[str], title: str = "") -> None:
    """
    Display data in a formatted ASCII table.
    """
    if title:
        print("\n" + "=" * 80)
        print(title)
        print("=" * 80)

    print(tabulate(data, headers=headers, tablefmt="grid"))


def extract_items_and_total(response: Dict) -> Tuple[List[Dict], Optional[int]]:
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

    return [], None


def get_collectors_page(size: int = 1000, offset: int = 0) -> Tuple[List[Dict], Optional[int]]:
    """
    Fetch collectors.
    """
    resource_path = "/setting/collectors"
    response = api_get(resource_path)

    items, total = extract_items_and_total(response)

    debug_print(f"Parsed items: {len(items)}")
    debug_print(f"Parsed total: {total}")

    return items, total


def get_all_collectors(page_size: int = 1000) -> List[Dict]:
    """
    Fetch all collectors.
    """
    page_items, _ = get_collectors_page(size=page_size, offset=0)
    collectors = page_items if page_items else []
    collectors.sort(key=lambda c: (c.get("id") is None, c.get("id")))
    return collectors


def build_summary_rows(collectors: List[Dict]) -> List[List]:
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


def build_detailed_rows(collectors: List[Dict]) -> List[List]:
    """
    Build detailed table rows for collectors.
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
            collector.get("numberOfInstances"),
            collector.get("numberOfHosts"),
            collector.get("numberOfSDTs"),
            collector.get("numberOfWebsites"),
            collector.get("enableFailBack"),
            collector.get("inSDT"),
            collector.get("isAdminAccount"),
        ])
    return rows


def show_all_collectors() -> None:
    """
    Fetch and display collector summary view.
    """
    print("\nFetching collectors...")
    collectors = get_all_collectors()

    if not collectors:
        print("No collectors found.")
        return

    headers = [
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
    rows = build_summary_rows(collectors)
    display_table(rows, headers, "LogicMonitor Collectors")


def show_detailed_collectors() -> None:
    """
    Fetch and display detailed collector view.
    """
    print("\nFetching collectors (detailed)...")
    collectors = get_all_collectors()

    if not collectors:
        print("No collectors found.")
        return

    headers = [
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
        "Number Of Hosts",
        "Number Of SDTs",
        "Number Of Websites",
        "Enable FailBack",
        "In SDT",
        "Is Admin Account",
    ]
    rows = build_detailed_rows(collectors)
    display_table(rows, headers, "LogicMonitor Collectors - Detailed")


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

    return args


if __name__ == "__main__":
    args = parse_args()
    DEBUG = args.debug
    validate_env()

    if args.show_all:
        show_all_collectors()
    elif args.detailed:
        show_detailed_collectors()

# eof
