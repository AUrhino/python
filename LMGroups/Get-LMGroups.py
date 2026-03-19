#!/usr/bin/env python3
"""
LogicMonitor API - Get Device Groups
------------------------------------
This script retrieves LogicMonitor device groups and displays them in ASCII tables
or tree views.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage:
- Show help:
    python3 Get-LMGroups.py

- Show device group summary:
    python3 Get-LMGroups.py --show

- Show detailed device group view:
    python3 Get-LMGroups.py --show-detailed

- Show a single device group:
    python3 Get-LMGroups.py --show-single --id 26

- Show all device groups in ASCII tree format:
    python3 Get-LMGroups.py --tree

- Show all device groups in Unicode tree format:
    python3 Get-LMGroups.py --tree_with_unicode

- Show a subtree starting from a specific group ID:
    python3 Get-LMGroups.py --tree-from-id 26

- Enable debug output:
    python3 Get-LMGroups.py --show --debug
"""

import argparse
import base64
import hashlib
import hmac
import os
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

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
API_VERSION = "3"


def validate_env() -> None:
    """
    Validate required environment variables.
    """
    missing = [
        name
        for name, value in {
            "ACCESS_ID": ACCESS_ID,
            "ACCESS_KEY": ACCESS_KEY,
            "COMPANY": COMPANY,
        }.items()
        if not value
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

    LMv1 signatures are calculated using only the resource path, not query parameters.
    """
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb + epoch + data + resource_path
    hmac_hash = hmac.new(
        ACCESS_KEY.encode(),
        msg=request_vars.encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()
    signature = base64.b64encode(hmac_hash.encode()).decode()
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"

    headers = {
        "Authorization": auth,
        "Accept": "application/json",
        "X-Version": API_VERSION,
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


def do_get(resource_path: str, params: Optional[Dict[str, str]] = None) -> requests.Response:
    """
    Perform the raw GET request and return the response object.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    debug_print(f"API endpoint: GET {resource_path}")
    if params:
        debug_print(f"Query params: {params}")
    debug_print(f"Full URL: {url}")
    debug_headers(headers)

    response = requests.get(url, headers=headers, params=params, timeout=30)

    debug_print(f"HTTP status: {response.status_code} {response.reason}")
    if response.status_code != 200:
        debug_print(f"Response body: {response.text[:1000]}")

    return response


def api_get(resource_path: str, params: Optional[Dict[str, str]] = None) -> Dict:
    """
    Perform a GET request to the LogicMonitor API.
    """
    try:
        response = do_get(resource_path, params=params)
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
        print("\n" + "=" * 100)
        print(title)
        print("=" * 100)

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


def normalize_fields(fields: Optional[List[str]]) -> Optional[str]:
    """
    Join a list of field names into a comma-separated API value.
    """
    if not fields:
        return None
    return ",".join(fields)


def normalize_single_object(response: Dict) -> Dict:
    """
    Return the object payload from either a wrapped or direct LM response.
    """
    if not isinstance(response, dict):
        return {}

    data = response.get("data")
    if isinstance(data, dict):
        return data

    return response


def stringify_properties(properties: Optional[List[Dict]]) -> str:
    """
    Convert LogicMonitor name/value property objects into a compact string.
    """
    if not properties:
        return ""

    pairs = []
    for prop in properties:
        if not isinstance(prop, dict):
            continue
        name = prop.get("name", "")
        value = prop.get("value", "")
        pairs.append(f"{name}={value}")

    return "; ".join(pairs)


def build_group_list_params(
    fields: Optional[List[str]] = None,
    size: int = 1000,
    offset: int = 0,
    sort: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> Dict[str, str]:
    """
    Build query parameters for listing groups.
    """
    params: Dict[str, str] = {
        "size": str(size),
        "offset": str(offset),
    }

    field_str = normalize_fields(fields)
    if field_str:
        params["fields"] = field_str
    if sort:
        params["sort"] = sort
    if filter_expr:
        params["filter"] = filter_expr

    return params


def get_device_groups_page(
    fields: Optional[List[str]] = None,
    size: int = 1000,
    offset: int = 0,
    sort: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> Tuple[List[Dict], Optional[int]]:
    """
    Fetch one page of device groups.
    """
    resource_path = "/device/groups"
    params = build_group_list_params(
        fields=fields,
        size=size,
        offset=offset,
        sort=sort,
        filter_expr=filter_expr,
    )
    response = api_get(resource_path, params=params)

    items, total = extract_items_and_total(response)

    debug_print(f"Parsed items: {len(items)}")
    debug_print(f"Parsed total: {total}")

    return items, total


def get_all_device_groups(
    fields: Optional[List[str]] = None,
    filter_expr: Optional[str] = None,
    sort: Optional[str] = "+id",
    page_size: int = 1000,
) -> List[Dict]:
    """
    Fetch all device groups across paginated results.
    """
    all_groups: List[Dict] = []
    offset = 0
    total: Optional[int] = None

    while True:
        page_items, page_total = get_device_groups_page(
            fields=fields,
            size=page_size,
            offset=offset,
            sort=sort,
            filter_expr=filter_expr,
        )

        if not page_items:
            break

        all_groups.extend(page_items)

        if page_total is not None:
            total = page_total

        offset += len(page_items)

        if len(page_items) < page_size:
            break

        if total is not None and offset >= total:
            break

    all_groups.sort(key=lambda g: (g.get("id") is None, g.get("id")))
    return all_groups


def get_device_group_by_id(group_id: int, fields: Optional[List[str]] = None) -> Dict:
    """
    Fetch a single device group by its ID.
    """
    resource_path = f"/device/groups/{group_id}"
    params: Dict[str, str] = {}

    field_str = normalize_fields(fields)
    if field_str:
        params["fields"] = field_str

    response = api_get(resource_path, params=params)
    return normalize_single_object(response)


def build_summary_rows(groups: List[Dict]) -> List[List]:
    """
    Build summary table rows for device groups.
    """
    rows = []
    for group in groups:
        if not isinstance(group, dict):
            continue

        rows.append([
            group.get("id"),
            group.get("name"),
            group.get("description"),
            group.get("disableAlerting"),
            group.get("fullPath"),
            group.get("appliesTo"),
        ])
    return rows


def build_detailed_rows(groups: List[Dict]) -> List[List]:
    """
    Build detailed table rows for device groups.
    """
    rows = []
    for group in groups:
        if not isinstance(group, dict):
            continue

        rows.append([
            group.get("id"),
            group.get("name"),
            group.get("description"),
            group.get("disableAlerting"),
            group.get("fullPath"),
            group.get("appliesTo"),
            stringify_properties(group.get("customProperties")),
            group.get("parentId"),
            group.get("groupType"),
            group.get("numOfHosts"),
        ])
    return rows


def build_single_row(group: Dict) -> List[List]:
    """
    Build a single-row detailed view for one device group.
    """
    if not isinstance(group, dict):
        return []

    return [[
        group.get("id"),
        group.get("name"),
        group.get("description"),
        group.get("disableAlerting"),
        group.get("fullPath"),
        group.get("appliesTo"),
        stringify_properties(group.get("customProperties")),
        group.get("parentId"),
        group.get("groupType"),
        group.get("numOfHosts"),
    ]]


def show_groups() -> None:
    """
    Fetch and display device group summary view.
    """
    print("\nFetching device groups...")
    groups = get_all_device_groups(
        fields=["id", "name", "description", "disableAlerting", "fullPath", "appliesTo"]
    )

    if not groups:
        print("No device groups found.")
        return

    headers = [
        "ID",
        "Name",
        "Description",
        "Disable Alerting",
        "Full Path",
        "Applies To",
    ]
    rows = build_summary_rows(groups)
    display_table(rows, headers, "LogicMonitor Device Groups")


def show_detailed_groups() -> None:
    """
    Fetch and display detailed device group view.
    """
    print("\nFetching device groups (detailed)...")
    groups = get_all_device_groups(
        fields=[
            "id",
            "name",
            "description",
            "disableAlerting",
            "fullPath",
            "appliesTo",
            "customProperties",
            "parentId",
            "groupType",
            "numOfHosts",
        ]
    )

    if not groups:
        print("No device groups found.")
        return

    headers = [
        "ID",
        "Name",
        "Description",
        "Disable Alerting",
        "Full Path",
        "Applies To",
        "Custom Properties",
        "Parent ID",
        "Group Type",
        "Num Of Hosts",
    ]
    rows = build_detailed_rows(groups)
    display_table(rows, headers, "LogicMonitor Device Groups - Detailed")


def show_single_group(group_id: int) -> None:
    """
    Fetch and display one device group in detailed view.
    """
    print(f"\nFetching device group {group_id}...")
    group = get_device_group_by_id(
        group_id,
        fields=[
            "id",
            "name",
            "description",
            "disableAlerting",
            "fullPath",
            "appliesTo",
            "customProperties",
            "parentId",
            "groupType",
            "numOfHosts",
        ],
    )

    if not group:
        print(f"Device group {group_id} not found.")
        return

    headers = [
        "ID",
        "Name",
        "Description",
        "Disable Alerting",
        "Full Path",
        "Applies To",
        "Custom Properties",
        "Parent ID",
        "Group Type",
        "Num Of Hosts",
    ]
    rows = build_single_row(group)
    display_table(rows, headers, f"LogicMonitor Device Group - {group_id}")


def format_tree_label(group_id: Optional[int], name: Optional[str]) -> str:
    """
    Format one tree label as:
    26_Collector mapping
    """
    group_id_value = "" if group_id is None else str(group_id)
    name_value = "" if name is None else str(name)
    return f"{group_id_value}_{name_value}"


def get_tree_children(group: Dict) -> List[Dict]:
    """
    Return sorted subgroup objects for a group.
    """
    subgroups = group.get("subGroups") or []
    if not isinstance(subgroups, list):
        return []

    return sorted(
        [sg for sg in subgroups if isinstance(sg, dict)],
        key=lambda sg: ((sg.get("name") or "").lower(), sg.get("id") or 0),
    )


def fetch_group_for_tree(group_id: int, cache: Dict[int, Dict]) -> Dict:
    """
    Fetch one group with the fields needed for tree rendering.
    """
    if group_id not in cache:
        cache[group_id] = get_device_group_by_id(
            group_id,
            fields=["id", "name", "numOfDirectSubGroups", "subGroups"],
        )
    return cache[group_id]


def append_tree_lines(
    group_id: int,
    lines: List[str],
    visited: Set[int],
    cache: Dict[int, Dict],
    branch_style: str,
    prefix: str = "",
    is_last: bool = True,
    show_connector: bool = False,
) -> None:
    """
    Recursively collect tree lines using either ASCII or Unicode branch markers.
    """
    if group_id in visited:
        return

    visited.add(group_id)
    group = fetch_group_for_tree(group_id, cache)

    if not group:
        return

    label = format_tree_label(group.get("id"), group.get("name"))

    if branch_style == "unicode":
        last_connector = "└── "
        mid_connector = "├── "
        last_padding = "    "
        mid_padding = "│   "
    else:
        last_connector = r"\-- "
        mid_connector = "+-- "
        last_padding = "    "
        mid_padding = "|   "

    if show_connector:
        connector = last_connector if is_last else mid_connector
        lines.append(f"{prefix}{connector}{label}")
        child_prefix = f"{prefix}{last_padding if is_last else mid_padding}"
    else:
        lines.append(label)
        child_prefix = ""

    direct_subgroup_count = group.get("numOfDirectSubGroups") or 0
    if direct_subgroup_count == 0:
        return

    children = get_tree_children(group)
    if not children:
        return

    for index, child in enumerate(children):
        child_id = child.get("id")
        if child_id is None:
            continue

        append_tree_lines(
            group_id=child_id,
            lines=lines,
            visited=visited,
            cache=cache,
            branch_style=branch_style,
            prefix=child_prefix,
            is_last=(index == len(children) - 1),
            show_connector=True,
        )


def get_tree_start_groups() -> List[Dict]:
    """
    Determine the top-level device groups under the root group.
    """
    groups = get_all_device_groups(
        fields=["id", "parentId", "name"],
        sort="+id",
    )

    if not groups:
        return []

    top_level_groups = [g for g in groups if g.get("parentId") == 1]
    if top_level_groups:
        top_level_groups.sort(key=lambda g: ((g.get("name") or "").lower(), g.get("id") or 0))
        return top_level_groups

    groups.sort(
        key=lambda g: (
            g.get("parentId") if g.get("parentId") is not None else -1,
            (g.get("name") or "").lower(),
            g.get("id") or 0,
        )
    )
    return groups


def render_tree(start_group_ids: List[int], title: str, branch_style: str) -> None:
    """
    Render one or more root groups in either ASCII or Unicode tree format.
    """
    lines: List[str] = []
    visited: Set[int] = set()
    cache: Dict[int, Dict] = {}

    for root_id in start_group_ids:
        append_tree_lines(
            group_id=root_id,
            lines=lines,
            visited=visited,
            cache=cache,
            branch_style=branch_style,
            prefix="",
            is_last=True,
            show_connector=False,
        )

    if not lines:
        print("No device groups found.")
        return

    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)
    for line in lines:
        print(line)


def show_group_tree() -> None:
    """
    Display all device groups in an ASCII tree view.
    """
    print("\nFetching device group tree...")
    start_groups = get_tree_start_groups()

    if not start_groups:
        print("No device groups found.")
        return

    root_ids = [group.get("id") for group in start_groups if group.get("id") is not None]
    render_tree(root_ids, "LogicMonitor Device Group Tree", "ascii")


def show_group_tree_with_unicode() -> None:
    """
    Display all device groups in a Unicode tree view.
    """
    print("\nFetching device group tree (Unicode)...")
    start_groups = get_tree_start_groups()

    if not start_groups:
        print("No device groups found.")
        return

    root_ids = [group.get("id") for group in start_groups if group.get("id") is not None]
    render_tree(root_ids, "LogicMonitor Device Group Tree (Unicode)", "unicode")


def show_group_tree_from_id(group_id: int) -> None:
    """
    Display a subtree starting from a specific group ID in ASCII tree format.
    """
    print(f"\nFetching device group tree from ID {group_id}...")

    root_group = get_device_group_by_id(
        group_id,
        fields=["id", "name", "numOfDirectSubGroups", "subGroups"],
    )

    if not root_group or root_group.get("id") is None:
        print(f"Device group {group_id} not found.")
        return

    render_tree([group_id], f"LogicMonitor Device Group Tree From ID {group_id}", "ascii")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve LogicMonitor device groups via the LogicMonitor REST API."
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--show",
        action="store_true",
        help="Display device group summary in an ASCII table.",
    )
    group.add_argument(
        "--show-detailed",
        action="store_true",
        help="Display detailed device group information in an ASCII table.",
    )
    group.add_argument(
        "--show-single",
        action="store_true",
        help="Display one device group in a detailed ASCII table. Requires --id.",
    )
    group.add_argument(
        "--tree",
        action="store_true",
        help="Display all device groups in ASCII tree format using only id and name.",
    )
    group.add_argument(
        "--tree_with_unicode",
        action="store_true",
        help="Display all device groups in Unicode tree format using only id and name.",
    )
    group.add_argument(
        "--tree-from-id",
        type=int,
        metavar="GROUP_ID",
        help="Display an ASCII subtree from the specified device group ID using only id and name.",
    )

    parser.add_argument(
        "--id",
        type=int,
        help="Device group ID used with --show-single.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output and print API endpoints, headers, and HTTP codes.",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if not (
        args.show
        or args.show_detailed
        or args.show_single
        or args.tree
        or args.tree_with_unicode
        or args.tree_from_id is not None
    ):
        parser.print_help()
        sys.exit(0)

    if args.show_single and args.id is None:
        parser.error("--show-single requires --id")

    if args.id is not None and not args.show_single:
        parser.error("--id can only be used with --show-single")

    return args


if __name__ == "__main__":
    args = parse_args()
    DEBUG = args.debug
    validate_env()

    if args.show:
        show_groups()
    elif args.show_detailed:
        show_detailed_groups()
    elif args.show_single:
        show_single_group(args.id)
    elif args.tree:
        show_group_tree()
    elif args.tree_with_unicode:
        show_group_tree_with_unicode()
    elif args.tree_from_id is not None:
        show_group_tree_from_id(args.tree_from_id)

# eof
