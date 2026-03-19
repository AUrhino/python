"""
LogicMonitor API - Get Roles
----------------------------
This script retrieves LogicMonitor roles, displays them in an ASCII table,
retrieves a specific role by ID, and can export all roles to individual JSON files.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage:
- Show help:
    python3 Get-LMRoles.py

- Show all roles in a table:
    python3 Get-LMRoles.py --show-all

- Get a specific role by ID:
    python3 Get-LMRoles.py --id 12

- Export all roles to individual files:
    python3 Get-LMRoles.py --extract-all

- Enable debug output:
    python3 Get-LMRoles.py --show-all --debug
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
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

    return {
        "Content-Type": "application/json",
        "Authorization": auth
    }


def api_get(resource_path: str) -> Dict:
    """
    Perform a GET request to the LogicMonitor API.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    debug_print(f"API endpoint: GET {resource_path}")
    debug_print(f"Full URL: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return {}

    debug_print(f"Response status: {response.status_code}")

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

    print(f"Error: {response.status_code} - {response.text}")
    return {}


def display_table(data: List[List], headers: List[str], title: str = "") -> None:
    """
    Display data in a formatted ASCII table.
    """
    if title:
        print("\n" + "=" * 60)
        print(title)
        print("=" * 60)

    print(tabulate(data, headers=headers, tablefmt="grid"))


def extract_items_and_total(response: Dict) -> Tuple[List[Dict], Optional[int]]:
    """
    Support both top-level and nested pagination response formats.
    """
    if not isinstance(response, dict):
        return [], None

    # Format 1: {"items": [...], "total": 123}
    if isinstance(response.get("items"), list):
        items = response.get("items", [])
        total = response.get("total")
        return items, total

    # Format 2: {"data": {"items": [...], "total": 123}}
    data = response.get("data", {})
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        items = data.get("items", [])
        total = data.get("total")
        return items, total

    return [], None


def get_roles_page(size: int = 1000, offset: int = 0) -> Tuple[List[Dict], Optional[int]]:
    """
    Fetch one page of roles.
    """
    resource_path = "/setting/roles"
    response = api_get(resource_path)

    items, total = extract_items_and_total(response)

    debug_print(f"Parsed items: {len(items)}")
    debug_print(f"Parsed total: {total}")

    return items, total


def get_all_roles(page_size: int = 1000) -> List[Dict]:
    """
    Fetch all roles.
    """
    page_items, _ = get_roles_page(size=page_size, offset=0)
    roles = page_items if page_items else []
    roles.sort(key=lambda r: (r.get("id") is None, r.get("id")))
    return roles


def get_role_by_id(role_id: int) -> Dict:
    """
    Fetch a single role by ID.
    """
    response = api_get(f"/setting/roles/{role_id}")

    # Support both top-level role object and nested {"data": {...}}
    if isinstance(response, dict):
        if "id" in response or "name" in response:
            return response

        data = response.get("data", {})
        if isinstance(data, dict) and data:
            return data

    print(f"No role data returned for ID {role_id}.")
    return {}


def build_role_table_rows(roles: List[Dict]) -> List[List]:
    """
    Build table rows for roles.
    """
    rows = []
    for role in roles:
        if not isinstance(role, dict):
            continue
        rows.append([
            role.get("id"),
            role.get("name"),
            role.get("description")
        ])
    return rows


def sanitize_filename(value: str) -> str:
    """
    Sanitize a string for use as a filename.
    """
    cleaned = re.sub(r"[^\w\-\. ]+", "_", value).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)

    if not cleaned:
        cleaned = "unnamed"

    return cleaned


def show_all_roles() -> None:
    """
    Fetch and display all roles in a table.
    """
    print("\nFetching all roles...")
    roles = get_all_roles()

    if not roles:
        print("No roles found.")
        return

    rows = build_role_table_rows(roles)
    headers = ["ID", "Name", "Description"]
    display_table(rows, headers, "LogicMonitor Roles")


def show_role_by_id(role_id: int) -> None:
    """
    Fetch and display one role by ID.
    """
    print(f"\nFetching role ID: {role_id}")
    role = get_role_by_id(role_id)

    if not role:
        return

    summary_rows = [[
        role.get("id"),
        role.get("name"),
        role.get("description")
    ]]
    display_table(summary_rows, ["ID", "Name", "Description"], f"LogicMonitor Role {role_id}")

    print("\nRaw JSON:")
    print(json.dumps(role, indent=4))


def extract_all_roles() -> None:
    """
    Fetch all roles, then fetch each role by ID and write it to output/<name>.json.
    """
    print("\nFetching all roles for export...")
    roles = get_all_roles()

    if not roles:
        print("No roles found.")
        return

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    exported = []
    failed = []

    for role in roles:
        if not isinstance(role, dict):
            continue

        role_id = role.get("id")
        if role_id is None:
            failed.append(["<missing>", "Unknown", "Missing role ID"])
            continue

        role_detail = get_role_by_id(role_id)
        if not role_detail:
            failed.append([role_id, role.get("name"), "Failed to fetch role detail"])
            continue

        role_name = role_detail.get("name") or f"id_{role_id}"
        safe_name = sanitize_filename(role_name)
        filename = f"{safe_name}.json"
        filepath = os.path.join(output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as file:
                json.dump(role_detail, file, indent=4, ensure_ascii=False)
            exported.append([role_id, role_name, filepath])
        except OSError as exc:
            failed.append([role_id, role_name, str(exc)])

    if exported:
        display_table(exported, ["ID", "Name", "File"], "Exported Role Files")

    if failed:
        display_table(failed, ["ID", "Name", "Error"], "Failed Exports")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve and export LogicMonitor roles via the LogicMonitor REST API."
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--show-all",
        action="store_true",
        help="Display all roles in an ASCII table."
    )
    group.add_argument(
        "--id",
        type=int,
        help="Retrieve a specific role by ID."
    )
    group.add_argument(
        "--extract-all",
        action="store_true",
        help="Export every role to output/<name>.json."
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output and print API endpoints being called."
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if not (args.show_all or args.id is not None or args.extract_all):
        parser.print_help()
        sys.exit(0)

    return args


if __name__ == "__main__":
    args = parse_args()
    DEBUG = args.debug
    validate_env()

    if args.show_all:
        show_all_roles()
    elif args.id is not None:
        show_role_by_id(args.id)
    elif args.extract_all:
        extract_all_roles()

# eof
