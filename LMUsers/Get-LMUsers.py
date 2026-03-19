"""
LogicMonitor API - Get Users
----------------------------
This script retrieves LogicMonitor users, displays them in an ASCII table,
retrieves a specific user by ID, and can export all users to individual JSON files.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage:
- Show help:
    python3 Get-LMUsers.py

- Show all users in a table:
    python3 Get-LMUsers.py --show-all

- Get a specific user by ID:
    python3 Get-LMUsers.py --id 12

- Export all users to individual files:
    python3 Get-LMUsers.py --extract-all

- Enable debug output:
    python3 Get-LMUsers.py --show-all --debug
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


def get_users_page(size: int = 1000, offset: int = 0) -> Tuple[List[Dict], Optional[int]]:
    """
    Fetch one page of users.
    """
    resource_path = "/setting/admins"
    response = api_get(resource_path)

    items, total = extract_items_and_total(response)

    debug_print(f"Parsed items: {len(items)}")
    debug_print(f"Parsed total: {total}")

    return items, total


def get_all_users(page_size: int = 1000) -> List[Dict]:
    """
    Fetch all users.
    """
    page_items, _ = get_users_page(size=page_size, offset=0)
    users = page_items if page_items else []
    users.sort(key=lambda u: (u.get("id") is None, u.get("id")))
    return users


def get_user_by_id(user_id: int) -> Dict:
    """
    Fetch a single user by ID.
    """
    response = api_get(f"/setting/admins/{user_id}")

    if isinstance(response, dict):
        if "id" in response or "username" in response:
            return response

        data = response.get("data", {})
        if isinstance(data, dict) and data:
            return data

    print(f"No user data returned for ID {user_id}.")
    return {}


def build_user_table_rows(users: List[Dict]) -> List[List]:
    """
    Build table rows for users.
    """
    rows = []
    for user in users:
        if not isinstance(user, dict):
            continue
        rows.append([
            user.get("id"),
            user.get("username"),
            user.get("email"),
            user.get("status"),
            user.get("note")
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


def format_value(value) -> str:
    """
    Format a value for table display.
    """
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, ensure_ascii=False)
    return str(value)


def show_all_users() -> None:
    """
    Fetch and display all users in a table.
    """
    print("\nFetching all users...")
    users = get_all_users()

    if not users:
        print("No users found.")
        return

    rows = build_user_table_rows(users)
    headers = ["ID", "Username", "Email", "Status", "Note"]
    display_table(rows, headers, "LogicMonitor Users")


def show_user_by_id(user_id: int) -> None:
    """
    Fetch and display one user by ID with all fields.
    """
    print(f"\nFetching user ID: {user_id}")
    user = get_user_by_id(user_id)

    if not user:
        return

    rows = []
    for key in sorted(user.keys()):
        rows.append([key, format_value(user.get(key))])

    display_table(rows, ["Field", "Value"], f"LogicMonitor User {user_id}")


def extract_all_users() -> None:
    """
    Fetch all users, then fetch each user by ID and write it to output/<username>.json.
    """
    print("\nFetching all users for export...")
    users = get_all_users()

    if not users:
        print("No users found.")
        return

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    exported = []
    failed = []
    used_filenames = set()

    for user in users:
        if not isinstance(user, dict):
            continue

        user_id = user.get("id")
        if user_id is None:
            failed.append(["<missing>", "Unknown", "Missing user ID"])
            continue

        user_detail = get_user_by_id(user_id)
        if not user_detail:
            failed.append([user_id, user.get("username"), "Failed to fetch user detail"])
            continue

        username = user_detail.get("username") or f"id_{user_id}"
        safe_name = sanitize_filename(username)
        filename = f"{safe_name}.json"

        if filename in used_filenames:
            filename = f"{safe_name}_{user_id}.json"

        used_filenames.add(filename)
        filepath = os.path.join(output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as file:
                json.dump(user_detail, file, indent=4, ensure_ascii=False)
            exported.append([user_id, username, filepath])
        except OSError as exc:
            failed.append([user_id, username, str(exc)])

    if exported:
        display_table(exported, ["ID", "Username", "File"], "Exported User Files")

    if failed:
        display_table(failed, ["ID", "Username", "Error"], "Failed Exports")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Retrieve and export LogicMonitor users via the LogicMonitor REST API."
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--show-all",
        action="store_true",
        help="Display all users in an ASCII table."
    )
    group.add_argument(
        "--id",
        type=int,
        help="Retrieve a specific user by ID and display all fields."
    )
    group.add_argument(
        "--extract-all",
        action="store_true",
        help="Export every user to output/<username>.json."
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
        show_all_users()
    elif args.id is not None:
        show_user_by_id(args.id)
    elif args.extract_all:
        extract_all_users()

# eof
