"""
LogicMonitor API - Export Dashboards
------------------------------------
Exports LogicMonitor dashboards to JSON files.

Filename:
- export_LMdashboards.py

Export endpoint used for each dashboard:
- /dashboard/dashboards/{id}?format=file&template=true

List endpoint used:
- /dashboard/dashboards

Requirements:
- Python 3.8+
- requests
- python-dotenv
- tabulate (required for --list-all)

Create a creds file called .env
.env file:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage examples:
  python export_LMdashboards.py --help
  python export_LMdashboards.py --out output_dashboards
  python export_LMdashboards.py --out output_dashboards --size 200 --sleep 0.2
  python export_LMdashboards.py --filter 'name~"NOC"' --out output_dashboards
  python export_LMdashboards.py --list-all
  python export_LMdashboards.py --list-all --filter 'name~"Prod"'

Install dependencies:
  pip install requests python-dotenv tabulate

Notes:
- If run with no options, prints --help and exits.
- Adds retry (3 attempts) for transient errors and continues on per-dashboard failure.
- If HTTP 429 (rate limited): sleeps 30 seconds (or honors Retry-After) then retries.
- Each dashboard export uses:
    /dashboard/dashboards/{id}?format=file&template=true
- Writes:
    - <out>/dashboards/index.json
    - <out>/dashboards/<name>.json
    - <out>/dashboards/_errors.json
- With --list-all:
    - prints dashboard list to stdout in an ASCII grid
    - does not create any files
    - skips per-dashboard export
- While exporting:
    - prints: Working on: ID,Name
"""

import os
import re
import sys
import time
import hmac
import json
import base64
import hashlib
import argparse
import random
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv


# -----------------------------
# Config / Auth (LMv1)
# -----------------------------
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY", "")
ACCESS_ID = os.getenv("ACCESS_ID", "")
COMPANY = os.getenv("COMPANY", "")

if not (ACCESS_KEY and ACCESS_ID and COMPANY):
    raise SystemExit(
        "Missing ACCESS_KEY / ACCESS_ID / COMPANY. Please set them in your environment or .env file."
    )

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> Dict[str, str]:
    """
    Generate LogicMonitor API authentication headers (LMv1).
    """
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb + epoch + data + resource_path
    hmac_hash = hmac.new(
        ACCESS_KEY.encode("utf-8"),
        msg=request_vars.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    signature = base64.b64encode(hmac_hash.encode("utf-8")).decode("utf-8")
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": auth,
    }


RETRY_STATUS_CODES = {406, 408, 500, 502, 503, 504}


def api_get_raw(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff_base: float = 1.0,
    rate_limit_sleep_s: int = 30,
) -> str:
    """
    Perform a GET request to the LogicMonitor API with retry handling.
    Returns raw response text.

    - If 429 (rate limited): sleep 30s (or Retry-After) then retry.
    - Retries on transient HTTP status codes and network errors.
    """
    url = BASE_URL + resource_path
    params = params or {}

    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            headers = generate_auth_headers("GET", resource_path)
            resp = requests.get(url, headers=headers, params=params, timeout=60)

            if resp.status_code == 200:
                return resp.text

            if resp.status_code in (401, 403):
                msg = resp.text.strip() or "<empty body>"
                raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {msg}")

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = rate_limit_sleep_s
                if retry_after:
                    try:
                        sleep_s = int(float(retry_after))
                    except ValueError:
                        pass

                msg = resp.text.strip() or "<empty body>"
                print(
                    f"Rate limited: GET {resource_path} returned 429 (attempt {attempt}/{retries}). "
                    f"Sleeping {sleep_s}s. Body: {msg}"
                )

                if attempt < retries:
                    time.sleep(sleep_s)
                    continue

                raise RuntimeError(f"GET {resource_path} failed after {retries} attempts: 429 - {msg}")

            if resp.status_code in RETRY_STATUS_CODES:
                msg = resp.text.strip() or "<empty body>"
                print(
                    f"Warning: GET {resource_path} returned {resp.status_code} "
                    f"(attempt {attempt}/{retries}). Body: {msg}"
                )
                if attempt < retries:
                    sleep_s = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    time.sleep(sleep_s)
                    continue
                raise RuntimeError(
                    f"GET {resource_path} failed after {retries} attempts: {resp.status_code} - {msg}"
                )

            msg = resp.text.strip() or "<empty body>"
            raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {msg}")

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            last_err = e
            print(f"Warning: network error on GET {resource_path} (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                sleep_s = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                time.sleep(sleep_s)
                continue
            raise RuntimeError(
                f"GET {resource_path} failed after {retries} attempts due to network errors: {e}"
            )

    if last_err:
        raise last_err
    return ""


def api_get_json(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff_base: float = 1.0,
    rate_limit_sleep_s: int = 30,
) -> Dict[str, Any]:
    """
    Perform a GET request and parse the response as JSON.
    """
    raw = api_get_raw(
        resource_path=resource_path,
        params=params,
        retries=retries,
        backoff_base=backoff_base,
        rate_limit_sleep_s=rate_limit_sleep_s,
    )

    try:
        return json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GET {resource_path} returned non-JSON content: {e}")


# -----------------------------
# Helpers
# -----------------------------
def safe_filename(name: str, max_len: int = 160) -> str:
    """
    Make a filesystem-safe filename fragment.
    """
    if not name:
        return "unnamed"
    name = name.strip()
    name = re.sub(r"[^\w\-.()@\[\] ]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def extract_items(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Extract items + meta from either:
    - LM standard wrapped response: {"data": {"items": [...], "total": ...}, "meta": {...}}
    - direct pagination response   : {"items": [...], "total": ...}
    """
    if not isinstance(payload, dict):
        return [], {}

    data = payload.get("data")
    if isinstance(data, dict) and "items" in data:
        items = data.get("items") or []
        meta = {
            "total": data.get("total"),
            "searchId": data.get("searchId"),
            "filteredCount": (payload.get("meta") or {}).get("filteredCount"),
        }
    else:
        items = payload.get("items") or []
        meta = {
            "total": payload.get("total"),
            "searchId": payload.get("searchId"),
            "filteredCount": (payload.get("meta") or {}).get("filteredCount"),
        }

    if not isinstance(items, list):
        items = []

    items = [i for i in items if isinstance(i, dict)]
    return items, meta


def build_dashboard_filename(
    name: Optional[str],
    dashboard_id: Optional[Any],
    used_names: Set[str],
) -> str:
    """
    Build a filename based on the dashboard name field.
    - Primary: <name>.json
    - Missing name: unnamed_<id>.json
    - Duplicate names: <name>__<id>.json
    """
    safe_name = safe_filename(name or "")

    if not name or safe_name == "unnamed":
        base = f"unnamed_{dashboard_id}" if dashboard_id is not None else "unnamed"
    else:
        base = safe_name

    filename = f"{base}.json"
    lowered = filename.lower()

    if lowered in used_names and dashboard_id is not None:
        filename = f"{base}__{dashboard_id}.json"
        lowered = filename.lower()

    counter = 2
    original_base = base
    while lowered in used_names:
        filename = f"{original_base}__{counter}.json"
        lowered = filename.lower()
        counter += 1

    used_names.add(lowered)
    return filename


# -----------------------------
# Dashboard Exporter
# -----------------------------
DASHBOARD_LIST_ENDPOINT = "/dashboard/dashboards"
DASHBOARD_EXPORT_ENDPOINT = "/dashboard/dashboards/{id}"


def list_all_dashboards(
    size: int = 200,
    sleep_s: float = 0.0,
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all dashboards using offset/size paging.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"size": size, "offset": offset}
        if fields:
            params["fields"] = fields
        if filter_expr:
            params["filter"] = filter_expr

        payload = api_get_json(DASHBOARD_LIST_ENDPOINT, params=params)
        items, meta = extract_items(payload)
        all_items.extend(items)

        total = meta.get("total")

        if items and sleep_s > 0:
            time.sleep(sleep_s)

        if not items:
            break

        if isinstance(total, int) and total > 0 and len(all_items) >= total:
            break

        if len(items) < size:
            break

        offset += size

    return all_items


def get_dashboard_export_file(dashboard_id: int) -> str:
    """
    Export a dashboard using:
      /dashboard/dashboards/{id}?format=file&template=true
    """
    resource_path = DASHBOARD_EXPORT_ENDPOINT.format(id=dashboard_id)
    params = {
        "format": "file",
        "template": "true",
    }
    return api_get_raw(resource_path, params=params)


def print_dashboard_list(dashboards: List[Dict[str, Any]]) -> None:
    """
    Print dashboards in an ASCII grid using tabulate.
    """
    try:
        from tabulate import tabulate
    except ImportError:
        raise SystemExit(
            "The 'tabulate' module is required for --list-all output.\n"
            "Install it with: pip install tabulate"
        )

    print("\n== Dashboard List ==")
    if not dashboards:
        print("No dashboards found.")
        return

    rows = []
    for item in dashboards:
        dashboard_id = item.get("id", "")
        name = item.get("name") or item.get("fullName") or "unnamed"
        group_name = item.get("groupName") or item.get("groupFullPath") or ""
        description = item.get("description") or ""

        rows.append([dashboard_id, name, group_name, description])

    print(
        tabulate(
            rows,
            headers=["ID", "Name", "Group", "Description"],
            tablefmt="grid",
        )
    )


def export_dashboards(
    out_dir: str,
    size: int,
    sleep_s: float,
    list_fields: Optional[str],
    filter_expr: Optional[str],
    list_all_only: bool,
) -> None:
    """
    Export dashboards and continue on per-dashboard failures.

    Writes:
      - <out_dir>/dashboards/index.json
      - <out_dir>/dashboards/<name>.json
      - <out_dir>/dashboards/_errors.json

    If list_all_only is True:
      - print all dashboards
      - create no files
      - skip per-dashboard export
    """
    print(f"\n== Fetching dashboards from {DASHBOARD_LIST_ENDPOINT} ==")

    try:
        dashboards = list_all_dashboards(
            size=size,
            sleep_s=sleep_s,
            fields=list_fields,
            filter_expr=filter_expr,
        )
    except Exception as e:
        if list_all_only:
            print(f"ERROR fetching dashboards: {e}")
            return

        dashboard_dir = os.path.join(out_dir, "dashboards")
        ensure_dir(dashboard_dir)
        err_path = os.path.join(dashboard_dir, "_errors.json")
        write_json(err_path, [{"stage": "list", "error": str(e)}])
        print(f"ERROR fetching dashboards: {e}")
        print(f"Details saved to: {err_path}")
        return

    if list_all_only:
        print_dashboard_list(dashboards)
        print("\nList-only mode enabled. No files created.")
        return

    dashboard_dir = os.path.join(out_dir, "dashboards")
    ensure_dir(dashboard_dir)

    index_path = os.path.join(dashboard_dir, "index.json")
    write_json(index_path, dashboards)
    print(f"Saved {len(dashboards)} dashboard list items -> {index_path}")

    errors: List[Dict[str, Any]] = []
    used_filenames: Set[str] = set()

    for item in dashboards:
        dashboard_id = item.get("id")
        name = item.get("name") or "unnamed"

        if dashboard_id is None:
            errors.append(
                {
                    "dashboardId": None,
                    "dashboardName": name,
                    "error": "List item missing id; skipped export.",
                }
            )
            continue

        try:
            print(f"Working on: {dashboard_id},{name}", flush=True)

            export_text = get_dashboard_export_file(int(dashboard_id))

            out_name = build_dashboard_filename(
                name=name,
                dashboard_id=dashboard_id,
                used_names=used_filenames,
            )
            write_text(os.path.join(dashboard_dir, out_name), export_text)

        except Exception as e:
            print(f"ERROR exporting dashboard id={dashboard_id} name={name}: {e}")
            errors.append(
                {
                    "dashboardId": dashboard_id,
                    "dashboardName": name,
                    "error": str(e),
                }
            )

        if sleep_s > 0:
            time.sleep(sleep_s)

    if errors:
        err_path = os.path.join(dashboard_dir, "_errors.json")
        write_json(err_path, errors)
        print(f"Saved {len(errors)} export errors -> {err_path}")

    print(f"Wrote dashboard export files -> {dashboard_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export LogicMonitor Dashboards to JSON.")
    parser.add_argument("--out", default="output_dashboards", help="Output directory (default: output_dashboards).")
    parser.add_argument("--size", type=int, default=200, help="Page size for list endpoint (default: 200).")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between requests (default: 0).")
    parser.add_argument(
        "--list-fields",
        default=None,
        help="Optional fields query param for dashboard list (comma-separated).",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help='Optional LM filter expression. Example: \'name~"NOC"\'.',
    )
    parser.add_argument(
        "--list-all",
        action="store_true",
        help="List all dashboards and print to screen only. No files are created.",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        raise SystemExit(0)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    export_dashboards(
        out_dir=args.out,
        size=args.size,
        sleep_s=args.sleep,
        list_fields=args.list_fields,
        filter_expr=args.filter,
        list_all_only=args.list_all,
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
