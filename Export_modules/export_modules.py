"""
LogicMonitor API - Export LogicModules (Modules)
------------------------------------------------
Exports LogicMonitor "modules" (LM Modules) to JSON files, by module type.

Covered module types (API v3 endpoints):
- DataSources        : /setting/datasources
- EventSources       : /setting/eventsources
- LogSources         : /setting/logsources
- ConfigSources      : /setting/configsources
- PropertySources    : /setting/propertyrules
- TopologySources    : /setting/topologysources
- JobMonitors        : /setting/batchjobs
- AppliesToFunctions : /setting/functions
- OIDs (SNMP SysOID) : /setting/oids

Requirements:
- Python 3.8+
- requests, python-dotenv

Create a file next to this python code called .env  
The content of .env file:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage examples:
  python export_modules.py --types datasources eventsources --out output
  python export_modules.py --types all --out output --size 200 --sleep 0.2
  python export_modules.py --types datasources --filter "name~\"CPU\"" --out output

Notes:
- Uses offset/size pagination where supported.
- Writes one JSON file per module item, plus an index file per module type.
"""

import os
import re
import time
import hmac
import json
import base64
import hashlib
import argparse
from typing import Any, Dict, List, Optional, Tuple

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
    return {"Content-Type": "application/json", "Authorization": auth}


def api_get(resource_path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Perform a GET request to the LogicMonitor API.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)
    resp = requests.get(url, headers=headers, params=params, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {resp.text}")

    return resp.json()


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


def extract_items(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Extract items + meta from LM standard response.
    Many endpoints return { "data": { "items": [...], "total": n, ... }, "status": ..., ... }
    """
    data = payload.get("data") or {}
    items = data.get("items") or []
    meta = {
        "total": data.get("total"),
        "searchId": data.get("searchId"),
        "filteredCount": (payload.get("meta") or {}).get("filteredCount"),
    }
    # items might sometimes not be list; normalize
    if not isinstance(items, list):
        items = []
    return items, meta


def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


# -----------------------------
# Module Exporter
# -----------------------------
MODULE_ENDPOINTS = {
    # logical name      # endpoint path
    "datasources": "/setting/datasources",
    "eventsources": "/setting/eventsources",
    "logsources": "/setting/logsources",
    "configsources": "/setting/configsources",
    "propertysources": "/setting/propertyrules",
    "topologysources": "/setting/topologysources",
    "jobmonitors": "/setting/batchjobs",
    "appliestofunctions": "/setting/functions",
    "oids": "/setting/oids",
}

# Some endpoints accept format=json (per swagger); harmless to send everywhere.
DEFAULT_FORMAT = "json"


def list_all_items(
    resource_path: str,
    size: int = 200,
    sleep_s: float = 0.0,
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all items for a list endpoint with offset/size paging.
    If an endpoint ignores size/offset, you'll typically get everything at once.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"format": DEFAULT_FORMAT, "size": size, "offset": offset}
        if fields:
            params["fields"] = fields
        if filter_expr:
            params["filter"] = filter_expr

        payload = api_get(resource_path, params=params)
        items, meta = extract_items(payload)
        all_items.extend([i for i in items if isinstance(i, dict)])

        # Stop conditions:
        # - If server returns fewer than requested, likely end of page.
        # - If total is provided and we reached it.
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


def export_module_type(
    module_key: str,
    out_dir: str,
    size: int,
    sleep_s: float,
    fields: Optional[str],
    filter_expr: Optional[str],
) -> None:
    """
    Export one module type:
    - Writes /<out_dir>/<module_key>/index.json (full list)
    - Writes /<out_dir>/<module_key>/<id>__<name>.json per item
    """
    if module_key not in MODULE_ENDPOINTS:
        raise ValueError(f"Unknown module type: {module_key}")

    resource_path = MODULE_ENDPOINTS[module_key]
    module_dir = os.path.join(out_dir, module_key)
    ensure_dir(module_dir)

    print(f"\n== Exporting {module_key} from {resource_path} ==")
    items = list_all_items(
        resource_path=resource_path,
        size=size,
        sleep_s=sleep_s,
        fields=fields,
        filter_expr=filter_expr,
    )

    # Save index
    index_path = os.path.join(module_dir, "index.json")
    write_json(index_path, items)
    print(f"Saved {len(items)} items -> {index_path}")

    # Save individual items
    for item in items:
        item_id = item.get("id")
        name = item.get("name") or item.get("displayName") or "unnamed"
        fname = f"{item_id}__{safe_filename(str(name))}.json" if item_id is not None else f"{safe_filename(str(name))}.json"
        write_json(os.path.join(module_dir, fname), item)

    print(f"Wrote per-item files -> {module_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export LogicMonitor Modules (LogicModules) to JSON.")
    parser.add_argument(
        "--types",
        nargs="+",
        default=["all"],
        help=(
            "Module types to export. Use one or more of: "
            + ", ".join(MODULE_ENDPOINTS.keys())
            + " or 'all'."
        ),
    )
    parser.add_argument("--out", default="output_modules", help="Output directory (default: output_modules).")
    parser.add_argument("--size", type=int, default=200, help="Page size for list endpoints (default: 200).")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between pages (default: 0).")
    parser.add_argument(
        "--fields",
        default=None,
        help="Optional fields query param (comma-separated). Note: some endpoints ignore this.",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Optional LM filter expression (string). Example: 'name~\"CPU\"'.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dir(args.out)

    requested = [t.lower() for t in args.types]
    if "all" in requested:
        requested = list(MODULE_ENDPOINTS.keys())

    for module_key in requested:
        export_module_type(
            module_key=module_key,
            out_dir=args.out,
            size=args.size,
            sleep_s=args.sleep,
            fields=args.fields,
            filter_expr=args.filter,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
