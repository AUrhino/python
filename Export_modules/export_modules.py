"""
LogicMonitor API - Export LogicModules (Modules)
------------------------------------------------
Exports LogicMonitor "modules" (LogicModules) to JSON files, by module type.

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

Create a creds file called .env
.env file:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Usage examples:
  python export_modules.py --types datasources eventsources --out output
  python export_modules.py --types all --out output --size 200 --sleep 0.2
  python export_modules.py --types datasources --filter 'name~"CPU"' --out output

Notes:
- Adds retry (3 attempts) for transient errors and continues on module-type failure.
- If HTTP 429 (rate limited): sleeps 30 seconds (or honors Retry-After) then retries.
- Writes one JSON file per module item, plus an index file per module type.

Created by Ryan Gillan
ryan.gillan@logicmonitor.com

"""

import os
import re
import time
import hmac
import json
import base64
import hashlib
import argparse
import random
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
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": auth,
    }


# Retry on these (excluding 429; handled explicitly for 30s sleep)
RETRY_STATUS_CODES = {406, 408, 500, 502, 503, 504}


def api_get(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff_base: float = 1.0,
    rate_limit_sleep_s: int = 30,
) -> Dict[str, Any]:
    """
    Perform a GET request to the LogicMonitor API with retry handling.
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
                return resp.json()

            # Auth / permission issues: fail fast
            if resp.status_code in (401, 403):
                msg = resp.text.strip() or "<empty body>"
                raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {msg}")

            # Rate limit: sleep 30s (or Retry-After) then retry
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

            # Retry on transient-ish statuses
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

            # Other status codes: fail fast
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
    return {}


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
    """
    data = payload.get("data") or {}
    items = data.get("items") or []
    meta = {
        "total": data.get("total"),
        "searchId": data.get("searchId"),
        "filteredCount": (payload.get("meta") or {}).get("filteredCount"),
    }
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

        total = meta.get("total")

        # Optional pacing between pages (separate from 429 handling)
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
    Export one module type and continue on error.

    Writes:
      - <out_dir>/<module_key>/index.json
      - <out_dir>/<module_key>/<id>__<name>.json
      - <out_dir>/<module_key>/_error.txt (if failed)
    """
    if module_key not in MODULE_ENDPOINTS:
        raise ValueError(f"Unknown module type: {module_key}")

    resource_path = MODULE_ENDPOINTS[module_key]
    module_dir = os.path.join(out_dir, module_key)
    ensure_dir(module_dir)

    print(f"\n== Exporting {module_key} from {resource_path} ==")

    try:
        items = list_all_items(
            resource_path=resource_path,
            size=size,
            sleep_s=sleep_s,
            fields=fields,
            filter_expr=filter_expr,
        )
    except Exception as e:
        err_path = os.path.join(module_dir, "_error.txt")
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(str(e) + "\n")
        print(f"ERROR exporting {module_key}: {e}")
        print(f"Continuing. Details saved to: {err_path}")
        return

    index_path = os.path.join(module_dir, "index.json")
    write_json(index_path, items)
    print(f"Saved {len(items)} items -> {index_path}")

    for item in items:
        item_id = item.get("id")
        name = item.get("name") or item.get("displayName") or "unnamed"
        if item_id is not None:
            fname = f"{item_id}__{safe_filename(str(name))}.json"
        else:
            fname = f"{safe_filename(str(name))}.json"
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
