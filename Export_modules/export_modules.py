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
  python export_modules.py --types propertysources --out output --debug

Notes:
- Running without arguments prints help and exits.
- Adds retry (3 attempts) for transient errors and continues on module-type failure.
- If HTTP 429 (rate limited): sleeps 30 seconds, or honors Retry-After, then retries.
- Writes one JSON file per module item.
- Does not write index.json.
- Per-item files are named without the LogicModule ID.
- If duplicate module names exist in the same module type, files are safely numbered.
- PropertySources use the /setting/propertyrules endpoint.
- Adds sort=+id to list requests.
- Use --debug to print request URLs and response status codes.

Created by Ryan Gillan
ryangillan@gmail.com
Ver 1.6
"""

import os
import sys
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
ACCESS_KEY = ""
ACCESS_ID = ""
COMPANY = ""
BASE_URL = ""


def load_lm_config() -> None:
    """
    Load LogicMonitor API credentials after CLI args are parsed.

    This allows --help, and running with no arguments, to display help
    without requiring a valid .env file.
    """
    global ACCESS_KEY, ACCESS_ID, COMPANY, BASE_URL

    load_dotenv()

    ACCESS_KEY = os.getenv("ACCESS_KEY", "")
    ACCESS_ID = os.getenv("ACCESS_ID", "")
    COMPANY = os.getenv("COMPANY", "")

    if not (ACCESS_KEY and ACCESS_ID and COMPANY):
        raise SystemExit(
            "Missing ACCESS_KEY / ACCESS_ID / COMPANY. "
            "Please set them in your environment or .env file."
        )

    BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> Dict[str, str]:
    """
    Generate LogicMonitor API authentication headers (LMv1).

    The LMv1 signature uses the resource path without query parameters.
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


# Retry on these status codes.
# HTTP 429 is handled separately because it needs rate-limit-aware sleep handling.
RETRY_STATUS_CODES = {406, 408, 500, 502, 503, 504}


def build_debug_url(url: str, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Build the final encoded request URL for debug output.

    Does not include headers or credentials.
    """
    try:
        prepared = requests.Request("GET", url, params=params or {}).prepare()
        return prepared.url or url
    except Exception:
        return url


def debug_print_request(resource_path: str, url: str, params: Dict[str, Any], attempt: int, retries: int) -> None:
    """
    Print the request URL for debugging.

    Authorization headers are intentionally not printed.
    """
    debug_url = build_debug_url(url, params)
    print(f"DEBUG request attempt {attempt}/{retries}: GET {debug_url}")
    print(f"DEBUG resource path used for LMv1 signature: {resource_path}")


def debug_print_response(resp: requests.Response) -> None:
    """
    Print response status details for debugging.

    For non-200 responses, prints a short body preview.
    """
    print(f"DEBUG response: HTTP {resp.status_code} {resp.reason}")

    if resp.status_code != 200:
        body = resp.text.strip() or "<empty body>"
        if len(body) > 1000:
            body = body[:1000] + "... <truncated>"
        print(f"DEBUG response body: {body}")


def api_get(
    resource_path: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    backoff_base: float = 1.0,
    rate_limit_sleep_s: int = 30,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Perform a GET request to the LogicMonitor API with retry handling.

    - If 429 rate limited: sleep 30s, or Retry-After, then retry.
    - Retries on transient HTTP status codes and network errors.
    - Fails fast on auth and permission errors.
    - If debug=True, prints the fully encoded request URL and response status.
    """
    url = BASE_URL + resource_path
    params = params or {}

    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            headers = generate_auth_headers("GET", resource_path)

            if debug:
                debug_print_request(resource_path, url, params, attempt, retries)

            resp = requests.get(url, headers=headers, params=params, timeout=60)

            if debug:
                debug_print_response(resp)

            if resp.status_code == 200:
                return resp.json()

            # Auth / permission issues: fail fast.
            if resp.status_code in (401, 403):
                msg = resp.text.strip() or "<empty body>"
                raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {msg}")

            # Rate limit: sleep 30s, or Retry-After, then retry.
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
                    f"Rate limited: GET {resource_path} returned 429 "
                    f"(attempt {attempt}/{retries}). Sleeping {sleep_s}s. Body: {msg}"
                )

                if attempt < retries:
                    time.sleep(sleep_s)
                    continue

                raise RuntimeError(
                    f"GET {resource_path} failed after {retries} attempts: 429 - {msg}"
                )

            # Retry on transient-ish statuses.
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
                    f"GET {resource_path} failed after {retries} attempts: "
                    f"{resp.status_code} - {msg}"
                )

            # Other status codes: fail fast.
            msg = resp.text.strip() or "<empty body>"
            raise RuntimeError(f"GET {resource_path} failed: {resp.status_code} - {msg}")

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            last_err = e
            print(
                f"Warning: network error on GET {resource_path} "
                f"(attempt {attempt}/{retries}): {e}"
            )

            if attempt < retries:
                sleep_s = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                time.sleep(sleep_s)
                continue

            raise RuntimeError(
                f"GET {resource_path} failed after {retries} attempts "
                f"due to network errors: {e}"
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

    return name or "unnamed"


def ensure_dir(path: str) -> None:
    """
    Create a directory if it does not already exist.
    """
    os.makedirs(path, exist_ok=True)


def remove_stale_index_file(directory: str) -> None:
    """
    Remove an index.json from a previous version of this script, if present.
    """
    index_path = os.path.join(directory, "index.json")

    if os.path.isfile(index_path):
        os.remove(index_path)
        print(f"Removed stale index file -> {index_path}")


def extract_items(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Extract items and pagination metadata from a standard LogicMonitor response.
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
    """
    Write an object to a pretty-printed JSON file.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def unique_json_path(directory: str, filename: str) -> str:
    """
    Return a unique JSON file path inside directory.

    This avoids overwriting files when multiple modules in the same module type
    have the same name. The LogicModule ID is intentionally not used.
    """
    stem, ext = os.path.splitext(filename)
    candidate = os.path.join(directory, filename)
    counter = 2

    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{stem}__{counter}{ext}")
        counter += 1

    return candidate


def module_filename(item: Dict[str, Any]) -> str:
    """
    Build a per-module filename without using the LogicModule ID.

    Preference order:
    - name
    - displayName
    - oid
    - unnamed
    """
    name = (
        item.get("name")
        or item.get("displayName")
        or item.get("oid")
        or "unnamed"
    )

    return f"{safe_filename(str(name))}.json"


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

MODULE_TYPE_ALIASES = {
    "propertyrules": "propertysources",
}

DEFAULT_FORMAT = "json"
DEFAULT_SORT = "+id"


def list_all_items(
    resource_path: str,
    size: int = 200,
    sleep_s: float = 0.0,
    fields: Optional[str] = None,
    filter_expr: Optional[str] = None,
    sort_expr: Optional[str] = DEFAULT_SORT,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Fetch all items for a list endpoint using offset/size paging.
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {
            "format": DEFAULT_FORMAT,
            "size": size,
            "offset": offset,
        }

        if sort_expr:
            params["sort"] = sort_expr

        if fields:
            params["fields"] = fields

        if filter_expr:
            params["filter"] = filter_expr

        payload = api_get(resource_path, params=params, debug=debug)
        items, meta = extract_items(payload)

        dict_items = [item for item in items if isinstance(item, dict)]
        all_items.extend(dict_items)

        total = meta.get("total")

        # Optional pacing between pages.
        # This is separate from HTTP 429 handling.
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
    debug: bool,
) -> None:
    """
    Export one module type and continue on error.

    Writes:
      - <out_dir>/<module_key>/<name>.json
      - <out_dir>/<module_key>/<name>__2.json if duplicate names exist
      - <out_dir>/<module_key>/_error.txt if failed

    Does not write index.json.
    """
    if module_key not in MODULE_ENDPOINTS:
        raise ValueError(f"Unknown module type: {module_key}")

    resource_path = MODULE_ENDPOINTS[module_key]
    module_dir = os.path.join(out_dir, module_key)
    ensure_dir(module_dir)
    remove_stale_index_file(module_dir)

    print(f"\n== Exporting {module_key} from {resource_path} ==")

    try:
        items = list_all_items(
            resource_path=resource_path,
            size=size,
            sleep_s=sleep_s,
            fields=fields,
            filter_expr=filter_expr,
            sort_expr=DEFAULT_SORT,
            debug=debug,
        )
    except Exception as e:
        err_path = os.path.join(module_dir, "_error.txt")

        with open(err_path, "w", encoding="utf-8") as f:
            f.write(str(e) + "\n")

        print(f"ERROR exporting {module_key}: {e}")
        print(f"Continuing. Details saved to: {err_path}")
        return

    print(
        f"Found {len(items)} modules of type: {module_key}. "
        f"Will save to the folder: {os.path.abspath(module_dir)}"
    )

    saved_count = 0

    for item in items:
        fname = module_filename(item)
        item_path = unique_json_path(module_dir, fname)
        write_json(item_path, item)
        saved_count += 1

    print(f"Saved {saved_count} per-item files -> {module_dir}")


# -----------------------------
# CLI
# -----------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    If no arguments are provided, print help and exit cleanly.
    """
    argv = sys.argv[1:] if argv is None else argv

    examples = f"""
Examples:
  Export all module types:
    python export_modules.py --types all --out output_modules

  Export only DataSources and EventSources:
    python export_modules.py --types datasources eventsources --out output_modules

  Export DataSources matching a name filter:
    python export_modules.py --types datasources --filter 'name~"CPU"' --out output_modules

  Export all module types with larger page size and page pacing:
    python export_modules.py --types all --out output_modules --size 200 --sleep 0.2

  Export only AppliesTo Functions:
    python export_modules.py --types appliestofunctions --out output_modules

  Export only SNMP SysOID maps:
    python export_modules.py --types oids --out output_modules

  Export PropertySources:
    python export_modules.py --types propertysources --out output_modules

  Export PropertySources using the API endpoint alias:
    python export_modules.py --types propertyrules --out output_modules

  Debug PropertySources request URLs:
    python export_modules.py --types propertysources --out output_modules --debug

Valid module types:
  {", ".join(MODULE_ENDPOINTS.keys())}
  all

Aliases:
  propertyrules -> propertysources
"""

    parser = argparse.ArgumentParser(
        description="Export LogicMonitor Modules (LogicModules) to JSON.",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--types",
        nargs="+",
        default=["all"],
        help=(
            "Module types to export. Use one or more of: "
            + ", ".join(MODULE_ENDPOINTS.keys())
            + ", propertyrules"
            + " or 'all'."
        ),
    )

    parser.add_argument(
        "--out",
        default="output_modules",
        help="Output directory (default: output_modules).",
    )

    parser.add_argument(
        "--size",
        type=int,
        default=200,
        help="Page size for list endpoints (default: 200).",
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between pages (default: 0).",
    )

    parser.add_argument(
        "--fields",
        default=None,
        help="Optional fields query param, comma-separated. Some endpoints may ignore this.",
    )

    parser.add_argument(
        "--filter",
        default=None,
        help="Optional LM filter expression. Example: 'name~\"CPU\"'.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print fully encoded request URLs, resource paths, response status codes, and non-200 body previews.",
    )

    if not argv:
        parser.print_help(sys.stdout)
        raise SystemExit(0)

    return parser.parse_args(argv)


def normalize_requested_types(types: List[str]) -> List[str]:
    """
    Normalize, alias, deduplicate, and validate requested module types.
    """
    normalized: List[str] = []

    for module_type in types:
        requested_type = module_type.lower()
        canonical_type = MODULE_TYPE_ALIASES.get(requested_type, requested_type)

        if canonical_type == "all":
            normalized.extend(list(MODULE_ENDPOINTS.keys()))
            continue

        normalized.append(canonical_type)

    unknown = [module_type for module_type in normalized if module_type not in MODULE_ENDPOINTS]

    if unknown:
        valid = ", ".join(MODULE_ENDPOINTS.keys())
        aliases = ", ".join(MODULE_TYPE_ALIASES.keys())
        raise SystemExit(
            f"Unknown module type(s): {', '.join(unknown)}\n"
            f"Valid values: {valid}, all\n"
            f"Aliases: {aliases}"
        )

    deduped: List[str] = []
    seen = set()

    for module_type in normalized:
        if module_type not in seen:
            deduped.append(module_type)
            seen.add(module_type)

    return deduped


def main() -> None:
    args = parse_args()

    # Load .env only after args are parsed so --help and no-arg help work
    # without requiring credentials.
    load_lm_config()

    ensure_dir(args.out)

    requested = normalize_requested_types(args.types)

    for module_key in requested:
        export_module_type(
            module_key=module_key,
            out_dir=args.out,
            size=args.size,
            sleep_s=args.sleep,
            fields=args.fields,
            filter_expr=args.filter,
            debug=args.debug,
        )

    print("\nDone.")


if __name__ == "__main__":
    main()

# EOF
