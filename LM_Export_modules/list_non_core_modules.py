"""List LogicMonitor DataSources that are not Core modules.

Credentials are loaded from a .env file containing ACCESS_ID, ACCESS_KEY,
and COMPANY. The script writes LM_Modules_noncore.csv in the current directory.
"""

import base64
import csv
import argparse
import hashlib
import hmac
import os
import time
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv


PAGE_SIZE = 100
API_VERSION = "3"
OUTPUT_FILE = "LM_Modules_noncore.csv"


def load_config() -> str:
    """Load credentials from .env and return the LogicMonitor base URL."""
    load_dotenv()
    access_id = os.getenv("ACCESS_ID", "")
    access_key = os.getenv("ACCESS_KEY", "")
    company = os.getenv("COMPANY", "")

    if not all((access_id, access_key, company)):
        raise SystemExit(
            "Missing ACCESS_ID, ACCESS_KEY, or COMPANY. "
            "Set them in a .env file."
        )

    global ACCESS_ID, ACCESS_KEY
    ACCESS_ID = access_id
    ACCESS_KEY = access_key
    return f"https://{company}.logicmonitor.com/santaba/rest"


ACCESS_ID = ""
ACCESS_KEY = ""


def auth_headers(resource_path: str) -> Dict[str, str]:
    epoch = str(int(time.time() * 1000))
    request_vars = "GET" + epoch + "" + resource_path
    digest = hmac.new(
        ACCESS_KEY.encode("utf-8"), request_vars.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    signature = base64.b64encode(digest.encode("utf-8")).decode("utf-8")
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Version": API_VERSION,
        "Authorization": f"LMv1 {ACCESS_ID}:{signature}:{epoch}",
    }


def get_json(base_url: str, resource_path: str, params: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:
    url = base_url + resource_path
    if debug:
        prepared = requests.Request("GET", url, params=params).prepare()
        print(f"DEBUG request: GET {prepared.url}")
    response = requests.get(
        url,
        params=params,
        headers=auth_headers(resource_path),
        timeout=60,
    )
    if debug:
        print(f"DEBUG response: HTTP {response.status_code}")
    if response.status_code == 429:
        wait = float(response.headers.get("Retry-After", "30"))
        print(f"Rate limited; waiting {wait:g} seconds")
        time.sleep(wait)
        response = requests.get(
            url,
            params=params,
            headers=auth_headers(resource_path),
            timeout=60,
        )
        if debug:
            print(f"DEBUG response after rate limit: HTTP {response.status_code}")
    response.raise_for_status()
    return response.json()


def list_datasources(base_url: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Return all DataSources using offset pagination."""
    datasources: List[Dict[str, Any]] = []
    offset = 0

    while True:
        payload = get_json(
            base_url,
            "/setting/datasources",
            {
                "offset": offset,
                "size": PAGE_SIZE,
                "fields": "name,id,displayName",
                "sort": "+id",
            },
            debug=debug,
        )
        data = payload.get("data", payload)
        items = data.get("items", [])
        datasources.extend(items)
        offset += len(items)
        total = data.get("total", offset)
        if not items or offset >= total:
            return datasources


def datasource_status(base_url: str, datasource: Dict[str, Any], debug: bool = False) -> List[Any]:
    datasource_id = datasource["id"]
    path = f"/setting/registry/metadata/datasource/{datasource_id}"
    try:
        payload = get_json(base_url, path, {}, debug=debug)
    except requests.HTTPError as error:
        # Some custom, unpublished, or deleted modules have no registry
        # metadata record. LogicMonitor returns 404 for those modules.
        if error.response is None or error.response.status_code != 404:
            raise
        payload = {"errorMessage": "Registry metadata not found"}

    if "errorMessage" in payload:
        namespace = "unpublished"
        registry_version = "No Version"
        locator = "No Locator"
        status = "Custom or modified"
    else:
        namespace = payload.get("namespace", "")
        registry_version = payload.get("registryVersion", "")
        locator = payload.get("lmLocator", "")
        status = payload.get("status", "")

    return [
        datasource_id,
        datasource.get("name", ""),
        datasource.get("displayName", ""),
        namespace,
        registry_version,
        locator,
        status,
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List LogicMonitor DataSources that are not Core modules.",
        epilog=("Examples:\n"
                "  python list_non_core_modules.py\n"
                "  python list_non_core_modules.py --output reports/noncore.csv\n"
                "  python list_non_core_modules.py --debug"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output", default=OUTPUT_FILE,
                        help=f"CSV output path (default: {OUTPUT_FILE})")
    parser.add_argument("--debug", action="store_true",
                        help="Print request URLs and HTTP status codes (never credentials)")
    args = parser.parse_args()

    base_url = load_config()
    print("Now pulling DataSources")
    datasources = list_datasources(base_url, debug=args.debug)
    print(f"Found {len(datasources)} DataSources; checking registry status")

    non_core = []
    for index, datasource in enumerate(datasources, start=1):
        row = datasource_status(base_url, datasource, debug=args.debug)
        if row[-1] != "Core":
            non_core.append(row)
        if index % 25 == 0 or index == len(datasources):
            print(f"Checked {index}/{len(datasources)}")

    output_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(
            ["id", "name", "displayName", "namespace", "registryVersion", "lmLocator", "status"]
        )
        writer.writerows(non_core)

    print(f"Done. Wrote {len(non_core)} non-Core DataSources to {output_path}")


if __name__ == "__main__":
    main()
