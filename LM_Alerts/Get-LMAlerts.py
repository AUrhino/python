"""
LogicMonitor API - Get Devices + Capture Alerts
------------------------------------------------
Extends your baseline script to:
- Page through devices
- Capture alerts account-wide and/or for a specific device
- Save raw JSON outputs to ./output

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
  ACCESS_ID=your_access_id
  ACCESS_KEY=your_access_key
  COMPANY=your_company_name
"""

import os
import time
import hmac
import hashlib
import base64
import json
from typing import Dict, Any, List, Optional, Tuple
import requests
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY", "")
ACCESS_ID = os.getenv("ACCESS_ID", "")
COMPANY = os.getenv("COMPANY", "")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
OUT_DIR = "output"


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> dict:
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
    return {"Content-Type": "application/json", "Authorization": auth}


def api_get(resource_path: str, params: Optional[Dict[str, Any]] = None) -> dict:
    """
    Perform a GET request to the LogicMonitor API.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    print(f"Error: {response.status_code} - {response.text}")
    return {}


def display_table(data: list, headers: list, title: str = ""):
    """
    Display data in a formatted table.
    """
    if title:
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
    print(tabulate(data, headers=headers, tablefmt="grid"))


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def save_json(filename: str, payload: Any):
    ensure_out_dir()
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved: {path}")


def paged_get_items(
    resource_path: str,
    base_params: Optional[Dict[str, Any]] = None,
    page_size: int = 200
) -> Tuple[List[dict], dict]:
    """
    Fetch all items from a paginated LM endpoint that uses size/offset.
    Returns: (all_items, last_raw_response)
    """
    all_items: List[dict] = []
    offset = 0
    last_resp: dict = {}

    while True:
        params = dict(base_params or {})
        params["size"] = page_size
        params["offset"] = offset

        resp = api_get(resource_path, params=params)
        last_resp = resp

        data = resp.get("data", {})
        items = data.get("items", [])
        if not isinstance(items, list):
            break

        all_items.extend([i for i in items if isinstance(i, dict)])

        # LM usually returns "total" for pagination responses.
        total = data.get("total")
        if isinstance(total, int):
            offset += page_size
            if offset >= total:
                break
        else:
            # Fallback: stop when we get fewer than page_size
            if len(items) < page_size:
                break
            offset += page_size

    return all_items, last_resp


# -------------------------------
# Devices
# -------------------------------

def get_devices(page_size: int = 200) -> List[dict]:
    """
    Fetch and display all devices (paged).
    Endpoint: GET /device/devices (paged via size/offset). :contentReference[oaicite:3]{index=3}
    """
    print("\nFetching devices (paged)...")
    devices, raw = paged_get_items("/device/devices", base_params=None, page_size=page_size)

    if not devices:
        print("No devices returned (or invalid response).")
        save_json("raw_getDevices.json", raw)
        return []

    table_data = []
    for device in devices:
        device_id = device.get("id")
        name = device.get("name")
        display_name = device.get("displayName")

        auto_props = {p.get("name"): p.get("value") for p in device.get("autoProperties", []) if isinstance(p, dict)}
        sys_props = {p.get("name"): p.get("value") for p in device.get("systemProperties", []) if isinstance(p, dict)}

        manufacturer = auto_props.get("auto.endpoint.manufacturer")
        sysinfo = sys_props.get("system.sysinfo")
        description = auto_props.get("auto.entphysical.descr")

        table_data.append([device_id, name, display_name, manufacturer, sysinfo, description])

    headers = ["Device ID", "Name", "Display Name", "Manufacturer", "Sysinfo", "Description"]
    display_table(table_data, headers, f"LogicMonitor Devices (count={len(devices)})")

    save_json("getDevices.json", devices)
    return devices


# -------------------------------
# Alerts (NEW)
# -------------------------------

def get_alerts_accountwide(
    lm_filter: Optional[str] = None,
    fields: Optional[str] = None,
    page_size: int = 200
) -> List[dict]:
    """
    Capture alerts account-wide.
    Endpoint: GET /alert/alerts (paged via size/offset, supports filter). :contentReference[oaicite:4]{index=4}

    lm_filter examples (depends on LM filter syntax in your portal):
      - "cleared:false" (active only)  [example only]
      - "severity:>=3"                [example only]
    """
    print("\nCapturing account-wide alerts...")
    params: Dict[str, Any] = {}
    if lm_filter:
        params["filter"] = lm_filter
    if fields:
        params["fields"] = fields

    alerts, raw = paged_get_items("/alert/alerts", base_params=params, page_size=page_size)

    if not alerts:
        print("No alerts found.")
        save_json("raw_getAlerts_accountwide.json", raw)
        return []

    # Small, readable table (don’t print everything)
    table_data = []
    for a in alerts[:50]:
        table_data.append([
            a.get("id"),
            a.get("monitorObjectName"),
            a.get("resourceTemplateName"),
            a.get("instanceName"),
            a.get("dataPointName"),
            a.get("severity"),
            a.get("startEpoch"),
            a.get("cleared"),
            a.get("acked"),
        ])

    headers = ["Alert ID", "Object", "DataSource", "Instance", "DataPoint", "Severity", "StartEpoch", "Cleared", "Acked"]
    display_table(table_data, headers, f"Account Alerts (showing first 50 of {len(alerts)})")

    save_json("getAlerts_accountwide.json", alerts)
    return alerts


def get_alerts_for_device(
    device_id: int,
    lm_filter: Optional[str] = None,
    need_message: bool = False,
    bound: str = "instances",
    page_size: int = 200,
    start: Optional[int] = None,
    end: Optional[int] = None
) -> List[dict]:
    """
    Capture alerts for a specific device.
    Endpoint: GET /device/devices/{id}/alerts :contentReference[oaicite:5]{index=5}

    Query params supported in swagger include:
      - needMessage (bool)
      - bound (defaults to "instances")
      - size/offset/filter
      - start/end (epoch) :contentReference[oaicite:6]{index=6}
    """
    print(f"\nCapturing alerts for device ID: {device_id}")
    params: Dict[str, Any] = {
        "needMessage": str(need_message).lower(),
        "bound": bound
    }
    if lm_filter:
        params["filter"] = lm_filter
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end

    alerts, raw = paged_get_items(f"/device/devices/{device_id}/alerts", base_params=params, page_size=page_size)

    if not alerts:
        print("No alerts found for this device.")
        save_json(f"raw_getAlerts_device_{device_id}.json", raw)
        return []

    table_data = []
    for a in alerts[:50]:
        table_data.append([
            a.get("id"),
            a.get("monitorObjectName"),
            a.get("resourceTemplateName"),
            a.get("instanceName"),
            a.get("dataPointName"),
            a.get("severity"),
            a.get("startEpoch"),
            a.get("cleared"),
            a.get("acked"),
        ])

    headers = ["Alert ID", "Object", "DataSource", "Instance", "DataPoint", "Severity", "StartEpoch", "Cleared", "Acked"]
    display_table(table_data, headers, f"Device Alerts (showing first 50 of {len(alerts)})")

    save_json(f"getAlerts_device_{device_id}.json", alerts)
    return alerts


def get_alert_by_id(alert_id: str, need_message: bool = True, fields: Optional[str] = None) -> dict:
    """
    Fetch a single alert by ID.
    Endpoint: GET /alert/alerts/{id} :contentReference[oaicite:7]{index=7}
    """
    print(f"\nFetching alert by ID: {alert_id}")
    params: Dict[str, Any] = {"needMessage": str(need_message).lower()}
    if fields:
        params["fields"] = fields
    resp = api_get(f"/alert/alerts/{alert_id}", params=params)
    save_json(f"getAlert_{alert_id}.json", resp)
    return resp


if __name__ == "__main__":
    # Keep your baseline flow, but add alert capture choices.
    devices = get_devices()

    # Capture account-wide alerts
    if input("\nCapture account-wide alerts? (y/n): ").strip().lower() == "y":
        lm_filter = input("Optional LM filter (blank for none): ").strip() or None
        get_alerts_accountwide(lm_filter=lm_filter)

    # Capture per-device alerts
    if devices and input("\nCapture alerts for a specific device? (y/n): ").strip().lower() == "y":
        device_id = int(input("Enter device ID: ").strip())
        lm_filter = input("Optional LM filter (blank for none): ").strip() or None
        need_msg = input("Include alert detail message? (y/n): ").strip().lower() == "y"
        get_alerts_for_device(device_id=device_id, lm_filter=lm_filter, need_message=need_msg)

    # Optional: fetch a single alert by ID
    if input("\nFetch a single alert by alert ID? (y/n): ").strip().lower() == "y":
        alert_id = input("Enter alert ID (string): ").strip()
        get_alert_by_id(alert_id, need_message=True)

    print("\nDone.")
