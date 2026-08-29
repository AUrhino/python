"""List LogicMonitor devices with optional fields, properties, CSV, and counts."""
import argparse, base64, csv, hashlib, hmac, json, os, time
from pathlib import Path
import requests
from dotenv import load_dotenv
from tabulate import tabulate

__version__ = "1.03"

load_dotenv()
ACCESS_KEY, ACCESS_ID, COMPANY = os.getenv("ACCESS_KEY"), os.getenv("ACCESS_ID"), os.getenv("COMPANY")
BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
PROPERTY_GROUPS = ("customProperties", "systemProperties", "autoProperties", "inheritedProperties")

def str_to_bool(value):
    if value.strip().lower() in ("true", "t", "yes", "y", "1"): return True
    if value.strip().lower() in ("false", "f", "no", "n", "0"): return False
    raise argparse.ArgumentTypeError("Expected true or false.")

def parse_args():
    parser = argparse.ArgumentParser(description=f"Get LogicMonitor devices (version {__version__}).")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--csv", dest="csv_path", metavar="PATH", help="Write the device table to PATH.")
    parser.add_argument("--show-counts", type=str_to_bool, default=False, metavar="true|false", help="Show DataSource and instance counts for every device (default: false).")
    parser.add_argument("--show_modules", "--show-modules", type=str_to_bool, default=False, metavar="true|false",
                        help="Display DataSources and their instances for every device (default: false).")
    parser.add_argument("--include-properties", metavar="NAME,...", help="Include comma-separated properties, e.g. snmp.community,ssh.user.")
    parser.add_argument("--include-fields", metavar="NAME,...", help="Include additional comma-separated device API fields as columns.")
    parser.add_argument("--filter", action="append", metavar="FIELD=VALUE",
                        help="Filter devices by an API field, e.g. --filter displayName=Lenny. Repeat for multiple filters.")
    return parser.parse_args()

def api_get(path, params=None):
    epoch = str(int(time.time() * 1000))
    digest = hmac.new(ACCESS_KEY.encode(), ("GET" + epoch + path).encode(), hashlib.sha256).hexdigest()
    signature = base64.b64encode(digest.encode()).decode()
    headers = {"Content-Type": "application/json", "Authorization": f"LMv1 {ACCESS_ID}:{signature}:{epoch}"}
    response = requests.get(BASE_URL + path, headers=headers, params=params)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}

def get_items(path, params=None):
    try:
        payload = api_get(path, params)
    except requests.RequestException as error:
        print(f"Warning: request failed for {path}: {error}")
        return []
    data = payload.get("data", {})
    return data.get("items", []) if isinstance(data, dict) and isinstance(data.get("items", []), list) else []

def property_values(device):
    result = {}
    for group in PROPERTY_GROUPS:
        for item in device.get(group, []) or []:
            if isinstance(item, dict) and item.get("name") is not None:
                result[item["name"]] = item.get("value")
    return result

def count_for_device(device):
    datasources = get_items(f"/device/devices/{device['id']}/devicedatasources")
    instances = sum(len(get_items(f"/device/devices/{device['id']}/devicedatasources/{item['dataSourceId']}/instances")) for item in datasources if item.get("dataSourceId") is not None)
    return len(datasources), instances

def show_modules_for_device(device):
    """Display DataSources and instances for one device."""
    device_id = device.get("id")
    label = device.get("displayName") or device.get("name") or device_id
    datasources = get_items(f"/device/devices/{device_id}/devicedatasources")
    print(f"\nDataSources for {label} (ID: {device_id})")
    datasource_rows = [[item.get("dataSourceId"), item.get("dataSourceName"), item.get("deviceName"), item.get("deviceDisplayName")] for item in datasources]
    print(tabulate(datasource_rows, headers=["DataSource ID", "Name", "Device Name", "Display Name"], tablefmt="grid"))
    for datasource in datasources:
        datasource_id = datasource.get("dataSourceId")
        if datasource_id is None:
            continue
        instances = get_items(f"/device/devices/{device_id}/devicedatasources/{datasource_id}/instances")
        print(f"\nInstances for DataSource {datasource_id} ({datasource.get('dataSourceName', '')})")
        instance_rows = [[item.get("id"), item.get("name"), item.get("deviceDisplayName"), item.get("dataSourceId")] for item in instances]
        print(tabulate(instance_rows, headers=["Instance ID", "Name", "Device Display Name", "DataSource ID"], tablefmt="grid"))

def export_csv(path, headers, rows):
    output = Path(path).expanduser(); output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle); writer.writerow(headers); writer.writerows(rows)
    print(f"Saved CSV to {output}")

def build_filters(filter_args):
    """Convert FIELD=VALUE arguments to LogicMonitor filter expressions."""
    expressions = []
    for item in filter_args or []:
        if "=" not in item:
            raise SystemExit(f"Invalid filter {item!r}; use FIELD=VALUE.")
        field, value = item.split("=", 1)
        field, value = field.strip(), value.strip()
        if not field or not value:
            raise SystemExit(f"Invalid filter {item!r}; use FIELD=VALUE.")
        expressions.append(f'{field}:"{value}"')
    return ",".join(expressions)

def main():
    args = parse_args()
    missing = [name for name, value in (("ACCESS_ID", ACCESS_ID), ("ACCESS_KEY", ACCESS_KEY), ("COMPANY", COMPANY)) if not value]
    if missing: raise SystemExit(f"Missing required environment variable(s): {', '.join(missing)}")
    props = [x.strip() for x in (args.include_properties or "").split(",") if x.strip()]
    fields = [x.strip() for x in (args.include_fields or "").split(",") if x.strip()]
    api_fields = list(dict.fromkeys(["id", "name", "displayName"] + fields + (list(PROPERTY_GROUPS) if props else [])))
    params = {"fields": ",".join(api_fields)}
    filter_expression = build_filters(args.filter)
    if filter_expression:
        params["filter"] = filter_expression
    print("Fetching devices...")
    devices = get_items("/device/devices", params)
    print(json.dumps({"total": len(devices)}, indent=2))
    if not devices: return
    headers = ["Device ID", "Name", "Display Name"] + fields + props
    if args.show_counts: headers += ["DataSource Count", "Instance Count"]
    rows = []
    for device in devices:
        values = property_values(device)
        row = [device.get("id"), device.get("name"), device.get("displayName")]
        row += [device.get(field) for field in fields] + [values.get(prop) for prop in props]
        if args.show_counts: row += list(count_for_device(device))
        rows.append(row)
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    Path("output").mkdir(exist_ok=True)
    with open("output/getDevices.json", "w") as handle: json.dump(devices, handle, indent=4)
    if args.csv_path: export_csv(args.csv_path, headers, rows)
    if args.show_modules:
        for device in devices:
            show_modules_for_device(device)

if __name__ == "__main__": main()
