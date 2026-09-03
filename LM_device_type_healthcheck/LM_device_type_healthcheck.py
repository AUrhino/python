#!/usr/bin/env python3
"""Identify LogicMonitor resources with little or no active monitoring.

The script lists datasources whose ``instanceNumber`` is greater than zero.
SNMP datasources are summarized as ``SNMP monitoring`` for Cisco resources,
and WMI/WINRM datasources are summarized as ``Windows monitoring`` for
Windows resources.  Relevant auto-properties are included for troubleshooting.

Credentials are read from ACCESS_ID, ACCESS_KEY, and COMPANY in the
environment (or a .env file).
"""

import argparse
import base64
import csv
import hashlib
import json
import hmac
import os
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")
BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
DEBUG = False


def api_get(path, params=None):
    epoch = str(int(time.time() * 1000))
    digest = hmac.new(ACCESS_KEY.encode(), ("GET" + epoch + path).encode(), hashlib.sha256).hexdigest()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Version": "3",
        "Authorization": f"LMv1 {ACCESS_ID}:{base64.b64encode(digest.encode()).decode()}:{epoch}",
    }
    query = urlencode(params or {})
    if DEBUG:
        print(f"[DEBUG] GET {path} params={params or {}}")
    response = requests.get(BASE_URL + path + (f"?{query}" if query else ""), headers=headers, timeout=60)
    if DEBUG:
        print(f"[DEBUG] HTTP {response.status_code} {path}")
    response.raise_for_status()
    return response.json()


def get_items(path, params=None, max_items=None):
    request_params = dict(params or {})
    page_size = int(request_params.get("size", 1000))
    offset = int(request_params.get("offset", 0))
    all_items = []
    total = None
    while True:
        request_params.update(size=page_size, offset=offset)
        try:
            payload = api_get(path, request_params)
        except requests.RequestException as error:
            print(f"Warning: request failed for {path}: {error}")
            return []
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        if isinstance(data, dict):
            page = data.get("items", [])
            if total is None and data.get("total") is not None:
                total = int(data["total"])
        else:
            page = data if isinstance(data, list) else []
        if not isinstance(page, list):
            page = []
        all_items.extend(page)
        if max_items is not None and len(all_items) >= max_items:
            return all_items[:max_items]
        offset += len(page)
        if not page or (total is not None and offset >= total) or len(page) < page_size:
            break
    return all_items


def properties(resource):
    result = {}
    for group in ("autoProperties", "systemProperties", "customProperties", "inheritedProperties"):
        for item in resource.get(group, []) or []:
            if isinstance(item, dict) and item.get("name") is not None:
                result[item["name"]] = item.get("value")
    return result


def classify(resource, props):
    text = " ".join(str(resource.get(key, "")) for key in ("name", "displayName", "deviceType"))
    text += " " + " ".join(str(value) for key, value in props.items() if "manufacturer" in key.lower() or key in ("auto.endpoint.os", "system.sysinfo"))
    text = text.casefold()
    if "cisco" in text:
        return "Cisco"
    if any(value in text for value in ("windows", "microsoft", "winrm", "wmi")):
        return "Windows"
    return "Other"


def is_true(value):
    return str(value).strip().casefold() in ("true", "1", "yes", "y", "t")


def load_active_module_reviews():
    config_path = Path(__file__).with_name("review-active-modules.json")
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)
        return {str(key): value if isinstance(value, list) else [value] for key, value in config.items()}
    except (OSError, json.JSONDecodeError, AttributeError) as error:
        raise SystemExit(f"Unable to load {config_path}: {error}")


def check_resource(resource, include_properties=(), review_active_modules=False, module_reviews=None):
    props = properties(resource)
    platform = classify(resource, props)
    datasources = get_items(
        f"/device/devices/{resource['id']}/devicedatasources",
        {"fields": "id,module,dataSourceName,dataSourceDisplayName,instanceNumber", "size": 1000, "offset": 0},
    )
    if DEBUG:
        debug_datasources = [{key: datasource.get(key) for key in
                              ("id", "module", "dataSourceName", "dataSourceDisplayName", "instanceNumber")}
                             for datasource in datasources]
        print(f"[DEBUG] Resource {resource.get('id')} datasource response:")
        print(json.dumps(debug_datasources, indent=2, default=str))
    active = [ds for ds in datasources if (ds.get("instanceNumber", 0) or 0) > 0]
    modules = [ds.get("module") or ds.get("dataSourceName") or ds.get("dataSourceDisplayName") or str(ds.get("id")) for ds in active]
    names = [ds.get("module") or ds.get("dataSourceName") or ds.get("dataSourceDisplayName") or str(ds.get("id")) for ds in active]
    review_names = list(names)
    snmp = [name for name in names if "snmp" in name.casefold()]
    windows = [name for name in names if any(term in name.casefold() for term in ("wmi", "winrm"))]
    categories = str(props.get("system.categories", "")).casefold()
    hoststatus = str(props.get("system.hoststatus", "")).casefold()
    active_ds = str(props.get("auto.activedatasources", "; ".join(names))).casefold()
    minimal_monitoring = "minimal monitoring" in str(props.get("system.groups", "")).casefold()
    methods = []
    if DEBUG:
        debug_properties = {key: props.get(key) for key in
                            ("system.categories", "system.hoststatus", "system.collector",
                             "auto.activedatasources", "auto.snmp.operational", "auto.wmi.operational",
                             "auto.api.responding", "auto.ssh.available", "system.sysoid")}
        print(f"[DEBUG] Resource {resource.get('id')} monitoring properties: {json.dumps(debug_properties, default=str)}")
    if "noping" in categories:
        methods.append("noping")
    else:
        methods.append("DEAD" if hoststatus == "dead" else "ping")
        if is_true(props.get("auto.snmp.operational")):
            methods.append("snmp")
        if is_true(props.get("auto.wmi.operational")):
            methods.append("wmi")
        if is_true(props.get("auto.api.responding")):
            methods.append("api")
        if is_true(props.get("auto.ssh.available")):
            methods.append("ssh")
        datasource_text = " ".join(modules + names).casefold()
        if props.get("system.collector") and "collector" in datasource_text:
            methods.append("collector")
        if "http" in active_ds:
            methods.append("http")
        if "https" in active_ds:
            methods.append("https")
    if platform == "Cisco":
        names = [name for name in names if name not in snmp]
    elif platform == "Windows":
        names = [name for name in names if name not in windows]
    monitoring = ",".join(methods)
    row = [resource.get("id"), resource.get("displayName") or resource.get("name"), resource.get("name", ""),
           props.get("auto.entphysical.descr", ""),
            props.get("predef.externalResourceType", ""), props.get("system.ips", ""), resource.get("hostStatus", ""), platform,
            monitoring, minimal_monitoring, "; ".join(modules), "; ".join(names), len(active), props.get("auto.snmp.operational", ""),
            props.get("auto.ssh.available", ""), props.get("auto.ssh.status", ""), props.get("auto.wmi.operational", ""),
            props.get("auto.api.responding", ""),
            props.get("system.sysoid", "") if snmp else ""]
    if review_active_modules:
        active_text = " ".join(review_names).casefold()
        matches = [any(str(alias).casefold() in active_text for alias in aliases) for aliases in module_reviews.values()]
        row.extend(matches)
        if DEBUG:
            print(f"[DEBUG] Resource {resource.get('id')}: active module review={dict(zip(module_reviews, matches))}")
    for property_name in include_properties:
        row.append(props.get(property_name, ""))
    return row


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Examples:\n"
            "  python3 LM_device_type_healthcheck.py\n"
            "  python3 LM_device_type_healthcheck.py --csv output/healthcheck.csv\n"
            "  python3 LM_device_type_healthcheck.py --folder output --csv healthcheck.csv\n"
            "  python3 LM_device_type_healthcheck.py --folder /tmp/lm-reports\n"
            "  python3 LM_device_type_healthcheck.py --markdown output/healthcheck.md\n"
            "  python3 LM_device_type_healthcheck.py --included-properties predef.externalResourceID\n"
            "  python3 LM_device_type_healthcheck.py --include-properties snmptrap.authToken\n"
            "  python3 LM_device_type_healthcheck.py --include-properties snmptrap.authToken,ssh.user\n"
            "  python3 LM_device_type_healthcheck.py --extra-fields deviceType,enableNetflow,link\n"
            "  python3 LM_device_type_healthcheck.py --creds-file ~/.config/logicmonitor.env\n"
            "  python3 LM_device_type_healthcheck.py --group-id 36\n"
            "  python3 LM_device_type_healthcheck.py --group-name 'Linux Servers'\n"
            "  python3 LM_device_type_healthcheck.py --test-mode --csv output/test-healthcheck.csv\n"
            "  python3 LM_device_type_healthcheck.py --debug --test-mode\n"
            "  python3 LM_device_type_healthcheck.py --output-folder reports --markdown reports/healthcheck.md\n"
            "  source ~/python/bin/activate && python3 LM_device_type_healthcheck.py --csv healthcheck.csv"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--csv", dest="csv_path", help="Also write results to this CSV path.")
    parser.add_argument("--folder", "--output-folder", dest="output_folder",
                        help="Folder for the CSV report. Defaults the filename to healthcheck.csv.")
    parser.add_argument("--markdown", dest="markdown_path", metavar="PATH",
                        help="Write the health-check results as a Markdown table.")
    parser.add_argument("--include-properties", "--included-properties", action="append", dest="included_properties",
                        metavar="NAME[,NAME... ]", help="Include named properties as columns. Repeat or use comma-separated names.")
    parser.add_argument("--extra-fields", action="append", dest="extra_fields", metavar="NAME[,NAME... ]",
                        help="Include top-level device API fields as columns. Repeat or use comma-separated names.")
    parser.add_argument("--review-active-modules", action="store_true",
                        help="Add True/False columns defined in review-active-modules.json.")
    parser.add_argument("--creds-file", metavar="PATH",
                        help="Use this credentials file instead of .env for this run.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--group-name", metavar="NAME", help="Get devices from the LogicMonitor group with this name or path.")
    group.add_argument("--group-id", type=int, metavar="ID", help="Get devices from the LogicMonitor group ID.")
    parser.add_argument("--test-mode", action="store_true", help="Process only the first 10 devices.")
    parser.add_argument("--debug", action="store_true", help="Show API requests and module matching details.")
    args = parser.parse_args()
    if not args.csv_path and not args.output_folder and not args.markdown_path:
        args.csv_path = "output/healthcheck.csv"
    global DEBUG
    DEBUG = args.debug
    module_reviews = load_active_module_reviews() if args.review_active_modules else {}
    global ACCESS_ID, ACCESS_KEY, COMPANY, BASE_URL
    if args.creds_file:
        credentials_file = Path(args.creds_file).expanduser()
        if not credentials_file.is_file():
            raise SystemExit(f"Credentials file not found: {credentials_file}")
        load_dotenv(credentials_file, override=True)
        ACCESS_KEY, ACCESS_ID, COMPANY = os.getenv("ACCESS_KEY"), os.getenv("ACCESS_ID"), os.getenv("COMPANY")
        BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
    missing = [name for name, value in (("ACCESS_ID", ACCESS_ID), ("ACCESS_KEY", ACCESS_KEY), ("COMPANY", COMPANY)) if not value]
    if missing:
        raise SystemExit(f"Missing required environment variable(s): {', '.join(missing)}")
    if args.group_id is not None:
        group_id = args.group_id
    elif args.group_name:
        groups = get_items("/device/groups", {"fields": "id,name,fullPath", "size": 1000, "offset": 0})
        matches = [item for item in groups if str(item.get("name", "")).casefold() == args.group_name.casefold()
                   or str(item.get("fullPath", "")).casefold() == args.group_name.casefold()]
        if not matches:
            raise SystemExit(f"LogicMonitor group not found: {args.group_name}")
        if len(matches) > 1:
            raise SystemExit(f"Multiple LogicMonitor groups matched {args.group_name!r}; use --group-id.")
        group_id = matches[0].get("id")
    else:
        group_id = None
    resource_path = f"/device/groups/{group_id}/devices" if group_id is not None else "/device/devices"
    resources = get_items(resource_path, {"size": 10 if args.test_mode else 1000, "offset": 0}, max_items=10 if args.test_mode else None)
    print(f"Total devices found: {len(resources)}" + (" (test mode limit)" if args.test_mode else ""))
    headers = ["Resource ID", "Resource", "name_or_fqdn", "Model", "External resource type", "IP addresses", "hostStatus", "Type", "Monitoring", "Minimal Monitoring", "Module", "Active datasources (other)", "Active datasource count",
               "auto.snmp.operational", "auto.ssh.available", "auto.ssh.status", "auto.wmi.operational",
               "auto.api.responding", "system.sysoid"]
    if args.review_active_modules:
        headers.extend(module_reviews)
    included_properties = []
    for value in args.included_properties or []:
        included_properties.extend(name.strip() for name in value.split(",") if name.strip())
    included_properties = list(dict.fromkeys(included_properties))
    extra_fields = []
    for value in args.extra_fields or []:
        extra_fields.extend(name.strip() for name in value.split(",") if name.strip())
    extra_fields = list(dict.fromkeys(extra_fields))
    headers.extend(included_properties)
    headers.extend(extra_fields)
    rows = []
    for resource in resources:
        if isinstance(resource, dict) and resource.get("id") is not None:
            row = check_resource(resource, included_properties, args.review_active_modules, module_reviews)
            row.extend(resource.get(field, "") for field in extra_fields)
            rows.append(row)
    if args.csv_path or args.output_folder:
        output = Path(args.csv_path or "healthcheck.csv").expanduser()
        if args.output_folder:
            output = Path(args.output_folder).expanduser() / output.name
        try:
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"CSV write successful: {output} ({len(rows)} devices written)")
        except OSError as error:
            print(f"CSV write failed: {error}")
    if args.markdown_path:
        output = Path(args.markdown_path).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        markdown_rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
        for row in rows:
            values = [str(value).replace("|", "\\|").replace("\n", " ") for value in row]
            markdown_rows.append("| " + " | ".join(values) + " |")
        output.write_text(f"# LogicMonitor Device Type Health Check\n\nDevice count: {len(rows)}\n\n" + "\n".join(markdown_rows) + "\n", encoding="utf-8")
        print(f"Markdown write successful: {output} ({len(rows)} devices written)")


if __name__ == "__main__":
    main()
