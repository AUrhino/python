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
import html
import os
import sys
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
__version__ = "1.1.0"


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


def load_active_module_reviews(path=None):
    config_path = Path(path).expanduser() if path else Path(__file__).with_name("review-active-modules.json")
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)
        return {str(key): value if isinstance(value, list) else [value] for key, value in config.items()}
    except (OSError, json.JSONDecodeError, AttributeError) as error:
        raise SystemExit(f"Unable to load {config_path}: {error}")


def load_property_status(path=None):
    config_path = Path(path).expanduser() if path else Path(__file__).with_name("property_status.json")
    try:
        with config_path.open(encoding="utf-8") as handle:
            config = json.load(handle)
        if not isinstance(config, dict):
            raise ValueError("the root value must be an object")
        return {str(key): [str(item).strip().strip("*") for item in (value if isinstance(value, list) else [value])
                           if str(item).strip().strip("*")] for key, value in config.items()}
    except (OSError, json.JSONDecodeError, ValueError, AttributeError) as error:
        raise SystemExit(f"Unable to load {config_path}: {error}")


def create_templates():
    templates = {
        Path(".env_example"): "ACCESS_ID=your_access_id\nACCESS_KEY=your_access_key\nCOMPANY=your_company_name\n",
        Path("modules_example.json"): json.dumps({"Interfaces": ["interface", "winif", "network"], "Processor": ["cpu", "processor", "proc"], "Memory": ["memory", "mem"], "ExampleModule": ["example_module_name"]}, indent=2) + "\n",
    }
    for path, content in templates.items():
        if path.exists():
            print(f"Template creation failed: {path} already exists")
            return
        path.write_text(content, encoding="utf-8")
        print(f"Template created: {path}")


def check_resource(resource, include_properties=(), review_active_modules=False, module_reviews=None, property_status=None):
    property_status = property_status or {}
    property_names = list(dict.fromkeys(name for names in property_status.values() for name in names))
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
                             "auto.activedatasources", *property_names, "auto.api.responding", "system.sysoid")}
        print(f"[DEBUG] Resource {resource.get('id')} monitoring properties: {json.dumps(debug_properties, default=str)}")
    if "noping" in categories:
        methods.append("noping")
    else:
        methods.append("DEAD" if hoststatus == "dead" else "ping")
        if any(is_true(props.get(name)) for name in property_status.get("SNMP", [])):
            methods.append("snmp")
        if any(is_true(props.get(name)) for name in property_status.get("WMI", []) + property_status.get("WINRM", [])):
            methods.append("wmi")
        if is_true(props.get("auto.api.responding")):
            methods.append("api")
        if any(is_true(props.get(name)) for name in property_status.get("SSH", [])):
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
            monitoring, minimal_monitoring, "; ".join(modules), "; ".join(names), len(active),
            *[props.get(name, "") for name in property_names],
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


def write_html_report(path, headers, rows):
    def cell(value):
        text = "" if value is None else str(value)
        css = ' class="false-value"' if text.strip().casefold() == "false" else ""
        return f"<td{css}>{html.escape(text)}</td>"

    header_cells = "".join(f'<th data-column="{index}">{html.escape(str(header))}<span class="resize-handle"></span></th>'
                            for index, header in enumerate(headers))
    body = "\n".join("<tr>" + "".join(cell(value) for value in row) + "</tr>" for row in rows)
    document = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>LogicMonitor Device Type Health Check</title>
<style>
body {{ font: 14px system-ui, sans-serif; margin: 20px; }}
.table-wrap {{ overflow: auto; max-height: 85vh; border: 1px solid #ccc; }}
table {{ border-collapse: collapse; white-space: nowrap; }}
th, td {{ border: 1px solid #ccc; padding: 6px 9px; text-align: left; }}
th {{ position: sticky; top: 0; background: #eee; cursor: pointer; user-select: none; }}
th .resize-handle {{ position: absolute; right: 0; top: 0; width: 7px; height: 100%; cursor: col-resize; }}
.false-value {{ background: #f8caca; color: #8b0000; font-weight: 600; }}
</style></head><body><h1>LogicMonitor Device Type Health Check</h1>
<p>Device count: {len(rows)}</p><div class="table-wrap"><table id="report"><thead><tr>{header_cells}</tr></thead><tbody>{body}</tbody></table></div>
<script>
const table = document.getElementById('report');
table.querySelectorAll('th').forEach((th, index) => {{
  th.addEventListener('click', event => {{ if (event.target.classList.contains('resize-handle')) return;
    const rows = [...table.tBodies[0].rows], ascending = th.dataset.order !== 'asc';
    rows.sort((a,b) => a.cells[index].textContent.localeCompare(b.cells[index].textContent, undefined, {{numeric:true, sensitivity:'base'}}) * (ascending ? 1 : -1));
    rows.forEach(row => table.tBodies[0].appendChild(row)); th.dataset.order = ascending ? 'asc' : 'desc';
  }});
  const handle = th.querySelector('.resize-handle');
  handle.addEventListener('mousedown', event => {{ event.stopPropagation(); const startX = event.pageX, startWidth = th.offsetWidth;
    const move = e => th.style.width = Math.max(40, startWidth + e.pageX - startX) + 'px';
    const stop = () => {{ document.removeEventListener('mousemove', move); document.removeEventListener('mouseup', stop); }};
    document.addEventListener('mousemove', move); document.addEventListener('mouseup', stop);
  }});
}});
</script></body></html>'''
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "## Usage\n"
            "  Run the health check, display help, or create starter files.\n"
            "  python3 LM_device_type_healthcheck.py\n"
            "  python3 LM_device_type_healthcheck.py --help\n"
            "  python3 LM_device_type_healthcheck.py --create-template\n\n"
            "## Output\n"
            "  Save results as CSV or Markdown. The default CSV is output/healthcheck.csv.\n"
            "  python3 LM_device_type_healthcheck.py --format csv\n"
            "  python3 LM_device_type_healthcheck.py --format html --folder output\n"
            "  python3 LM_device_type_healthcheck.py --format markdown --folder /tmp/lm-reports\n\n"
            "## Filter by group\n"
            "  Check devices in a LogicMonitor group by ID or name/path.\n"
            "  python3 LM_device_type_healthcheck.py --group-id 36\n"
            "  python3 LM_device_type_healthcheck.py --group-name 'Linux Servers'\n\n"
            "## Running with creds\n"
            "  Use .env by default, or provide a custom credentials file.\n"
            "  python3 LM_device_type_healthcheck.py --creds-file ~/.config/logicmonitor.env\n\n"
            "## Custom\n"
            "  Add named properties, API fields, or custom active-module definitions.\n"
            "  python3 LM_device_type_healthcheck.py --included-properties predef.externalResourceID\n"
            "  python3 LM_device_type_healthcheck.py --include-properties snmptrap.authToken,ssh.user\n"
            "  python3 LM_device_type_healthcheck.py --extra-fields deviceType,enableNetflow,link\n"
            "  python3 LM_device_type_healthcheck.py --modules-file modules_example.json --review-active-modules --group-id 36\n"
            "  Use groups and device fields with these templates to create focused reports.\n\n"
            "## Testing and debug\n"
            "  Limit processing to 10 devices or inspect API/module matching details.\n"
            "  python3 LM_device_type_healthcheck.py --test-mode --format csv\n"
            "  python3 LM_device_type_healthcheck.py --debug --test-mode\n\n"
            "## Author\n"
            "Ryan Gillan\n"
            "Email: ryangillan@gmail.com"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--format", choices=("csv", "html", "markdown"), default="csv",
                        help="Output format. Defaults to csv.")
    parser.add_argument("--folder", "--output-folder", dest="output_folder",
                        help="Folder for the CSV report. Defaults the filename to healthcheck.csv.")
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
    parser.add_argument("--modules-file", "--module-file", metavar="PATH", help="Override review-active-modules.json with a custom module definition file.")
    parser.add_argument("--property-status-file", metavar="PATH", help="Override property_status.json with a custom property definition file.")
    parser.add_argument("--create-template", action="store_true", help="Create .env_example and modules_example.json, then exit.")
    args = parser.parse_args()
    if args.create_template:
        create_templates()
        return
    if len(sys.argv) == 1:
        parser.print_help()
        return
    if not args.output_folder:
        args.output_folder = "output"
    global DEBUG
    DEBUG = args.debug
    module_reviews = load_active_module_reviews(args.modules_file) if args.review_active_modules else {}
    property_status = load_property_status(args.property_status_file)
    property_names = list(dict.fromkeys(name for names in property_status.values() for name in names))
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
    headers = ["Resource ID", "Resource", "name_or_fqdn", "Model", "External resource type", "IP addresses", "hostStatus", "Type", "Monitoring", "Minimal Monitoring", "Module", "Active datasources (other)", "Active datasource count"]
    headers.extend(property_names)
    headers.append("system.sysoid")
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
            row = check_resource(resource, included_properties, args.review_active_modules, module_reviews, property_status)
            row.extend(resource.get(field, "") for field in extra_fields)
            rows.append(row)
    output = Path(args.output_folder).expanduser() / f"healthcheck.{'md' if args.format == 'markdown' else args.format}"
    if args.format == "csv":
        try:
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f"CSV write successful: {output} ({len(rows)} devices written)")
        except OSError as error:
            print(f"CSV write failed: {error}")
    if args.format == "markdown":
        try:
            output.parent.mkdir(parents=True, exist_ok=True)
            markdown_rows = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
            for row in rows:
                values = []
                for value in row:
                    text = str(value).replace("|", "\\|").replace("\n", " ")
                    if text.strip().casefold() == "false":
                        text = f'<span style="color:#8b0000;background-color:#f8caca;font-weight:600">{text}</span>'
                    values.append(text)
                markdown_rows.append("| " + " | ".join(values) + " |")
            output.write_text(f"# LogicMonitor Device Type Health Check\n\nDevice count: {len(rows)}\n\n" + "\n".join(markdown_rows) + "\n", encoding="utf-8")
            print(f"Markdown write successful: {output} ({len(rows)} devices written)")
        except OSError as error:
            print(f"Markdown write failed: {error}")
    if args.format == "html":
        output.parent.mkdir(parents=True, exist_ok=True)
        try:
            write_html_report(output, headers, rows)
            print(f"HTML write successful: {output} ({len(rows)} devices written)")
        except OSError as error:
            print(f"HTML write failed: {error}")


if __name__ == "__main__":
    main()
