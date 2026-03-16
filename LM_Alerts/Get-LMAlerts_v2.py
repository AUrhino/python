#!/usr/bin/env python3
"""
Get-LMAlerts_v2.py
------------------
LogicMonitor Alerts CLI

Modes:
  1) Capture account-wide alerts
  2) Capture alerts for a specific object name / monitorObjectName
  3) Fetch a single alert by alert ID

Requirements:
- Python 3.x
- requests, python-dotenv
- optional: tabulate
- .env file with:
    ACCESS_ID=your_access_id
    ACCESS_KEY=your_access_key
    COMPANY=your_company_name
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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None


load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY", "")
ACCESS_ID = os.getenv("ACCESS_ID", "")
COMPANY = os.getenv("COMPANY", "")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"
DEFAULT_OUT_DIR = "output"
DEFAULT_PAGE_SIZE = 200
DEFAULT_PREVIEW_LIMIT = 50
REQUEST_TIMEOUT = 30
DISPLAY_TIMEZONE_NAME = "Australia/Sydney"

if ZoneInfo is not None:
    try:
        DISPLAY_TIMEZONE = ZoneInfo(DISPLAY_TIMEZONE_NAME)
    except Exception:
        DISPLAY_TIMEZONE = timezone(timedelta(hours=11))
else:
    DISPLAY_TIMEZONE = timezone(timedelta(hours=11))

PREVIEW_REQUIRED_FIELDS = [
    "id",
    "monitorObjectName",
    "resourceTemplateName",
    "instanceName",
    "dataPointName",
    "severity",
    "startEpoch",
    "endEpoch",
    "sdted",
    "SDT",
    "cleared",
    "acked",
    "detailMessage",
]

SINGLE_ALERT_REQUIRED_FIELDS = [
    "id",
    "monitorObjectName",
    "resourceTemplateName",
    "instanceName",
    "dataPointName",
    "severity",
    "startEpoch",
    "endEpoch",
    "sdted",
    "SDT",
    "cleared",
    "acked",
    "ackedBy",
    "rule",
    "chain",
    "detailMessage",
]


class LMAPIError(RuntimeError):
    """Raised when the LogicMonitor API returns an error or invalid response."""


def validate_env() -> None:
    missing = [
        name
        for name, value in {
            "ACCESS_ID": ACCESS_ID,
            "ACCESS_KEY": ACCESS_KEY,
            "COMPANY": COMPANY,
        }.items()
        if not value
    ]
    if missing:
        raise SystemExit(
            "Missing required environment variables in .env: "
            + ", ".join(missing)
        )


def positive_int(value: str) -> int:
    ivalue = int(value)
    if ivalue < 1:
        raise argparse.ArgumentTypeError("Value must be >= 1")
    return ivalue


def normalize_epoch_to_seconds(epoch_value: Any) -> Optional[int]:
    """
    Normalize epoch values that may be provided in:
    - seconds (10 digits)
    - milliseconds (13 digits)
    - microseconds
    - nanoseconds
    """
    if epoch_value in (None, ""):
        return None

    try:
        value = int(float(str(epoch_value).strip()))
    except (TypeError, ValueError):
        return None

    abs_value = abs(value)

    if abs_value >= 1_000_000_000_000_000_000:
        return value // 1_000_000_000
    if abs_value >= 1_000_000_000_000_000:
        return value // 1_000_000
    if abs_value >= 1_000_000_000_000:
        return value // 1_000
    return value


def days_ago_to_epoch_seconds(days: int) -> int:
    return int(time.time()) - (days * 86400)


def format_gmt_offset(dt: datetime) -> str:
    offset = dt.utcoffset()
    if offset is None:
        return "GMT+00:00"

    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"GMT{sign}{hours:02d}:{minutes:02d}"


def epoch_to_display_date(epoch_value: Any) -> str:
    epoch_seconds = normalize_epoch_to_seconds(epoch_value)
    if epoch_seconds is None:
        return ""

    try:
        dt = datetime.fromtimestamp(epoch_seconds, tz=DISPLAY_TIMEZONE)
    except (OSError, OverflowError, ValueError):
        return ""

    hour_12 = dt.hour % 12 or 12
    am_pm = "am" if dt.hour < 12 else "pm"
    gmt_offset = format_gmt_offset(dt)

    return (
        f"{dt.strftime('%A')} "
        f"{dt.day} "
        f"{dt.strftime('%B')} "
        f"{dt.year} "
        f"at {hour_12}:{dt.minute:02d}:{dt.second:02d} "
        f"{am_pm} "
        f"{gmt_offset}"
    )


def seconds_to_duration_string(total_seconds: int) -> str:
    if total_seconds < 0:
        return ""

    days = total_seconds // 86400
    remainder = total_seconds % 86400
    hours = remainder // 3600
    remainder %= 3600
    minutes = remainder // 60
    seconds = remainder % 60

    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")

    return " ".join(parts)


def alert_duration_value(alert: Dict[str, Any]) -> str:
    start_seconds = normalize_epoch_to_seconds(alert.get("startEpoch"))
    if start_seconds is None:
        return ""

    end_seconds = normalize_epoch_to_seconds(alert.get("endEpoch"))
    if end_seconds is None:
        end_seconds = int(time.time())

    duration_seconds = end_seconds - start_seconds
    return seconds_to_duration_string(duration_seconds)


def filter_alerts_since_days(
    alerts: List[Dict[str, Any]], days_ago: Optional[int]
) -> List[Dict[str, Any]]:
    if days_ago is None:
        return alerts

    cutoff_seconds = days_ago_to_epoch_seconds(days_ago)
    filtered: List[Dict[str, Any]] = []

    for alert in alerts:
        alert_start_seconds = normalize_epoch_to_seconds(alert.get("startEpoch"))
        if alert_start_seconds is None:
            continue
        if alert_start_seconds >= cutoff_seconds:
            filtered.append(alert)

    return filtered


def filter_alerts_by_object_name(
    alerts: List[Dict[str, Any]], object_name: str
) -> List[Dict[str, Any]]:
    target = object_name.strip().lower()
    filtered: List[Dict[str, Any]] = []

    for alert in alerts:
        monitor_object_name = str(alert.get("monitorObjectName", "")).strip().lower()
        if monitor_object_name == target:
            filtered.append(alert)

    return filtered


def alert_sdt_value(alert: Dict[str, Any]) -> str:
    sdted = alert.get("sdted")
    if isinstance(sdted, bool):
        return "True" if sdted else "False"
    if sdted not in (None, ""):
        return str(sdted)

    active_sdt = alert.get("SDT")
    if isinstance(active_sdt, dict):
        return "True" if active_sdt else "False"
    if active_sdt not in (None, "", [], {}):
        return "True"

    return ""


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def compact_message_value(detail_message: Any) -> str:
    if detail_message is None:
        return ""

    if isinstance(detail_message, dict):
        subject = detail_message.get("subject")
        body = detail_message.get("body")

        if subject not in (None, ""):
            return normalize_whitespace(str(subject))
        if body not in (None, ""):
            return normalize_whitespace(str(body))

        return normalize_whitespace(
            json.dumps(detail_message, ensure_ascii=False, separators=(",", ":"))
        )

    if isinstance(detail_message, list):
        return normalize_whitespace(
            json.dumps(detail_message, ensure_ascii=False, separators=(",", ":"))
        )

    return normalize_whitespace(str(detail_message))


def full_message_value(detail_message: Any) -> str:
    if detail_message is None:
        return ""

    if isinstance(detail_message, dict):
        subject = detail_message.get("subject")
        body = detail_message.get("body")
        parts: List[str] = []

        if subject not in (None, ""):
            parts.append(f"Subject: {subject}")
        if body not in (None, ""):
            parts.append(f"Body:\n{body}")

        if parts:
            return "\n".join(parts)

        return json.dumps(detail_message, indent=2, ensure_ascii=False)

    if isinstance(detail_message, list):
        return json.dumps(detail_message, indent=2, ensure_ascii=False)

    return str(detail_message)


def message_value_for_table(detail_message: Any, include_full_body: bool = False) -> str:
    if include_full_body:
        return full_message_value(detail_message)
    return compact_message_value(detail_message)


def pretty_message_value(detail_message: Any) -> str:
    if detail_message is None:
        return ""

    if isinstance(detail_message, (dict, list)):
        return json.dumps(detail_message, indent=2, ensure_ascii=False)

    return str(detail_message)


def ensure_required_fields(
    fields: Optional[str], required_fields: List[str]
) -> Optional[str]:
    if not fields:
        return fields

    merged: List[str] = []
    seen_lower = set()

    for part in fields.split(","):
        value = part.strip()
        if value and value.lower() not in seen_lower:
            merged.append(value)
            seen_lower.add(value.lower())

    for field_name in required_fields:
        if field_name.lower() not in seen_lower:
            merged.append(field_name)
            seen_lower.add(field_name.lower())

    return ",".join(merged)


def generate_auth_headers(
    http_verb: str, resource_path: str, data: str = ""
) -> Dict[str, str]:
    """
    Generate LogicMonitor API authentication headers.
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
        "Authorization": auth,
    }


def api_get(
    resource_path: str, params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Perform a GET request to the LogicMonitor API.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise LMAPIError(f"Request failed for {resource_path}: {exc}") from exc

    if not response.ok:
        raise LMAPIError(
            f"GET {resource_path} failed with {response.status_code}: {response.text}"
        )

    try:
        return response.json()
    except ValueError as exc:
        raise LMAPIError(f"GET {resource_path} did not return valid JSON.") from exc


def extract_data(payload: Dict[str, Any]) -> Any:
    """
    LogicMonitor responses are commonly wrapped as {"data": ...}.
    Fall back to the original payload if not wrapped.
    """
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


def save_json(out_dir: str, filename: str, payload: Any) -> None:
    ensure_out_dir(out_dir)
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"Saved: {path}")


def save_text(out_dir: str, filename: str, content: str) -> None:
    ensure_out_dir(out_dir)
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.rstrip() + "\n")
    print(f"Saved: {path}")


def render_ascii_grid(rows: List[List[Any]], headers: List[str]) -> str:
    prepared_headers = ["" if h is None else str(h) for h in headers]
    prepared_rows = [
        ["" if cell is None else str(cell) for cell in row]
        for row in rows
    ]

    def split_lines(value: str) -> List[str]:
        lines = value.splitlines()
        return lines if lines else [""]

    widths = [0] * len(prepared_headers)

    for idx, header in enumerate(prepared_headers):
        widths[idx] = max(widths[idx], max(len(line) for line in split_lines(header)))

    for row in prepared_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], max(len(line) for line in split_lines(cell)))

    def border(fill: str) -> str:
        return "+" + "+".join((fill * (width + 2)) for width in widths) + "+"

    def render_row_multiline(row: List[str]) -> List[str]:
        split_row = [split_lines(cell) for cell in row]
        height = max(len(cell_lines) for cell_lines in split_row)
        rendered_lines: List[str] = []

        for line_index in range(height):
            parts = []
            for col_index, cell_lines in enumerate(split_row):
                value = cell_lines[line_index] if line_index < len(cell_lines) else ""
                parts.append(" " + value.ljust(widths[col_index]) + " ")
            rendered_lines.append("|" + "|".join(parts) + "|")

        return rendered_lines

    lines: List[str] = [border("-")]
    lines.extend(render_row_multiline(prepared_headers))
    lines.append(border("="))

    for row in prepared_rows:
        lines.extend(render_row_multiline(row))
        lines.append(border("-"))

    return "\n".join(lines)


def render_table(data: List[List[Any]], headers: List[str], title: str = "") -> str:
    lines: List[str] = []

    if title:
        lines.append("=" * 155)
        lines.append(title)
        lines.append("=" * 155)

    if not data:
        lines.append("(no data)")
        return "\n".join(lines)

    str_rows = [[("" if cell is None else str(cell)) for cell in row] for row in data]

    if tabulate:
        table_text = tabulate(str_rows, headers=headers, tablefmt="grid")
    else:
        table_text = render_ascii_grid(str_rows, headers)

    lines.append(table_text)
    return "\n".join(lines)


def print_report(report_text: str) -> None:
    print(report_text)


def paged_get_items(
    resource_path: str,
    base_params: Optional[Dict[str, Any]] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch all items from a paginated LM endpoint that uses size/offset.
    Returns: (all_items, last_raw_response)
    """
    all_items: List[Dict[str, Any]] = []
    offset = 0
    last_resp: Dict[str, Any] = {}

    while True:
        params = dict(base_params or {})
        params["size"] = page_size
        params["offset"] = offset

        resp = api_get(resource_path, params=params)
        last_resp = resp

        data = extract_data(resp)
        if not isinstance(data, dict):
            break

        items = data.get("items", [])
        if not isinstance(items, list):
            break

        all_items.extend([item for item in items if isinstance(item, dict)])

        total = data.get("total")
        if isinstance(total, int):
            offset += page_size
            if offset >= total:
                break
        else:
            if len(items) < page_size:
                break
            offset += page_size

    return all_items, last_resp


def enrich_alerts_with_detail_messages(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Populate detailMessage by doing a per-alert lookup.
    Used when object-specific mode requests --need-message.
    """
    enriched: List[Dict[str, Any]] = []

    for alert in alerts:
        alert_id = alert.get("id")
        if not alert_id:
            enriched.append(alert)
            continue

        try:
            detailed_alert = get_alert_by_id(
                alert_id=str(alert_id),
                need_message=True,
                fields="detailMessage",
            )
            merged = dict(alert)
            if isinstance(detailed_alert, dict):
                merged.update(detailed_alert)
            enriched.append(merged)
        except LMAPIError:
            enriched.append(alert)

    return enriched


def format_alert_rows(
    alerts: List[Dict[str, Any]],
    limit: int = DEFAULT_PREVIEW_LIMIT,
    include_full_message_in_table: bool = False,
) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for alert in alerts[:limit]:
        start_epoch = alert.get("startEpoch")
        rows.append(
            [
                alert.get("id"),
                alert.get("monitorObjectName"),
                alert.get("resourceTemplateName"),
                alert.get("instanceName"),
                alert.get("dataPointName"),
                alert.get("severity"),
                start_epoch,
                epoch_to_display_date(start_epoch),
                alert_duration_value(alert),
                alert_sdt_value(alert),
                alert.get("cleared"),
                alert.get("acked"),
                message_value_for_table(
                    alert.get("detailMessage"),
                    include_full_body=include_full_message_in_table,
                ),
            ]
        )
    return rows


def build_alert_preview_report(
    alerts: List[Dict[str, Any]],
    title: str,
    include_full_message_in_table: bool = False,
) -> str:
    headers = [
        "Alert ID",
        "Object",
        "DataSource",
        "Instance",
        "DataPoint",
        "Severity",
        "StartEpoch",
        "Date",
        "Duration",
        "SDT",
        "Cleared",
        "Acked",
        "Message",
    ]

    report_text = render_table(
        format_alert_rows(
            alerts,
            include_full_message_in_table=include_full_message_in_table,
        ),
        headers,
        title,
    )

    if len(alerts) > DEFAULT_PREVIEW_LIMIT:
        report_text += (
            f"\n\nShowing first {DEFAULT_PREVIEW_LIMIT} of {len(alerts)} alerts."
        )

    return report_text


def build_single_alert_report(
    alert: Dict[str, Any],
    include_full_message_in_table: bool = False,
    include_detail_section: bool = True,
) -> str:
    start_epoch = alert.get("startEpoch")
    detail_message = alert.get("detailMessage")

    summary_rows = [
        ["Alert ID", alert.get("id")],
        ["Object", alert.get("monitorObjectName")],
        ["DataSource", alert.get("resourceTemplateName")],
        ["Instance", alert.get("instanceName")],
        ["DataPoint", alert.get("dataPointName")],
        ["Severity", alert.get("severity")],
        ["StartEpoch", start_epoch],
        ["Date", epoch_to_display_date(start_epoch)],
        ["Duration", alert_duration_value(alert)],
        ["SDT", alert_sdt_value(alert)],
        ["EndEpoch", alert.get("endEpoch")],
        ["Cleared", alert.get("cleared")],
        ["Acked", alert.get("acked")],
        ["Acked By", alert.get("ackedBy")],
        ["Rule", alert.get("rule")],
        ["Chain", alert.get("chain")],
        [
            "Message",
            message_value_for_table(
                detail_message,
                include_full_body=include_full_message_in_table,
            ),
        ],
    ]

    report_text = render_table(summary_rows, ["Field", "Value"], "Alert Summary")

    if include_detail_section:
        pretty_message = pretty_message_value(detail_message)
        if pretty_message:
            report_text += f"\n\nDetail Message:\n{pretty_message}"

    return report_text


def get_alerts_accountwide(
    lm_filter: Optional[str] = None,
    fields: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    """
    Capture alerts account-wide.
    """
    params: Dict[str, Any] = {}
    if lm_filter:
        params["filter"] = lm_filter
    if fields:
        params["fields"] = ensure_required_fields(fields, PREVIEW_REQUIRED_FIELDS)

    alerts, _ = paged_get_items("/alert/alerts", base_params=params, page_size=page_size)
    return alerts


def get_alerts_for_object_name(
    object_name: str,
    lm_filter: Optional[str] = None,
    fields: Optional[str] = None,
    need_message: bool = False,
    page_size: int = DEFAULT_PAGE_SIZE,
    days_ago: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Capture alerts for a specific monitorObjectName.
    This uses account-wide alerts and filters client-side by monitorObjectName.
    """
    alerts = get_alerts_accountwide(
        lm_filter=lm_filter,
        fields=fields,
        page_size=page_size,
    )

    alerts = filter_alerts_by_object_name(alerts, object_name)
    alerts = filter_alerts_since_days(alerts, days_ago)

    if need_message and alerts:
        alerts = enrich_alerts_with_detail_messages(alerts)

    return alerts


def get_alert_by_id(
    alert_id: str,
    need_message: bool = False,
    fields: Optional[str] = None,
    custom_columns: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch a single alert by ID.
    """
    params: Dict[str, Any] = {
        "needMessage": str(need_message).lower(),
    }
    if fields:
        params["fields"] = ensure_required_fields(fields, SINGLE_ALERT_REQUIRED_FIELDS)
    if custom_columns:
        params["customColumns"] = custom_columns

    resp = api_get(f"/alert/alerts/{alert_id}", params=params)
    data = extract_data(resp)

    if not isinstance(data, dict):
        raise LMAPIError(f"Unexpected alert payload for alert ID {alert_id}: {data}")

    return data


def build_parser() -> argparse.ArgumentParser:
    main_examples = """Examples:
  Show this help:
    python Get-LMAlerts_v2.py --help

  Account-wide alerts:
    python3 Get-LMAlerts_v2.py account
    python3 Get-LMAlerts_v2.py account --filter "cleared:false"
    python3 Get-LMAlerts_v2.py account --fields "id,severity,monitorObjectName"
    python3 Get-LMAlerts_v2.py account --days-ago 1
    python3 Get-LMAlerts_v2.py account --days-ago 7 --filter "cleared:false"
    python3 Get-LMAlerts_v2.py account --save-table

  Alerts for a specific object name:
    python3 Get-LMAlerts_v2.py object --object-name "HP-Printer"
    python3 Get-LMAlerts_v2.py object --object-name "HP-Printer" --need-message
    python3 Get-LMAlerts_v2.py object --object-name "HP-Printer" --days-ago 7
    python3 Get-LMAlerts_v2.py object --object-name "HP-Printer" --days-ago 14 --need-message --save-table
    python3 Get-LMAlerts_v2.py object --object-name "HP-Printer" --filter "severity:>=3"

  Single alert by ID:
    python3 Get-LMAlerts_v2.py alert --alert-id DS267
    python3 Get-LMAlerts_v2.py alert --alert-id DS267 --need-message
    python3 Get-LMAlerts_v2.py alert --alert-id DS267 --need-message --save-table

Notes:
  --days-ago means "alerts since N days ago"
  Date is shown in Australia/Sydney
  Duration is computed from startEpoch/endEpoch, or startEpoch-to-now for active alerts
  Message is populated when detailMessage is returned by the API
  Object matching uses monitorObjectName and is case-insensitive exact match
  In object mode, --need-message performs one extra alert lookup per matched alert
  --save-table writes the displayed ASCII report to a .text file in the output directory
  When --save-table is used, the saved table includes the full message body when available
  Saved table output does not append a separate Detail Message section
  JSON output is still saved under ./output by default
"""

    parser = argparse.ArgumentParser(
        prog="Get-LMAlerts_v2.py",
        description="Fetch LogicMonitor alerts from the command line.",
        epilog=main_examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    account_examples = """Examples:
  python Get-LMAlerts_v2.py account
  python Get-LMAlerts_v2.py account --filter "cleared:false"
  python Get-LMAlerts_v2.py account --days-ago 7
  python Get-LMAlerts_v2.py account --save-table
"""

    account_parser = subparsers.add_parser(
        "account",
        help="Capture account-wide alerts",
        description="Capture account-wide alerts.",
        epilog=account_examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    account_parser.add_argument(
        "--filter",
        dest="lm_filter",
        help="Optional LogicMonitor filter string",
    )
    account_parser.add_argument(
        "--fields",
        help="Optional fields list",
    )
    account_parser.add_argument(
        "--days-ago",
        type=positive_int,
        help="Only show alerts from the last N days (examples: 1, 7, 14)",
    )
    account_parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Page size for paginated requests (default: {DEFAULT_PAGE_SIZE})",
    )
    account_parser.add_argument(
        "--save-table",
        action="store_true",
        help="Save the displayed ASCII table/report to a .text file",
    )
    account_parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUT_DIR,
        help=f"Directory to save output files (default: {DEFAULT_OUT_DIR})",
    )

    object_examples = """Examples:
  python Get-LMAlerts_v2.py object --object-name "HP-Printer"
  python Get-LMAlerts_v2.py object --object-name "HP-Printer" --need-message
  python Get-LMAlerts_v2.py object --object-name "HP-Printer" --days-ago 7
  python Get-LMAlerts_v2.py object --object-name "HP-Printer" --days-ago 14 --need-message --save-table
"""

    object_parser = subparsers.add_parser(
        "object",
        aliases=["device"],
        help="Capture alerts for a specific object name / monitorObjectName",
        description="Capture alerts for a specific object name / monitorObjectName.",
        epilog=object_examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    object_parser.set_defaults(command="object")
    object_parser.add_argument(
        "--object-name",
        required=True,
        help="Object name to match against monitorObjectName",
    )
    object_parser.add_argument(
        "--filter",
        dest="lm_filter",
        help="Optional LogicMonitor filter string",
    )
    object_parser.add_argument(
        "--fields",
        help="Optional fields list",
    )
    object_parser.add_argument(
        "--need-message",
        action="store_true",
        help="Populate detail message by doing a per-alert lookup",
    )
    object_parser.add_argument(
        "--days-ago",
        type=positive_int,
        help="Only show alerts from the last N days (examples: 1, 7, 14)",
    )
    object_parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Page size for paginated requests (default: {DEFAULT_PAGE_SIZE})",
    )
    object_parser.add_argument(
        "--save-table",
        action="store_true",
        help="Save the displayed ASCII table/report to a .text file",
    )
    object_parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUT_DIR,
        help=f"Directory to save output files (default: {DEFAULT_OUT_DIR})",
    )

    alert_examples = """Examples:
  python Get-LMAlerts_v2.py alert --alert-id DS267
  python Get-LMAlerts_v2.py alert --alert-id DS267 --need-message
  python Get-LMAlerts_v2.py alert --alert-id DS267 --need-message --save-table
"""

    alert_parser = subparsers.add_parser(
        "alert",
        help="Fetch a single alert by alert ID",
        description="Fetch a single alert by alert ID.",
        epilog=alert_examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    alert_parser.add_argument(
        "--alert-id",
        required=True,
        help="Alert ID",
    )
    alert_parser.add_argument(
        "--need-message",
        action="store_true",
        help="Include alert detail message where supported",
    )
    alert_parser.add_argument(
        "--fields",
        help="Optional fields list",
    )
    alert_parser.add_argument(
        "--custom-columns",
        help="Optional customColumns value",
    )
    alert_parser.add_argument(
        "--save-table",
        action="store_true",
        help="Save the displayed ASCII table/report to a .text file",
    )
    alert_parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUT_DIR,
        help=f"Directory to save output files (default: {DEFAULT_OUT_DIR})",
    )

    return parser


def main() -> int:
    validate_env()
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "account":
            alerts = get_alerts_accountwide(
                lm_filter=args.lm_filter,
                fields=args.fields,
                page_size=args.page_size,
            )

            alerts = filter_alerts_since_days(alerts, args.days_ago)

            if alerts:
                title = (
                    f"Account Alerts (showing first {DEFAULT_PREVIEW_LIMIT} of "
                    f"{len(alerts)})"
                )
                if args.days_ago:
                    title += f" - last {args.days_ago} day(s)"
                console_report = build_alert_preview_report(
                    alerts,
                    title,
                    include_full_message_in_table=False,
                )
                file_report = build_alert_preview_report(
                    alerts,
                    title,
                    include_full_message_in_table=True,
                )
            else:
                if args.days_ago:
                    console_report = f"No alerts found in the last {args.days_ago} day(s)."
                else:
                    console_report = "No alerts found."
                file_report = console_report

            print_report(console_report)

            if args.save_table:
                text_filename = "getAlerts_accountwide.text"
                if args.days_ago:
                    text_filename = f"getAlerts_accountwide_{args.days_ago}d.text"
                save_text(args.output_dir, text_filename, file_report)

            json_filename = "getAlerts_accountwide.json"
            if args.days_ago:
                json_filename = f"getAlerts_accountwide_{args.days_ago}d.json"
            save_json(args.output_dir, json_filename, alerts)
            return 0

        if args.command == "object":
            alerts = get_alerts_for_object_name(
                object_name=args.object_name,
                lm_filter=args.lm_filter,
                fields=args.fields,
                need_message=args.need_message,
                page_size=args.page_size,
                days_ago=args.days_ago,
            )

            if alerts:
                title = (
                    f"Object Alerts for {args.object_name} "
                    f"(showing first {DEFAULT_PREVIEW_LIMIT} of {len(alerts)})"
                )
                if args.days_ago:
                    title += f" - last {args.days_ago} day(s)"
                console_report = build_alert_preview_report(
                    alerts,
                    title,
                    include_full_message_in_table=False,
                )
                file_report = build_alert_preview_report(
                    alerts,
                    title,
                    include_full_message_in_table=True,
                )
            else:
                if args.days_ago:
                    console_report = (
                        f"No alerts found for object name {args.object_name} "
                        f"in the last {args.days_ago} day(s)."
                    )
                else:
                    console_report = (
                        f"No alerts found for object name {args.object_name}."
                    )
                file_report = console_report

            print_report(console_report)

            object_slug = sanitize_filename(args.object_name)

            if args.save_table:
                text_filename = f"getAlerts_object_{object_slug}.text"
                if args.days_ago:
                    text_filename = (
                        f"getAlerts_object_{object_slug}_{args.days_ago}d.text"
                    )
                save_text(args.output_dir, text_filename, file_report)

            json_filename = f"getAlerts_object_{object_slug}.json"
            if args.days_ago:
                json_filename = f"getAlerts_object_{object_slug}_{args.days_ago}d.json"
            save_json(args.output_dir, json_filename, alerts)
            return 0

        if args.command == "alert":
            alert = get_alert_by_id(
                alert_id=args.alert_id,
                need_message=args.need_message,
                fields=args.fields,
                custom_columns=args.custom_columns,
            )

            console_report = build_single_alert_report(
                alert,
                include_full_message_in_table=False,
                include_detail_section=True,
            )
            file_report = build_single_alert_report(
                alert,
                include_full_message_in_table=True,
                include_detail_section=False,
            )

            print_report(console_report)

            if args.save_table:
                save_text(
                    args.output_dir,
                    f"getAlert_{sanitize_filename(args.alert_id)}.text",
                    file_report,
                )

            save_json(
                args.output_dir,
                f"getAlert_{sanitize_filename(args.alert_id)}.json",
                alert,
            )
            return 0

        parser.print_help()
        return 1

    except LMAPIError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
