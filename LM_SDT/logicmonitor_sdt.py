#!/usr/bin/env python3
"""Create, list, and cancel LogicMonitor Scheduled Downtimes (SDTs).

Credentials come from LM_COMPANY, LM_ACCESS_ID, and LM_ACCESS_KEY, or the
corresponding command-line options.  CSV rows use the same names as the API
payload (for example: type, deviceId, startDateTime, endDateTime, comment).
"""

import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

import requests
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


API_PATH = "/sdt/sdts"
INT_FIELDS = {"sdtType", "deviceId", "deviceGroupId", "startDateTime", "endDateTime",
              "weekDay", "monthDay", "hour", "minute", "endHour", "endMinute", "duration"}


def positive_ids(values: Iterable[str]) -> List[int]:
    result = []
    for value in values:
        try:
            number = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid ID: {value}") from exc
        if number <= 0:
            raise argparse.ArgumentTypeError(f"ID must be positive: {value}")
        result.append(number)
    return result


def positive_id(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("ID must be positive")
    return number


def parse_args() -> argparse.Namespace:
    # Accept the compact forms shown in the usage examples.
    raw = sys.argv[1:]
    if raw and raw[0].lower() in {"-id", "--id"}:
        sys.argv[1:1] = ["set-device"]
    elif raw and raw[0].lower() in {"remove-sdt", "remove_sdt"}:
        sys.argv[1:1] = ["cancel-sdt"]
    parser = argparse.ArgumentParser(
        description="Manage LogicMonitor device and device-group SDTs",
        epilog="""Examples:
  %(prog)s -Id 123 -StartDate now -EndDate "+1hr" -Comment "1 hr SDT"
  %(prog)s -Id 123 -StartDate "2024-01-01 00:00" -EndDate "2024-01-02 00:00" -Comment "Extended maintenance"
  %(prog)s -Id 123 -SdtType Weekly -StartHour 13 -StartMinute 7 -EndHour 14 -EndMinute 7 -WeekDay Monday,Thursday
  %(prog)s set-group 456 --start now --end "+1day" --comment "Group maintenance"
  %(prog)s set-name "server-01" --start now --end "+1hr" --comment "Device maintenance"
  %(prog)s set-csv maintenance.csv
  %(prog)s show-sdt --device-id 123
  %(prog)s Remove-SDT -Id 12345

Credentials are read from COMPANY, ACCESS_ID, and ACCESS_KEY in .env.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--company", default=os.getenv("COMPANY", os.getenv("LM_COMPANY")))
    parser.add_argument("--access-id", default=os.getenv("ACCESS_ID", os.getenv("LM_ACCESS_ID")))
    parser.add_argument("--access-key", default=os.getenv("ACCESS_KEY", os.getenv("LM_ACCESS_KEY")))
    parser.add_argument("--delay", type=float, default=0.0, help="seconds between API calls")
    parser.add_argument("--debug", action="store_true", help="show request and response diagnostics")
    sub = parser.add_subparsers(dest="command", required=True)

    def schedule(p: argparse.ArgumentParser) -> None:
        p.add_argument("--start", "-StartDate", dest="startDateTime", help="now, +1hr, +1day, or YYYY-MM-DD HH:MM")
        p.add_argument("--end", "-EndDate", dest="endDateTime", help="now, +1hr, +1day, or YYYY-MM-DD HH:MM")
        p.add_argument("--sdt-type", "-SdtType", dest="sdtType", default="OneTime")
        p.add_argument("--comment", "-Comment", default="")
        p.add_argument("--week-day", "-WeekDay", dest="weekDay")
        p.add_argument("--month-day", dest="monthDay", type=int, choices=range(1, 32))
        p.add_argument("--hour", "--start-hour", "-StartHour", dest="hour", type=int, choices=range(24))
        p.add_argument("--minute", "--start-minute", "-StartMinute", dest="minute", type=int, choices=range(60))
        p.add_argument("--end-hour", "-EndHour", dest="endHour", type=int, choices=range(24))
        p.add_argument("--end-minute", "-EndMinute", dest="endMinute", type=int, choices=range(60))
        p.add_argument("--duration", type=int)

    p = sub.add_parser("set-group", help="set SDT for device group IDs")
    p.add_argument("group_ids", nargs="+", type=positive_id)
    schedule(p)
    p = sub.add_parser("set-device", help="set SDT for device IDs")
    p.add_argument("device_ids", nargs="*", type=positive_id)
    p.add_argument("--id", "-Id", dest="option_ids", nargs="+")
    schedule(p)
    p = sub.add_parser("set-name", help="set SDT for a device display name")
    p.add_argument("deviceDisplayName")
    schedule(p)
    p = sub.add_parser("set-csv", help="set SDTs from a CSV file")
    p.add_argument("csv_file")
    p = sub.add_parser("list-sdt", aliases=["show-sdt"], help="list SDTs")
    p.add_argument("--type", choices=["DeviceSDT", "DeviceGroupSDT"])
    p.add_argument("--device-id", type=int)
    p.add_argument("--group-id", type=int)
    p.add_argument("--size", type=int, default=1000)
    p.add_argument("--json", action="store_true", dest="as_json")
    p = sub.add_parser("cancel-sdt", aliases=["remove-sdt"], help="cancel SDTs by ID")
    p.add_argument("sdt_ids", nargs="*", help="SDT IDs such as DS_123")
    p.add_argument("--id", "-Id", dest="option_ids", nargs="+")
    return parser.parse_args()


class LMClient:
    def __init__(self, args: argparse.Namespace):
        if not all([args.company, args.access_id, args.access_key]):
            raise ValueError("set LM_COMPANY, LM_ACCESS_ID, and LM_ACCESS_KEY or pass credential options")
        self.company, self.access_id, self.access_key = args.company, args.access_id, args.access_key
        self.delay = args.delay
        self.debug = args.debug
        if self.debug:
            print(f"DEBUG credentials: company={self.company!r}, access_id_set={bool(self.access_id)}, access_key_set={bool(self.access_key)}", file=sys.stderr)

    def request(self, method: str, path: str, payload: Any = None) -> Dict[str, Any]:
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        epoch = str(int(time.time() * 1000))
        resource_path = path.split("?", 1)[0]
        digest = hmac.new(self.access_key.encode(), (method + epoch + body + resource_path).encode(), hashlib.sha256).hexdigest()
        signature = base64.b64encode(digest.encode()).decode()
        headers = {"Content-Type": "application/json", "Authorization": f"LMv1 {self.access_id}:{signature}:{epoch}"}
        if self.debug:
            print(f"DEBUG request: {method} {path}", file=sys.stderr)
            print(f"DEBUG body: {body or '<empty>'}", file=sys.stderr)
        if self.delay:
            time.sleep(self.delay)
        response = requests.request(method, f"https://{self.company}.logicmonitor.com/santaba/rest{path}", headers=headers, data=body or None, timeout=30)
        if self.debug:
            print(f"DEBUG response: HTTP {response.status_code}", file=sys.stderr)
            print(f"DEBUG response body: {response.text}", file=sys.stderr)
        try:
            result = response.json() if response.content else {}
        except ValueError:
            result = {"content": response.text}
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"{method} {path} failed ({response.status_code}): {json.dumps(result)}")
        if isinstance(result, dict) and result.get("status") not in (None, 0, 200):
            raise RuntimeError(f"{method} {path} failed (API {result.get('status')}): {result.get('errmsg', result)}")
        return result


def payload_from_args(args: argparse.Namespace, target: Dict[str, Any]) -> Dict[str, Any]:
    recurrence = {"onetime": 1, "one-time": 1, "weekly": 2, "monthly": 3, "daily": 4}
    sdt_type = recurrence.get(str(args.sdtType).strip().lower(), args.sdtType)
    try:
        sdt_type = int(sdt_type)
    except (TypeError, ValueError) as exc:
        raise ValueError("SdtType must be OneTime, Weekly, Monthly, Daily, or 1-4") from exc
    data = {"type": target.pop("type"), "sdtType": sdt_type, "comment": args.comment, **target}
    for key in INT_FIELDS - {"sdtType"}:
        value = getattr(args, key, None)
        if value is not None:
            data[key] = value
    if sdt_type == 1:
        data["startDateTime"] = parse_datetime(data.get("startDateTime"))
        data["endDateTime"] = parse_datetime(data.get("endDateTime"))
    if sdt_type == 1 and (data.get("startDateTime") is None or data.get("endDateTime") is None):
        raise ValueError("one-time SDTs require --start and --end epoch milliseconds")
    if sdt_type == 3 and "monthDay" not in data:
        raise ValueError("monthly SDTs require --month-day")
    if isinstance(data.get("weekDay"), str):
        days = {"sunday": 1, "monday": 2, "tuesday": 3, "wednesday": 4, "thursday": 5, "friday": 6, "saturday": 7}
        try:
            data["weekDay"] = [days[d.strip().lower()] for d in data["weekDay"].split(",")]
        except KeyError as exc:
            raise ValueError(f"invalid weekday: {exc.args[0]}") from exc
    return data


def parse_datetime(value: Any) -> Any:
    if value is None or isinstance(value, int): return value
    text = str(value).strip().lower()
    now = datetime.now(timezone.utc)
    if text == "now": return int(now.timestamp() * 1000)
    if text.startswith("+"):
        if text.endswith("day"):
            amount, unit = text[1:-3], "day"
        elif text.endswith("hr"):
            amount, unit = text[1:-2], "hr"
        else:
            amount, unit = "", ""
        if amount.isdigit() and unit in {"hr", "day"}:
            hours = int(amount) if unit == "hr" else 24 * int(amount)
            return int((now + timedelta(hours=hours)).timestamp() * 1000)
    try:
        return int(datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError as exc:
        raise ValueError(f"invalid date: {value}") from exc


def print_sdt_table(result: Dict[str, Any]) -> None:
    data = result.get("data", result) if isinstance(result, dict) else result
    items = data.get("items", []) if isinstance(data, dict) else []
    if not items:
        print("No SDTs found.")
        return
    headers = ["ID", "Device", "Device ID", "Start (local)", "End (local)", "Comment", "Admin", "Type", "Effective"]
    rows = [[item.get("id"), item.get("deviceDisplayName", item.get("deviceGroupName", "")),
             item.get("deviceId", item.get("deviceGroupId", "")), item.get("startDateTimeOnLocal", ""),
             item.get("endDateTimeOnLocal", ""), item.get("comment", ""), item.get("admin", ""),
             item.get("sdtType", item.get("type", "")), item.get("isEffective", "")] for item in items]
    if tabulate:
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print("\t".join(headers))
        for row in rows:
            print("\t".join(str(value) for value in row))


def csv_value(key: str, value: str) -> Any:
    if key in INT_FIELDS and value != "":
        return int(value)
    return value


def main() -> int:
    args = parse_args()
    try:
        client = LMClient(args)
        if args.command.startswith("set-"):
            rows = []
            if args.command == "set-group":
                rows = [payload_from_args(args, {"type": "DeviceGroupSDT", "deviceGroupId": i, "dataSourceId": 0}) for i in args.group_ids]
            elif args.command == "set-device":
                ids = [positive_id(i) for i in (args.device_ids + (args.option_ids or []))]
                if not ids:
                    raise ValueError("set-device requires at least one device ID")
                rows = [payload_from_args(args, {"type": "DeviceSDT", "deviceId": i}) for i in ids]
            elif args.command == "set-name":
                rows = [payload_from_args(args, {"type": "DeviceSDT", "deviceDisplayName": args.deviceDisplayName})]
            else:
                with open(args.csv_file, newline="", encoding="utf-8-sig") as handle:
                    for row in csv.DictReader(handle):
                        row = {k: csv_value(k, v.strip()) for k, v in row.items() if v is not None and v.strip() != ""}
                        if "type" not in row:
                            raise ValueError("CSV requires a type column")
                        row.setdefault("sdtType", 1)
                        if row["sdtType"] == 1 and not {"startDateTime", "endDateTime"}.issubset(row):
                            raise ValueError("CSV one-time rows require startDateTime and endDateTime")
                        if row["sdtType"] == 3 and "monthDay" not in row:
                            raise ValueError("CSV monthly rows require monthDay")
                        rows.append(row)
            for row in rows:
                result = client.request("POST", API_PATH, row)
                print(json.dumps(result.get("data", result), default=str))
        elif args.command in {"list-sdt", "show-sdt"}:
            if args.device_id:
                path = f"/device/devices/{args.device_id}/sdts?size={args.size}&offset=0&sort=-endDateTime&fields=id,startDateTimeOnLocal,endDateTimeOnLocal,comment,admin,type,sdtType,deviceId,deviceDisplayName"
            elif args.group_id:
                path = f"/device/groups/{args.group_id}/sdts?size={args.size}&offset=0&sort=-endDateTime"
            else:
                query = [f"size={args.size}", "offset=0", "sort=-endDateTime"]
                if args.type: query.append(f"filter=type:{args.type}")
                path = API_PATH + "?" + "&".join(query)
            result = client.request("GET", path)
            if args.as_json:
                print(json.dumps(result, indent=2, default=str))
            else:
                print_sdt_table(result)
        else:
            sdt_ids = args.sdt_ids + (args.option_ids or [])
            if not sdt_ids:
                raise ValueError("cancel-sdt requires at least one SDT ID")
            for sdt_id in sdt_ids:
                client.request("DELETE", f"{API_PATH}/{sdt_id}")
                print(f"Cancelled {sdt_id}")
        return 0
    except (OSError, ValueError, RuntimeError, requests.RequestException) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
