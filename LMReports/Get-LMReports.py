#!/usr/bin/env python3
"""
Get-LMReports.py

LogicMonitor Reports utility.

Features:
  - List all configured reports
  - Execute a report by report ID
  - Retrieve the generated resulturl
  - Download the generated report to an output directory
  - Query an existing execution task
  - Get a report definition
  - Supports normal argparse syntax and key=value syntax

Examples:

  List all reports:
    python Get-LMReports.py --list-all

  Execute report ID 9 and save it to ./output:
    python Get-LMReports.py --getReport 9

  Same using key=value syntax:
    python Get-LMReports.py getReport=9

  Execute report ID 9 and save to a specific folder:
    python Get-LMReports.py --getReport 9 --out ./reports

  Same using key=value syntax:
    python Get-LMReports.py getReport=9 out=./reports

  Query an existing execution task and download the result:
    python Get-LMReports.py --getReport 9 --taskId 7542324844873791160

  Get report definition JSON without executing:
    python Get-LMReports.py --get-definition 9

Requirements:
    pip install requests python-dotenv tabulate

.env file:
    ACCESS_ID=your_access_id
    ACCESS_KEY=your_access_key
    COMPANY=your_logicmonitor_account
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
from datetime import datetime
from urllib.parse import urlencode, unquote

import requests
from dotenv import load_dotenv
from tabulate import tabulate


# ============================================================================
# Configuration
# ============================================================================

load_dotenv()

ACCESS_ID = os.getenv("ACCESS_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
COMPANY = os.getenv("COMPANY")

if not ACCESS_ID:
    print("ERROR: ACCESS_ID is not set in .env", file=sys.stderr)
    sys.exit(1)

if not ACCESS_KEY:
    print("ERROR: ACCESS_KEY is not set in .env", file=sys.stderr)
    sys.exit(1)

if not COMPANY:
    print("ERROR: COMPANY is not set in .env", file=sys.stderr)
    sys.exit(1)


BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"

REPORT_LIST_ENDPOINT = "/report/reports"
REPORT_ENDPOINT = "/report/reports/{id}"
REPORT_EXECUTION_ENDPOINT = "/report/reports/{id}/executions"
REPORT_TASK_ENDPOINT = "/report/reports/{id}/tasks/{taskId}"

DEFAULT_PAGE_SIZE = 1000
DEFAULT_RETRIES = 5
DEFAULT_TIMEOUT = 120
DEFAULT_OUTPUT_DIR = "./output"


# ============================================================================
# LogicMonitor LMv1 Authentication
# ============================================================================

def build_auth_headers(http_verb, resource_path, data=""):
    """
    Build LogicMonitor LMv1 authentication headers.
    """

    epoch = str(int(time.time() * 1000))

    request_vars = (
        http_verb.upper()
        + epoch
        + data
        + resource_path
    )

    digest = hmac.new(
        ACCESS_KEY.encode("utf-8"),
        msg=request_vars.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    signature = base64.b64encode(
        digest.encode("utf-8")
    ).decode("utf-8")

    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"

    return {
        "Authorization": auth,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Version": "3",
        "User-Agent": "Get-LMReports.py/1.1",
    }


# ============================================================================
# LogicMonitor API Request
# ============================================================================

def api_request(
    http_verb,
    resource_path,
    params=None,
    payload=None,
    retries=DEFAULT_RETRIES,
):
    """
    Perform an authenticated LogicMonitor API request.
    """

    params = params or {}

    query_string = urlencode(params, doseq=True)

    url = f"{BASE_URL}{resource_path}"

    if query_string:
        url = f"{url}?{query_string}"

    if payload is None:
        request_body = ""
    else:
        request_body = json.dumps(
            payload,
            separators=(",", ":"),
        )

    for attempt in range(1, retries + 1):

        headers = build_auth_headers(
            http_verb,
            resource_path,
            request_body,
        )

        try:
            response = requests.request(
                method=http_verb,
                url=url,
                headers=headers,
                data=request_body if request_body else None,
                timeout=DEFAULT_TIMEOUT,
            )

        except requests.RequestException as exc:

            print(
                f"Request failed "
                f"(attempt {attempt}/{retries}): {exc}",
                file=sys.stderr,
            )

            if attempt == retries:
                raise

            time.sleep(attempt * 2)
            continue

        # ------------------------------------------------------------------
        # Rate limiting
        # ------------------------------------------------------------------

        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            try:
                wait_time = int(retry_after)
            except (TypeError, ValueError):
                wait_time = attempt * 5

            print(
                f"HTTP 429 - rate limited. "
                f"Waiting {wait_time} seconds...",
                file=sys.stderr,
            )

            time.sleep(wait_time)
            continue

        # ------------------------------------------------------------------
        # Server errors
        # ------------------------------------------------------------------

        if response.status_code >= 500:

            print(
                f"HTTP {response.status_code} "
                f"(attempt {attempt}/{retries})",
                file=sys.stderr,
            )

            if attempt == retries:
                response.raise_for_status()

            time.sleep(attempt * 2)
            continue

        # ------------------------------------------------------------------
        # Other HTTP errors
        # ------------------------------------------------------------------

        if not response.ok:

            print(
                "\nLogicMonitor API request failed:",
                file=sys.stderr,
            )

            print(
                f"  Method: {http_verb}",
                file=sys.stderr,
            )

            print(
                f"  URL:    {url}",
                file=sys.stderr,
            )

            print(
                f"  Status: {response.status_code}",
                file=sys.stderr,
            )

            print(
                f"  Body:   {response.text}",
                file=sys.stderr,
            )

            response.raise_for_status()

        return response

    raise RuntimeError(
        f"Request failed after {retries} attempts: "
        f"{http_verb} {resource_path}"
    )


def api_get_json(resource_path, params=None):

    response = api_request(
        "GET",
        resource_path,
        params=params,
    )

    try:
        return response.json()

    except ValueError:
        print(
            "ERROR: LogicMonitor did not return JSON.",
            file=sys.stderr,
        )

        print(response.text, file=sys.stderr)
        raise


def api_post_json(resource_path, payload=None):

    response = api_request(
        "POST",
        resource_path,
        payload=payload,
    )

    try:
        return response.json()

    except ValueError:
        print(
            "ERROR: LogicMonitor did not return JSON.",
            file=sys.stderr,
        )

        print(response.text, file=sys.stderr)
        raise


# ============================================================================
# Response Helpers
# ============================================================================

def extract_items(response_json):
    """
    Extract list of objects from common LogicMonitor API response formats.
    """

    if isinstance(response_json, list):
        return response_json

    if not isinstance(response_json, dict):
        return []

    data = response_json.get("data")

    if isinstance(data, dict):

        if isinstance(data.get("items"), list):
            return data["items"]

        if isinstance(data.get("data"), list):
            return data["data"]

    if isinstance(data, list):
        return data

    if isinstance(response_json.get("items"), list):
        return response_json["items"]

    return []


def extract_result_object(response_json):
    """
    Extract execution result from either:

        {
            "reportId": 9,
            "taskId": "...",
            "resulturl": "..."
        }

    or:

        {
            "data": {
                "reportId": 9,
                "taskId": "...",
                "resulturl": "..."
            }
        }
    """

    if not isinstance(response_json, dict):
        return {}

    data = response_json.get("data")

    if isinstance(data, dict):
        return data

    return response_json


# ============================================================================
# Report API Functions
# ============================================================================

def list_all_reports(
    page_size=DEFAULT_PAGE_SIZE,
    api_filter=None,
):
    """
    Retrieve all configured LogicMonitor reports.
    """

    all_reports = []
    offset = 0

    while True:

        params = {
            "size": page_size,
            "offset": offset,
        }

        if api_filter:
            params["filter"] = api_filter

        result = api_get_json(
            REPORT_LIST_ENDPOINT,
            params=params,
        )

        items = extract_items(result)

        if not items:
            break

        all_reports.extend(items)

        if len(items) < page_size:
            break

        offset += page_size

    return all_reports


def get_report_definition(report_id):
    """
    Get report definition JSON.
    """

    resource_path = REPORT_ENDPOINT.format(
        id=report_id
    )

    return api_get_json(resource_path)


def execute_report(report_id):
    """
    Execute a LogicMonitor report.

    POST:
        /report/reports/{id}/executions

    Payload:
        {
            "withAdminId": 0
        }
    """

    resource_path = REPORT_EXECUTION_ENDPOINT.format(
        id=report_id
    )

    payload = {
        "withAdminId": 0
    }

    return api_post_json(
        resource_path,
        payload=payload,
    )


def get_report_task(report_id, task_id):
    """
    Retrieve an existing report execution task.

    GET:
        /report/reports/{id}/tasks/{taskId}
    """

    resource_path = REPORT_TASK_ENDPOINT.format(
        id=report_id,
        taskId=task_id,
    )

    return api_get_json(resource_path)


# ============================================================================
# Filename Helpers
# ============================================================================

def sanitize_filename(filename):
    """
    Remove characters that are unsafe in filenames.
    """

    filename = unquote(filename)

    filename = filename.replace("\\", "_")
    filename = filename.replace("/", "_")

    filename = re.sub(
        r'[<>:"|?*]',
        "_",
        filename,
    )

    filename = filename.strip(" .")

    if not filename:
        filename = "report"

    return filename


def filename_from_content_disposition(header):
    """
    Extract filename from Content-Disposition.
    """

    if not header:
        return None

    # RFC 5987 style:
    # filename*=UTF-8''something.csv

    match = re.search(
        r"filename\*=UTF-8''([^;]+)",
        header,
        flags=re.IGNORECASE,
    )

    if match:
        return sanitize_filename(
            unquote(match.group(1))
        )

    # Standard:
    # filename="something.csv"

    match = re.search(
        r'filename="?([^";]+)"?',
        header,
        flags=re.IGNORECASE,
    )

    if match:
        return sanitize_filename(
            match.group(1)
        )

    return None


def extension_from_content_type(content_type):
    """
    Guess report extension from HTTP Content-Type.
    """

    content_type = (
        content_type
        or ""
    ).lower()

    if "text/csv" in content_type:
        return ".csv"

    if "application/csv" in content_type:
        return ".csv"

    if "application/pdf" in content_type:
        return ".pdf"

    if "spreadsheetml" in content_type:
        return ".xlsx"

    if "application/vnd.ms-excel" in content_type:
        return ".xls"

    if "text/html" in content_type:
        return ".html"

    if "application/json" in content_type:
        return ".json"

    if "text/plain" in content_type:
        return ".txt"

    return ".dat"


# ============================================================================
# Download Generated Report
# ============================================================================

def download_report(
    result_url,
    output_dir,
    report_id,
    task_id=None,
):
    """
    Download a generated report from resulturl.

    The resulturl returned by the executions endpoint is used directly.

    Redirects are followed automatically.
    """

    if not result_url:
        raise ValueError(
            "No resulturl was supplied."
        )

    os.makedirs(
        output_dir,
        exist_ok=True,
    )

    print()
    print("Downloading generated report...")
    print(f"URL       : {result_url}")
    print(f"Output dir: {os.path.abspath(output_dir)}")

    try:

        response = requests.get(
            result_url,
            stream=True,
            allow_redirects=True,
            timeout=DEFAULT_TIMEOUT,
            headers={
                "User-Agent": "Get-LMReports.py/1.1"
            },
        )

    except requests.RequestException as exc:

        raise RuntimeError(
            f"Unable to download generated report: {exc}"
        ) from exc

    if not response.ok:

        print(
            f"Download failed: HTTP {response.status_code}",
            file=sys.stderr,
        )

        print(
            response.text[:2000],
            file=sys.stderr,
        )

        response.raise_for_status()

    # ----------------------------------------------------------------------
    # Determine filename
    # ----------------------------------------------------------------------

    content_disposition = response.headers.get(
        "Content-Disposition"
    )

    filename = filename_from_content_disposition(
        content_disposition
    )

    if not filename:

        content_type = response.headers.get(
            "Content-Type",
            "",
        )

        extension = extension_from_content_type(
            content_type
        )

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        if task_id:

            filename = (
                f"report_{report_id}_"
                f"{task_id}_"
                f"{timestamp}"
                f"{extension}"
            )

        else:

            filename = (
                f"report_{report_id}_"
                f"{timestamp}"
                f"{extension}"
            )

    filename = sanitize_filename(filename)

    output_path = os.path.join(
        output_dir,
        filename,
    )

    # ----------------------------------------------------------------------
    # Avoid accidentally overwriting an existing file
    # ----------------------------------------------------------------------

    if os.path.exists(output_path):

        base, extension = os.path.splitext(
            output_path
        )

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        output_path = (
            f"{base}_{timestamp}{extension}"
        )

    # ----------------------------------------------------------------------
    # Save file
    # ----------------------------------------------------------------------

    with open(output_path, "wb") as file_handle:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):

            if chunk:
                file_handle.write(chunk)

    file_size = os.path.getsize(
        output_path
    )

    print()
    print("=" * 80)
    print("Report downloaded successfully")
    print("=" * 80)
    print(f"File : {os.path.abspath(output_path)}")
    print(f"Size : {file_size:,} bytes")
    print("=" * 80)

    return output_path


# ============================================================================
# Execute and Download
# ============================================================================

def execute_and_download_report(
    report_id,
    output_dir,
):
    """
    Execute report, obtain resulturl, and download generated report.
    """

    print(f"Executing LogicMonitor report ID {report_id}...")

    result = execute_report(
        report_id
    )

    obj = extract_result_object(
        result
    )

    returned_report_id = (
        obj.get("reportId")
        or report_id
    )

    task_id = (
        obj.get("taskId")
        or obj.get("task_id")
    )

    result_url = (
        obj.get("resulturl")
        or obj.get("resultUrl")
        or obj.get("resultURL")
        or obj.get("result_url")
    )

    print()
    print(f"Report ID : {returned_report_id}")

    if task_id:
        print(f"Task ID   : {task_id}")

    if result_url:
        print(f"Result URL: {result_url}")

    if not result_url:

        print()
        print(
            "ERROR: Report execution did not return a resulturl.",
            file=sys.stderr,
        )

        print()
        print("Full API response:")
        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return None

    return download_report(
        result_url=result_url,
        output_dir=output_dir,
        report_id=returned_report_id,
        task_id=task_id,
    )


# ============================================================================
# Existing Task Download
# ============================================================================

def download_existing_task(
    report_id,
    task_id,
    output_dir,
):
    """
    Query an existing task and download its generated report.
    """

    print(
        f"Querying report {report_id}, "
        f"task {task_id}..."
    )

    result = get_report_task(
        report_id,
        task_id,
    )

    obj = extract_result_object(
        result
    )

    result_url = (
        obj.get("resulturl")
        or obj.get("resultUrl")
        or obj.get("resultURL")
        or obj.get("result_url")
    )

    if not result_url:

        print(
            "ERROR: Task response did not contain a resulturl.",
            file=sys.stderr,
        )

        print()
        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return None

    print(f"Result URL: {result_url}")

    return download_report(
        result_url=result_url,
        output_dir=output_dir,
        report_id=report_id,
        task_id=task_id,
    )


# ============================================================================
# Display Functions
# ============================================================================

def print_report_list(reports):

    if not reports:
        print("No reports found.")
        return

    rows = []

    for report in reports:

        rows.append([
            report.get("id", ""),
            report.get("name", ""),
            report.get("type", ""),
            report.get("format", ""),
            report.get("reportLinkNum", ""),
            report.get("lastGenerateOn", ""),
        ])

    headers = [
        "ID",
        "Name",
        "Type",
        "Format",
        "History Count",
        "Last Generated",
    ]

    print(
        tabulate(
            rows,
            headers=headers,
            tablefmt="simple",
        )
    )

    print()
    print(f"Total reports: {len(reports)}")


def print_json(result):

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=False,
        )
    )


# ============================================================================
# Argument Compatibility
# ============================================================================

def normalise_arguments(argv):
    """
    Support both argparse style:

        --getReport 9 --out ./reports

    and key=value style:

        getReport=9 out=./reports
    """

    converted = []

    for arg in argv:

        if arg.startswith("getReport="):

            converted.extend([
                "--getReport",
                arg.split("=", 1)[1],
            ])

        elif arg.startswith("taskId="):

            converted.extend([
                "--taskId",
                arg.split("=", 1)[1],
            ])

        elif arg.startswith("out="):

            converted.extend([
                "--out",
                arg.split("=", 1)[1],
            ])

        elif arg.startswith("size="):

            converted.extend([
                "--size",
                arg.split("=", 1)[1],
            ])

        elif arg.startswith("filter="):

            converted.extend([
                "--filter",
                arg.split("=", 1)[1],
            ])

        elif arg.startswith("getDefinition="):

            converted.extend([
                "--get-definition",
                arg.split("=", 1)[1],
            ])

        elif arg in (
            "list-all",
            "listAll=true",
            "listAll",
        ):

            converted.append(
                "--list-all"
            )

        else:

            converted.append(arg)

    return converted


# ============================================================================
# Main
# ============================================================================

def main():

    argv = normalise_arguments(
        sys.argv[1:]
    )

    parser = argparse.ArgumentParser(
        description=(
            "List, execute and download LogicMonitor reports."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  List all configured reports:

    python Get-LMReports.py --list-all


  Execute report ID 9 and download to ./output:

    python Get-LMReports.py --getReport 9

    python Get-LMReports.py getReport=9


  Execute report ID 9 and download to ./reports:

    python Get-LMReports.py --getReport 9 --out ./reports

    python Get-LMReports.py getReport=9 out=./reports


  Execute report ID 9 and download to C:\\temp\\reports:

    python Get-LMReports.py --getReport 9 --out "C:\\temp\\reports"


  Download an existing execution task:

    python Get-LMReports.py --getReport 9 --taskId 7542324844873791160

    python Get-LMReports.py getReport=9 taskId=7542324844873791160


  Get report definition JSON:

    python Get-LMReports.py --get-definition 9

    python Get-LMReports.py getDefinition=9
"""
    )

    parser.add_argument(
        "--list-all",
        action="store_true",
        help="List all configured LogicMonitor reports.",
    )

    parser.add_argument(
        "--getReport",
        type=int,
        metavar="REPORT_ID",
        help=(
            "Execute the specified report and "
            "download the generated report."
        ),
    )

    parser.add_argument(
        "--taskId",
        metavar="TASK_ID",
        help=(
            "Download an existing report execution task. "
            "Used with --getReport."
        ),
    )

    parser.add_argument(
        "--get-definition",
        type=int,
        metavar="REPORT_ID",
        help=(
            "Get report configuration JSON "
            "without executing the report."
        ),
    )

    parser.add_argument(
        "--out",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIRECTORY",
        help=(
            "Directory where generated reports are saved. "
            f"Default: {DEFAULT_OUTPUT_DIR}"
        ),
    )

    parser.add_argument(
        "--size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=(
            "API page size when listing reports. "
            f"Default: {DEFAULT_PAGE_SIZE}"
        ),
    )

    parser.add_argument(
        "--filter",
        dest="api_filter",
        help="LogicMonitor API filter.",
    )

    args = parser.parse_args(argv)

    # -----------------------------------------------------------------------
    # List reports
    # -----------------------------------------------------------------------

    if args.list_all:

        reports = list_all_reports(
            page_size=args.size,
            api_filter=args.api_filter,
        )

        print_report_list(
            reports
        )

        return

    # -----------------------------------------------------------------------
    # Get report definition
    # -----------------------------------------------------------------------

    if args.get_definition is not None:

        result = get_report_definition(
            args.get_definition
        )

        print_json(
            result
        )

        return

    # -----------------------------------------------------------------------
    # Execute/download report
    # -----------------------------------------------------------------------

    if args.getReport is not None:

        # Existing task
        if args.taskId:

            download_existing_task(
                report_id=args.getReport,
                task_id=args.taskId,
                output_dir=args.out,
            )

            return

        # Execute a new report and download it
        execute_and_download_report(
            report_id=args.getReport,
            output_dir=args.out,
        )

        return

    parser.print_help()


if __name__ == "__main__":
    main()
