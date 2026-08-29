# Get-LMReports.py

`Get-LMReports.py` is a Python utility for working with LogicMonitor Reports through the LogicMonitor REST API v3.

It can:

- List all configured LogicMonitor reports
- Execute a report by report ID
- Retrieve the generated `resulturl`
- Download the generated report to a local output directory
- Query an existing report execution task
- Retrieve the JSON definition of a report
- Accept both standard command-line arguments and simplified `key=value` syntax

## Requirements

- Python 3.8+
- LogicMonitor API credentials with permission to access and run reports

Install the required Python packages:

```bash
pip install requests python-dotenv tabulate
```

## Configuration

Create a `.env` file in the same directory as `Get-LMReports.py`:

```env
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_logicmonitor_account
```

For example:

```env
ACCESS_ID=abc123
ACCESS_KEY=your_api_access_key
COMPANY=example
```

The portal URL is built automatically as:

```text
https://<COMPANY>.logicmonitor.com
```

## Usage

### Show Help

```bash
python Get-LMReports.py --help
```

### List All Reports

```bash
python Get-LMReports.py --list-all
```

Simplified syntax:

```bash
python Get-LMReports.py list-all
```

The output includes:

- Report ID
- Name
- Type
- Format
- History count
- Last generated time

### Filter the Report List

```bash
python Get-LMReports.py --list-all --filter 'name~"Resource Inventory"'
```

### Execute a Report and Download It

Execute report ID `9` and save the generated file to the default `./output` directory:

```bash
python Get-LMReports.py --getReport 9
```

Simplified syntax:

```bash
python Get-LMReports.py getReport=9
```

The script performs:

```text
POST /santaba/rest/report/reports/9/executions
```

with the request body:

```json
{
  "withAdminId": 0
}
```

The API response contains values similar to:

```json
{
  "reportId": 9,
  "taskId": "7542324844873791160",
  "resulturl": "https://example.logicmonitor.com/santaba/rest/report/reports/view?..."
}
```

The script then downloads the file from `resulturl`.

### Save to a Different Output Directory

Linux/macOS:

```bash
python Get-LMReports.py --getReport 9 --out ./reports
```

Simplified syntax:

```bash
python Get-LMReports.py getReport=9 out=./reports
```

Windows PowerShell:

```powershell
python .\Get-LMReports.py getReport=9 out="C:\Temp\LMReports"
```

If the directory does not exist, it is created automatically.

### Download an Existing Execution Task

If you already know the report ID and task ID:

```bash
python Get-LMReports.py --getReport 9 --taskId 7542324844873791160
```

Simplified syntax:

```bash
python Get-LMReports.py getReport=9 taskId=7542324844873791160
```

This queries:

```text
GET /santaba/rest/report/reports/9/tasks/7542324844873791160
```

The returned `resulturl` is then downloaded to the output folder.

### Get a Report Definition

To retrieve the report configuration JSON without running the report:

```bash
python Get-LMReports.py --get-definition 9
```

Simplified syntax:

```bash
python Get-LMReports.py getDefinition=9
```

This queries:

```text
GET /santaba/rest/report/reports/9
```

## Command-Line Options

| Option | Description |
|---|---|
| `--list-all` | List all configured LogicMonitor reports |
| `--getReport REPORT_ID` | Execute a report and download the generated output |
| `--taskId TASK_ID` | Query an existing execution task; used with `--getReport` |
| `--get-definition REPORT_ID` | Return the report definition JSON without executing it |
| `--out DIRECTORY` | Output directory for downloaded reports; default is `./output` |
| `--size NUMBER` | API page size when listing reports; default is `1000` |
| `--filter FILTER` | LogicMonitor API filter applied when listing reports |
| `--help` | Show command-line help |

## Supported Simplified Syntax

The script also accepts the following forms:

```text
getReport=9
taskId=7542324844873791160
getDefinition=9
out=./reports
size=1000
filter=<LogicMonitor filter>
list-all
listAll
listAll=true
```

Example:

```bash
python Get-LMReports.py getReport=9 out=./reports
```

## Downloaded File Names

When LogicMonitor returns a filename in the HTTP `Content-Disposition` header, the script uses that filename.

For example:

```text
Resource Inventory.csv
```

If the response does not include a filename, the script generates one using the report ID, task ID, timestamp, and detected file type.

Example:

```text
report_9_7542324844873791160_20260820_104500.csv
```

The script avoids overwriting an existing file by adding a timestamp when necessary.

## Authentication

The script uses LogicMonitor LMv1 authentication.

For each API request, the signature is generated from:

```text
HTTP_METHOD + EPOCH + REQUEST_BODY + RESOURCE_PATH
```

For POST requests, the exact JSON string used to generate the LMv1 signature is also sent as the HTTP request body.

The script sends:

```text
X-Version: 3
```

to use the LogicMonitor REST API v3.

## Report Execution Flow

When running:

```bash
python Get-LMReports.py getReport=9
```

the flow is:

```text
Report ID 9
    |
    v
POST /report/reports/9/executions
    |
    v
reportId + taskId + resulturl
    |
    v
GET resulturl
    |
    v
Save generated report to ./output
```

## Example Output

```text
Executing LogicMonitor report ID 9...

Report ID : 9
Task ID   : 7542324844873791160
Result URL: https://example.logicmonitor.com/santaba/rest/report/reports/view?...

Downloading generated report...
URL       : https://example.logicmonitor.com/santaba/rest/report/reports/view?...
Output dir: C:\Temp\LMReports

================================================================================
Report downloaded successfully
================================================================================
File : C:\Temp\LMReports\Resource Inventory.csv
Size : 184,392 bytes
================================================================================
```

## Troubleshooting

### Missing `.env` Values

If one of the required environment variables is missing, the script exits with a message such as:

```text
ERROR: ACCESS_ID is not set in .env
```

Verify that `.env` contains:

```env
ACCESS_ID=...
ACCESS_KEY=...
COMPANY=...
```

### HTTP 401 / Authentication Failure

Check:

- `ACCESS_ID`
- `ACCESS_KEY`
- API token permissions
- Portal/company name
- System clock accuracy

LMv1 authentication uses the current epoch time, so significant clock drift can cause authentication failures.

### HTTP 403

Confirm that the API user associated with the token has permission to:

- View reports
- Execute reports
- Access the generated report output

### HTTP 429

The script detects LogicMonitor API rate limiting and waits before retrying.

### Report Executes but No `resulturl` Is Returned

The script prints the full API response when no `resulturl` is found.

You can also query the task later if a `taskId` was returned:

```bash
python Get-LMReports.py getReport=9 taskId=<TASK_ID>
```

### Downloaded File Has an Unexpected Extension

The script first uses the filename supplied by LogicMonitor.

If LogicMonitor does not provide one, the extension is inferred from the HTTP `Content-Type`, including:

- `.csv`
- `.pdf`
- `.xlsx`
- `.xls`
- `.html`
- `.json`
- `.txt`

Unknown content types use `.dat`.

## LogicMonitor API Endpoints Used

List reports:

```text
GET /santaba/rest/report/reports
```

Get report definition:

```text
GET /santaba/rest/report/reports/{id}
```

Execute report:

```text
POST /santaba/rest/report/reports/{id}/executions
```

Get an existing execution task:

```text
GET /santaba/rest/report/reports/{id}/tasks/{taskId}
```

## Files

Recommended directory layout:

```text
Get-LMReports/
├── Get-LMReports.py
├── README.md
├── .env
└── output/
```

Do not commit `.env` to source control because it contains LogicMonitor API credentials.

A suitable `.gitignore` entry is:

```gitignore
.env
output/
__pycache__/
*.pyc
```

## Security Notes

- Keep `ACCESS_ID` and `ACCESS_KEY` private.
- Do not print or commit API credentials.
- Treat generated `resulturl` values as sensitive because they may contain temporary access parameters.
- Do not store `.env` in a public repository.

## LogicMonitor References

- LogicMonitor REST API Developer's Guide
- LogicMonitor REST API v3 Swagger documentation
- LogicMonitor Reports API
- LogicMonitor API authentication documentation

Primary documentation:

```text
https://www.logicmonitor.com/support/rest-api-developers-guide/overview/using-logicmonitors-rest-api
https://www.logicmonitor.com/support/rest-api-v3-swagger-documentation
```



## Author
Ryan Gillan  
Email: ryangillan@gmail.com
