# LogicMonitor API – Get LM Alerts

`Get-LMAlerts.py` retrieves LogicMonitor alerts through the REST API. It can retrieve account-wide alerts, alerts for one device, or one alert by ID. Results are displayed in the terminal and JSON files are written to `output/`.

Version: **1.0.6**

## Requirements

- Python 3.8 or newer
- A LogicMonitor API access ID and API key with permission to read alerts
- Python packages: `requests`, `python-dotenv` (optional: `tabulate`)

## Setup

1. Open a terminal in the directory containing `Get-LMAlerts.py`.
2. Install dependencies:

```bash
python3 -m pip install requests python-dotenv tabulate
```

3. Create a `.env` file in the project root:

```env
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name
```

Do not commit `.env` or share the access key.

## Quick start

Running without arguments prints the help and examples:

```bash
python3 Get-LMAlerts.py
python3 Get-LMAlerts.py --version
```

## Usage examples

Account-wide alerts:

```bash
python3 Get-LMAlerts.py account
python3 Get-LMAlerts.py account --days-ago 7
python3 Get-LMAlerts.py account --filter "cleared:false" --save-table
python3 Get-LMAlerts.py account --fields "id,severity,monitorObjectName"
python3 Get-LMAlerts.py account --verbose
python3 Get-LMAlerts.py account --page-size 100 --output-dir ./output
python3 Get-LMAlerts.py account --debug --verbose
```

Alerts for a device:

```bash
python3 Get-LMAlerts.py device --device-id 123
python3 Get-LMAlerts.py device --device-id 123 --debug
python3 Get-LMAlerts.py device --device-id 123 --days-ago 14 --need-message
python3 Get-LMAlerts.py device --device-id 123 --filter "severity:>=3"
python3 Get-LMAlerts.py device --device-id 123 --verbose
python3 Get-LMAlerts.py device --device-id 123 --start 1773581054 --end 1773667454
python3 Get-LMAlerts.py device --device-id 123 --fields "id,severity,monitorObjectName"
python3 Get-LMAlerts.py device --device-id 123 --custom-columns "property=value"
python3 Get-LMAlerts.py device --device-id 123 --bound instances --page-size 100 --output-dir ./output
python3 Get-LMAlerts.py device --device-id 123 --debug --verbose
```

Fetch one alert:

```bash
python3 Get-LMAlerts.py alert --alert-id DS267
python3 Get-LMAlerts.py alert --alert-id DS267 --need-message --save-table
python3 Get-LMAlerts.py alert --alert-id DS267 --verbose
python3 Get-LMAlerts.py alert --alert-id DS267 --fields "id,severity,monitorObjectName"
python3 Get-LMAlerts.py alert --alert-id DS267 --custom-columns "property=value" --output-dir ./output
python3 Get-LMAlerts.py alert --alert-id DS267 --debug --verbose
```

## Output

- Results are printed as a formatted table.
- Account-wide alert tables include the LogicMonitor `monitorObjectId` as `Device ID` and the alert `internalId`.
- JSON responses are saved under `output/`.
- `--save-table` also saves the report as a `.text` file.
- Use `--output-dir PATH` to choose another output directory.
- `--days-ago N` returns alerts from the last N days.
- Device mode accepts epoch timestamps with `--start` and `--end`.
- Dates are displayed in the `Australia/Sydney` timezone.
- Add `--debug` to print the API URL, request parameters, response status, and full error traceback when troubleshooting.
- Add `--verbose` to display every field returned by the API. `StartEpoch` is hidden from default views but remains available in verbose output.

## Author

Ryan Gillan  
Email: ryangillan@gmail.com
