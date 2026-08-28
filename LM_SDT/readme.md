# LogicMonitor SDT Toolkit

`logicmonitor_sdt.py` creates, lists, and cancels LogicMonitor Scheduled Downtimes (SDTs) for devices and device groups.

## Requirements

- Python 3.9+
- Python packages:

```bash
python3 -m pip install requests python-dotenv
```

Activate the virtual environment used for this toolkit:

```bash
source ~/python/bin/activate
```

## Configuration

Create a `.env` file in this directory:

```dotenv
COMPANY=your-logicmonitor-company
ACCESS_ID=your-api-access-id
ACCESS_KEY=your-api-access-key
```

The API key is sensitive. Do not commit `.env` to source control. Credentials can also be supplied with `--company`, `--access-id`, and `--access-key`.

## Usage

### Device IDs

```bash
python logicmonitor_sdt.py -Id 123 -StartDate now -EndDate "+1hr" -Comment "1 hr SDT"
python logicmonitor_sdt.py -Id 123 -StartDate now -EndDate "+1day" -Comment "1 day SDT"
python logicmonitor_sdt.py -Id 123 -StartDate "2024-01-01 00:00" -EndDate "2024-01-02 00:00" -Comment "Extended maintenance"
```

Dates may be `now`, relative values such as `+1hr` or `+1day`, or UTC timestamps in `YYYY-MM-DD HH:MM` format.

### Weekly SDT

```bash
python logicmonitor_sdt.py -Id 123 -SdtType Weekly   -StartHour 13 -StartMinute 7 -EndHour 14 -EndMinute 7   -WeekDay Monday,Thursday
```

Supported recurrence types are `OneTime`, `Weekly`, `Monthly`, and `Daily`. Weekly weekdays are Sunday through Saturday; multiple days are comma-separated.

### Device groups and display names

```bash
python logicmonitor_sdt.py set-group 456 --start now --end "+1day" --comment "Group maintenance"
python logicmonitor_sdt.py set-name "server-01" --start now --end "+1hr" --comment "Device maintenance"
```

### CSV

```bash
python logicmonitor_sdt.py set-csv maintenance.csv
```

CSV columns use LogicMonitor API property names. A one-time example:

```csv
type,deviceId,startDateTime,endDateTime,comment
DeviceSDT,123,2024-01-01 00:00,2024-01-02 00:00,Extended maintenance
DeviceGroupSDT,456,2024-01-01 00:00,2024-01-01 01:00,Group maintenance
```

For CSV timestamps, use epoch milliseconds. The CSV `type` must be `DeviceSDT` or `DeviceGroupSDT`; device rows require `deviceId` or `deviceDisplayName`, and group rows require `deviceGroupId` plus `dataSourceId` (use `0` for all data sources).

### List and cancel

List all SDTs in a table:

```bash
python logicmonitor_sdt.py show-sdt
```

List SDTs for a specific device. Device listings are sorted by end time, newest first:

```bash
python logicmonitor_sdt.py show-sdt --device-id 123
```

List SDTs for a device group:

```bash
python logicmonitor_sdt.py show-sdt --group-id 456
```

Use `--json` for the raw API response instead of the table:

```bash
python logicmonitor_sdt.py show-sdt --device-id 123 --json
```

Cancel an SDT by its LogicMonitor SDT ID:

```bash
python logicmonitor_sdt.py Remove-SDT -Id H_10
python logicmonitor_sdt.py cancel-sdt H_10
```

Use debug mode to inspect authentication, endpoint, status, and response details without printing credential values:

```bash
python logicmonitor_sdt.py --debug show-sdt --device-id 123
```

Use `python logicmonitor_sdt.py --help` for the complete command reference.

## Exit codes

- `0`: operation completed successfully
- `1`: configuration, validation, file, or API error
- `130`: interrupted with Ctrl+C


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
