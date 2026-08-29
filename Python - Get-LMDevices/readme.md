# LogicMonitor Device Exporter

Version: **1.03**

`Get-LMDevices.py` retrieves LogicMonitor devices using the REST API and displays them in a table. It supports filtering, CSV export, selected properties and fields, DataSource/instance counts, and detailed module output.

## Requirements

- Python 3.x
- `requests`
- `tabulate`
- `python-dotenv`
- LogicMonitor API credentials in `.env`

```env
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name
```

## Setup

```bash
source ~/python/bin/activate
pip install requests tabulate python-dotenv
```

## Usage

```bash
python3 Get-LMDevices.py --help
python3 Get-LMDevices.py --version
python3 Get-LMDevices.py
```

The device list uses the LogicMonitor endpoint `/santaba/rest/device/devices/`. Filters are sent as `filter=FIELD:"VALUE"`; string filters support a trailing wildcard.

## Options

```text
--csv PATH
    Export the displayed device table to PATH.

--show-counts true|false
    Add DataSource and instance counts to the device table. Default: false.

--show_modules true|false
--show-modules true|false
    Display a DataSource/module table for each device, including instance and
    monitoring-instance counts. Default: false.

--include-properties NAME,...
    Include comma-separated properties such as snmp.community,ssh.user.
    Properties may come from customProperties, systemProperties,
    autoProperties, or inheritedProperties.

--include-fields NAME,...
    Include additional device API fields as columns.

--filter FIELD=VALUE
    Filter devices by an API field. Repeat the option for multiple filters.

--debug
    Display the prepared request URL, HTTP status, and response keys.

--version
    Display version 1.03.
```

## Examples

```bash
# Display all devices
python3 Get-LMDevices.py

# Display help and version
python3 Get-LMDevices.py --help
python3 Get-LMDevices.py --version

# Exact display-name filter
python3 Get-LMDevices.py --filter displayName=Lenny

# Display-name wildcard filter
python3 Get-LMDevices.py --filter 'displayName=Le*'

# Filter by device name or device type
python3 Get-LMDevices.py --filter name=192.168.20.24
python3 Get-LMDevices.py --filter deviceType=18

# Inspect the generated request
python3 Get-LMDevices.py --filter displayName=Lenny --debug

# Include selected fields and properties
python3 Get-LMDevices.py \
  --include-fields description,link,hostStatus \
  --include-properties snmp.community,ssh.user

# Include DataSource and instance counts
python3 Get-LMDevices.py --show-counts true

# Display DataSources/modules with instance counts
python3 Get-LMDevices.py --filter displayName=Lenny --show_modules true

# Export the displayed table to CSV
python3 Get-LMDevices.py --csv output/devices.csv
python3 Get-LMDevices.py --show-counts true --csv output/devices-with-counts.csv
```

## Output

The script prints a device total, for example:

```json
{
  "total": 14
}
```

Raw device data is saved to `output/getDevices.json`. When `--show_modules true` is used, each device’s module request uses the DataSource endpoint with `size=1000`, `offset=0`, and these fields:

```text
id,dataSourceName,dataSourceDisplayName,instanceNumber,monitoringInstanceNumber
```

## Author

Ryan Gillan  
Email: ryangillan@gmail.com
