# LogicMonitor API Integration
This Python script uses the LogicMonitor API to retrieve devices in a tabular format.
It supports optional device fields, selected properties, DataSource/instance counts, and CSV export.

---
## LogicMonitor API Credentials
- `ACCESS_ID`
- `ACCESS_KEY`
- `COMPANY`

---

## Setup

1. **Clone or download this repository.**
2. **Create a `.env` file in the project root** with the following content:
   ```env
   ACCESS_ID=your_access_id
   ACCESS_KEY=your_access_key
   COMPANY=your_company_name

---

## Output
- The script will display retrieved information in a formatted table in the console.
- A device count is displayed as JSON, for example `{ "total": 14 }`.
- The device response is saved to `output/getDevices.json`.
- Use `--csv` to save the displayed table to a CSV file.

## Command-line options

```text
--csv PATH
    Write the displayed device table to PATH.
--show-counts true|false
    Include DataSource and instance counts for each device (default: false).
--show_modules true|false
    Display each device's DataSources and their instances (default: false).
--include-properties NAME,...
    Include selected properties from customProperties, systemProperties,
    autoProperties, or inheritedProperties.
--include-fields NAME,...
    Include additional device fields returned by the API.
--filter FIELD=VALUE
    Filter devices by an API field. Can be repeated.
--version
    Display the script version.
```

## Examples

```bash
python3 Get-LMDevices.py --help
python3 Get-LMDevices.py --version
python3 Get-LMDevices.py
python3 Get-LMDevices.py --show-counts true
python3 Get-LMDevices.py --show-counts false
python3 Get-LMDevices.py --show_modules true
python3 Get-LMDevices.py --csv ./output/devices.csv
python3 Get-LMDevices.py --include-properties snmp.community,ssh.user
python3 Get-LMDevices.py --include-properties snmp.community,ssh.user --csv ./output/devices.csv
python3 Get-LMDevices.py --include-fields description,link,hostStatus
python3 Get-LMDevices.py --include-fields description,link --include-properties snmp.community,ssh.user --show-counts true
python3 Get-LMDevices.py --filter displayName=Lenny
python3 Get-LMDevices.py --filter name=192.168.20.24
python3 Get-LMDevices.py --filter hostStatus=normal --filter disableAlerting=false
```

---


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
