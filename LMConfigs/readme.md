# LogicMonitor API - Get LM details for Common Config backups
This Python script uses the LogicMonitor API to show Common Configs
This has been tested with Cisco equipment, adding other Common Config types is possiable.

---
## LogicMonitor API Credentials
- `ACCESS_ID`
- `ACCESS_KEY`
- `COMPANY`

---

## Setup

1. **Clone or download this repository.**
2. **Create a `.env` file in the project root** with the following content:
```.env
   ACCESS_ID=your_access_id
   ACCESS_KEY=your_access_key
   COMPANY=your_company_name
```
---

## Output
- The script will display retrieved information in a formatted table in the console.
- JSON responses will be saved in the output/ directory, including device information and data source instances.


## Requirements:
- Python 3.8+
- requests, tabulate, python-dotenv


## Examples:
```
- Show help:
    python3 Get-LMGroupConfigSources.py
    python3 Get-LMGroupConfigSources.py --help

- Enable debug output:
    python3 Get-LMGroupConfigSources.py --debug

- Enable version:
    python3 Get-LMGroupConfigSources.py --version

- Lookup by group ID
    python3 Get-LMGroupConfigSources.py --group_id 12435

- Lookup by full group path
    python3 Get-LMGroupConfigSources.py --group_name "Australia/ACME/Store 1"

- Filter instance names using comma-separated partial matches
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup"

- Filter instance names and include device properties (custom,system,inherited and auto properties)
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup" --include_properties "system.staticgroups,snmp.community"

- Same filter with group name
    python3 Get-LMGroupConfigSources.py --group_name "Australia/ACME/Store 1" --instance_name_filter "running, startup"

- Export results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --csv output/configs.csv

- Export filtered results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup" --csv output/configs.csv

- Enable debug output
    python3 Get-LMGroupConfigSources.py --group_id 12435 --debug

- Instance name filter behavior:
    --instance_name_filter "running, startup"

- This is a case-insensitive substring match:
    "running" -> matches "Running-Config"
    "startup" -> matches "Startup-Config"

```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
