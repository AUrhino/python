# LogicMonitor API - Get LM details for Common Config or IOS backups
This Python script uses the LogicMonitor API to show Common Configs or IOS backups
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

- Show script version
    python3 Get-LMGroupConfigSources.py --version

- Lookup by group ID
    python3 Get-LMGroupConfigSources.py --group_id 12435

- Lookup by full group path
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/Store 1"

- Filter datasource name exact match
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH_Exec_Standard"

- Filter datasource name with wildcard
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*"

- Exclude a datasource name
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "!Cisco_IOS"

- Match a datasource name starting with a literal !
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "\\!Cisco_IOS"

- Include wildcard and exclude one datasource
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*,!Cisco_IOS"

- Filter instance names using comma-separated partial matches
    python3 Get-LMGroupConfigSources.py --group_id 12435 --instance_name_filter "running, startup"

- Same filter with group name
    python3 Get-LMGroupConfigSources.py --group_name "Australia/Stores/Stores/Store 1" --instance_name_filter "running, startup"

- Include device properties
    python3 Get-LMGroupConfigSources.py --group_id 12435 --include_properties "system.staticgroup,snmp.community"

- Filter datasource, instance names and include device properties
    python3 Get-LMGroupConfigSources.py --group_id 12435 --filterDS "SSH*" --instance_name_filter "running, startup" --include_properties "system.staticgroup,snmp.community"

- Export results to CSV
    python3 Get-LMGroupConfigSources.py --group_id 12435 --csv output/configs.csv

- Enable debug output
    python3 Get-LMGroupConfigSources.py --group_id 12435 --debug

Datasource filter behavior:
    --filterDS "SSH_Exec_Standard"
    --filterDS "SSH*"
    --filterDS "!Cisco_IOS"
    --filterDS "\\!Cisco_IOS"
    --filterDS "SSH*,!Cisco_IOS"

Rules:
    - leading !  => exclude pattern
    - leading \! => literal ! in the pattern
    - * is supported as a wildcard

Instance name filter behavior:
    --instance_name_filter "running, startup"

This is a case-insensitive substring match:
    "running" -> matches "Running-Config"
    "startup" -> matches "Startup-Config"

Included properties behavior:
    --include_properties "system.staticgroup,snmp.community"

For each requested property, the script checks these arrays on the device:
    - customProperties
    - systemProperties
    - autoProperties
    - inheritedProperties

If a property is not found, the output cell is left blank.

```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
