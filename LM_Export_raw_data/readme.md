# LogicMonitor API - Extract raw data for a module for all devices in a group
This Python script uses the LogicMonitor API to extract raw data
Files are exported to a specific folder.

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
    python3 export_lm_rawdata.py

- Show matching inventory:
    python3 export_lm_rawdata.py --show-all --module "SNMP_Network_Interfaces"

- Export all datapoints for a module:
    python3 export_lm_rawdata.py --csv snmp_interfaces.csv --module "SNMP_Network_Interfaces"

- Export only selected datapoints:
    python3 export_lm_rawdata.py --csv util.csv --module "SNMP_Network_Interfaces" \
        --datapoints InUtilizationPercent OutUtilizationPercent

- Export discovered datapoints matching a regex:
    python3 export_lm_rawdata.py --csv temp.csv --module "Some_Module" \
        --datapoint-regex "temp|humidity"
- Enable debug output:
    python3 export_lm_rawdata.py --debug
```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
