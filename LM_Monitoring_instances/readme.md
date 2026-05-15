# LogicMonitor Manage Instances in bulk
This Python script uses the LogicMonitor API stop monitoring or alerting on instances

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
  python unmanage_interfaces_from_csv_id_v9.py --input TEST_NEt_Interfaces.csv --debug
  python unmanage_interfaces_from_csv_id_v9.py --input TEST_NEt_Interfaces.csv --apply --debug
```

## Sample csv:
This is the output of a report with the Network Interfaces
```
Company:,MyPortal,,,
Powered By:,LogicMonitor Inc.,,,
Description:,,,,
,,,,
Id,Resource,Datasource,Instance,Manage,stopMonitoring,disableAlerting
1,Lenny,Network Interfaces (Linux_SSH_NetworkInterfaces),veth3b28b4af,false,false,true
```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
