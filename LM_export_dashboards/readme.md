# LogicMonitor Export Dashboards
This Python script uses the LogicMonitor API extract Dashboards to a file
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
  python export_LMdashboards.py --help
  python export_LMdashboards.py --out output_dashboards
  python export_LMdashboards.py --out output_dashboards --size 200 --sleep 0.2
  python export_LMdashboards.py --filter 'name~"NOC"' --out output_dashboards
  python export_LMdashboards.py --list-all
  python export_LMdashboards.py --list-all --filter 'name~"Prod"'
```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
