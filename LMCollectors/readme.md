# LogicMonitor API - Get LM Collectors
This Python script uses the LogicMonitor API to show Collectors.
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
    python3 Get-LMCollectors.py

- Show collector summary:
    python3 Get-LMCollectors.py --show-all

- Show detailed collector view:
    python3 Get-LMCollectors.py --detailed

- Enable debug output:
    python3 Get-LMCollectors.py --show-all --debug
```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
