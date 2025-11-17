# LogicMonitor API Integration
This Python script uses the LogicMonitor API to retrieve device details and their associated data sources.
It can list all devices in your LogicMonitor account in a tabular format.
Additionally, you have the option to provide a specific device ID to fetch detailed information about its data sources and their instances.

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
- JSON responses will be saved in the output/ directory, including device information and data source instances.

---


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
