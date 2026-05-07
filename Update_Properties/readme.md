# LogicMonitor API - Update LM properties from a csv file
LogicMonitor API - Update Device Properties from CSV
Reads a CSV file and updates/adds LogicMonitor custom properties on devices.

Behavior in the script:
- For each key:value pair, it calls:
- ```PUT /device/devices/{deviceId}/properties/{propertyName}```
- If that property already exists on the device, the value is replaced with the CSV value.
- If the property does not exist and LM returns 404, it falls back to:
- ```POST /device/devices/{deviceId}/properties```
- It does not remove any existing properties that are not in the CSV.
- It skips system.* and auto.* properties because those are not normal custom properties.
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


## Requirements:
- Python 3.8+
- requests, tabulate, python-dotenv


## Examples:
```
   python update_device_properties_from_csv.py --csv devices.csv --dry-run
   python update_device_properties_from_csv.py --csv devices.csv
```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
