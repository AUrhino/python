# LogicMonitor API - Export Modules
This Python script uses the LogicMonitor API to export Modules to json format.
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

---
## Details and Options:
Covered module types (API v3 endpoints):
- DataSources        : /setting/datasources
- EventSources       : /setting/eventsources
- LogSources         : /setting/logsources
- ConfigSources      : /setting/configsources
- PropertySources    : /setting/propertyrules
- TopologySources    : /setting/topologysources
- JobMonitors        : /setting/batchjobs
- AppliesToFunctions : /setting/functions
- OIDs (SNMP SysOID) : /setting/oids

## Requirements:
- Python 3.8+
- requests, python-dotenv


## Examples:
```
  Show help and examples:
    python export_modules.py
    python export_modules.py --help

  Export all module types:
    python export_modules.py --types all --out output_modules

  Export only DataSources and EventSources:
    python export_modules.py --types datasources eventsources --out output_modules

  Export DataSources matching a name filter:
    python export_modules.py --types datasources --filter 'name~"CPU"' --out output_modules

  Export all module types with larger page size and page pacing:
    python export_modules.py --types all --out output_modules --size 200 --sleep 0.2

  Export only AppliesTo Functions:
    python export_modules.py --types appliestofunctions --out output_modules

  Export only SNMP SysOID maps:
    python export_modules.py --types oids --out output_modules

  Export PropertySources:
    python export_modules.py --types propertysources --out output_modules

  Export PropertySources using the API endpoint alias:
    python export_modules.py --types propertyrules --out output_modules

  Debug PropertySources request URLs:
    python export_modules.py --types propertysources --out output_modules --debug

Valid module types:
  datasources, eventsources, logsources, configsources, propertysources, topologysources, jobmonitors, appliestofunctions, oids
  all
```
## Notes:
- Adds retry (3 attempts) for transient errors and continues on module-type failure.
- If HTTP 429 (rate limited): sleeps 30 seconds (or honors Retry-After) then retries.
- Writes one JSON file per module item, plus an index file per module type.


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
