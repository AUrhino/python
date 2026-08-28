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

  Export only custom/no-core modules (changed core modules are included):
    python export_modules.py --types all --out output_modules --custom-only

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
- Writes one JSON file per module item.
- `--custom-only` excludes modules where `installationMetadata.originAuthorNamespace` is `core`, unless `installationMetadata.isChangedFromOrigin` is `true`. The option defaults to `false`.


## Author
Ryan Gillan  
Email: ryangillan@gmail.com


### Include Services

Use `--include-services` to include LogicMonitor services in DataSource exports.

Services are identified by:

```text
collectMethod == "aggregate"
```

The flag is opt-in and defaults to false.

Examples:

```bash
python export_modules.py --types datasources --out output_modules --include-services
python export_modules.py --types datasources --out output_modules --custom-only --include-services
```

When combined with `--custom-only`, aggregate services are explicitly included while the remaining modules are filtered using registry metadata (`namespace=core` is excluded; missing/null namespace is treated as custom).


### Custom-only performance warning

`--custom-only` is **very, very slow** because the exporter must process each module against another LogicMonitor registry-metadata endpoint to determine whether it is core or custom.

For example, each DataSource requires an additional registry metadata request such as:

```text
/setting/registry/metadata/datasource/<id>
```

### Overwrite behavior

The `--overwrite` option controls what happens when an output file already exists.

Default:

```bash
python export_modules.py --types datasources --out output_modules --overwrite true
```

With `--overwrite true` (the default), an existing module filename is replaced.

With:

```bash
python export_modules.py --types datasources --out output_modules --overwrite false
```

existing files are preserved. New files use the normal naming rules:

```text
<out_dir>/<module_key>/<name>.json
<out_dir>/<module_key>/<name>.md                # when --markdown is used
<out_dir>/<module_key>/<name>__2.json           # duplicate/new filename
<out_dir>/<module_key>/<name>__2.md             # duplicate/new filename + markdown
<out_dir>/<module_key>/_error.txt               # when a module type export fails
```

This means `--overwrite false` is useful when retaining an existing backup and adding another export without replacing files already on disk.
