# LogicMonitor Device Type Health Check

`LM_device_type_healthcheck.py` reviews LogicMonitor resources and helps identify devices with no monitoring, or only minimal monitoring, enabled.

## Requirements

- Python 3
- Python packages: `requests`, `python-dotenv`, and `tabulate`
- LogicMonitor API credentials available as environment variables or in `.env`:

```dotenv
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name
```

By default, credentials are loaded from `.env`. Use a custom credentials file when needed:

```bash
python3 LM_device_type_healthcheck.py --creds-file ~/.config/logicmonitor.env
```

Limit the check to a LogicMonitor device group by name or ID:

```bash
python3 LM_device_type_healthcheck.py --group-name "Linux Servers"
python3 LM_device_type_healthcheck.py --group-id 36
```

Group names match either the group name or its full path. If a name is ambiguous, use the group ID.

Use test mode to process only the first 10 devices:

```bash
python3 LM_device_type_healthcheck.py --test-mode --csv output/test-healthcheck.csv
```

Create starter files for credentials and module reviews:

```bash
python3 LM_device_type_healthcheck.py --create-template
```

This creates `.env_example` and `modules_example.json` in the current folder. Use a custom module definition file with:

```bash
python3 LM_device_type_healthcheck.py --review-active-modules --modules-file modules_example.json --group-id 36
```

The module file defaults to `review-active-modules.json` when `--modules-file` is not supplied. Group and device-type filters can be combined with these templates to create focused reports.

Device retrieval is paged automatically using the LogicMonitor API `total`, `size`, and `offset` values, so resources beyond the first 1,000 devices are included. The script reports the total device count when it starts.

## Usage

Activate the supplied virtual environment:

```bash
source ~/python/bin/activate
```

Run the default health check:

```bash
python3 LM_device_type_healthcheck.py
```

With no arguments, the script displays help. When run with an operational option but no output destination, it uses credentials from `.env` and writes to `output/healthcheck.csv`. Use `--help` to display command help explicitly.

When run with options but no output destination, results default to `output/healthcheck.csv`:

```bash
python3 LM_device_type_healthcheck.py --group-id 36
```

Export the results to CSV as well as displaying them:

```bash
python3 LM_device_type_healthcheck.py --csv output/healthcheck.csv
```

Choose a custom output folder. If `--csv` is omitted, the file is named `healthcheck.csv`:

```bash
python3 LM_device_type_healthcheck.py --folder output
python3 LM_device_type_healthcheck.py --folder output --csv device-health.csv
```

Save the results as a Markdown table:

```bash
python3 LM_device_type_healthcheck.py --markdown output/healthcheck.md
```

Include individual properties using `--included-properties` (or its `--include-properties` alias):

```bash
python3 LM_device_type_healthcheck.py --included-properties predef.externalResourceID
python3 LM_device_type_healthcheck.py --include-properties predef.externalResourceID,system.ips
python3 LM_device_type_healthcheck.py --include-properties systemProperties --markdown output/healthcheck.md
```

Property names are read from the resource's custom, system, inherited, and auto property lists. Values are shown in columns named after the requested properties.

Include additional top-level device API fields with `--extra-fields`:

```bash
python3 LM_device_type_healthcheck.py --extra-fields deviceType,enableNetflow,link
python3 LM_device_type_healthcheck.py --extra-fields hostStatus --csv output/healthcheck.csv
```

Review common active module categories with boolean columns:

```bash
python3 LM_device_type_healthcheck.py --review-active-modules --csv output/healthcheck.csv
```

The review definitions are stored in `review-active-modules.json`. Each key becomes an output column, and its value (or list of values) is matched case-insensitively as a substring against active datasource names. For example, adding `"TerminalServices": "Terminal Services"` adds a `TerminalServices` column automatically.

Module review is performed against all active module names before platform monitoring modules are excluded from `Active datasources (other)`. Use `--debug` to show API requests and the review match results:

```bash
python3 LM_device_type_healthcheck.py --debug --test-mode
```

## What it checks

For every resource, the script retrieves its datasources and keeps only those with `instanceNumber > 0`.

- Cisco resources: SNMP datasources are summarized as `SNMP monitoring` and excluded from the remaining datasource list.
- Windows resources: WMI/WINRM datasources are summarized as `Windows monitoring` and excluded from the remaining datasource list.
- SNMP resources: `system.sysoid` is displayed when an active SNMP datasource is present.
- Relevant properties are displayed for troubleshooting:
  - `auto.snmp.operational`
  - `auto.ssh.available`
  - `auto.ssh.status`
  - `auto.wmi.operational`

The `Monitoring` column follows the PropertySource logic: `noping` takes precedence; otherwise it starts with `DEAD` or `ping`, then adds responding SNMP, WMI, API, and SSH methods, the collector, and HTTP/HTTPS when present in the active datasource list. The relevant response properties are also shown, including `auto.api.responding`.

## Output fields

- `Resource ID` and `Resource`: identify the LogicMonitor resource.
- `name_or_fqdn`: the resource `name` value, such as an IP address or fully qualified domain name.
- `Model`: the `auto.entphysical.descr` property.
- `External resource type`: the `predef.externalResourceType` property.
- `IP addresses`: the `system.ips` property.
- `hostStatus`: the device API `hostStatus` value.
- `Type`: detected as Cisco, Windows, or Other based on resource fields and properties.
- `Monitoring`: the assumed platform monitoring method when detected.
- `Minimal Monitoring`: `True` when any `system.groups` folder path contains `Minimal Monitoring`.
- `Module`: active datasource/module names, normally the underscore-form API name.
- `Active datasources (other)`: the same datasource/module-name format, excluding the assumed Cisco/Windows monitoring datasource.
- `Active datasource count`: total active datasource count before exclusions.
- Auto-properties and `system.sysoid`: supporting monitoring-health details.

Results are written to a file with `--csv` or `--markdown`; the data table is never printed to the screen. Only a write success or failure message with the device count is shown. Markdown reports also include the device count in the file.

## Version

Current version: `1.0.0`

```bash
python3 LM_device_type_healthcheck.py --version
```

## Author

Ryan Gillan  
Email: ryangillan@gmail.com

Resources with an empty monitoring summary, no active datasources, or unexpected auto-property values should be reviewed in LogicMonitor.



## Author
Ryan Gillan  
Email: ryangillan@gmail.com
