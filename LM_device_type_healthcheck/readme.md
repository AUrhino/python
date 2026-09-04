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
python3 LM_device_type_healthcheck.py --test-mode --format csv
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

Example `modules_example.json`:

```json
{
  "Interfaces": [
    "interface",
    "winif",
    "network",
    "Brocade_Switch_Ports"
  ],
  "Memory": [
    "memory",
    "mem",
    "WinMemory64"
  ],
  "Ping": [
    "Ping"
  ],
  "TerminalServices": "Terminal Services"
}
```

Each JSON key becomes a `True`/`False` output column. A list checks multiple possible module names; a single string checks one name. Matching is case-insensitive and looks for the value within the active module name.

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

With no arguments, the script displays help. When run with an operational option, it uses credentials from `.env` and writes the selected format to `output/healthcheck.<format>`. Use `--help` to display command help explicitly.

When run with options, results default to `output/healthcheck.csv` unless another format is selected:

```bash
python3 LM_device_type_healthcheck.py --group-id 36
```

Select the output format with `--format`:

```bash
python3 LM_device_type_healthcheck.py --format csv
python3 LM_device_type_healthcheck.py --format html
python3 LM_device_type_healthcheck.py --format markdown
```

Choose a custom output folder. Files are automatically named `healthcheck.csv`, `healthcheck.html`, or `healthcheck.md`:

```bash
python3 LM_device_type_healthcheck.py --format html --folder output
python3 LM_device_type_healthcheck.py --format markdown --folder /tmp/lm-reports
```

The HTML report supports sorting by clicking column headers and resizing columns by dragging the right edge of a header. In both HTML and Markdown reports, cells whose value is `FALSE` are highlighted red.

```bash
python3 LM_device_type_healthcheck.py --format markdown --folder output
```

Include individual properties using `--included-properties` (or its `--include-properties` alias):

```bash
python3 LM_device_type_healthcheck.py --included-properties predef.externalResourceID
python3 LM_device_type_healthcheck.py --include-properties predef.externalResourceID,system.ips
python3 LM_device_type_healthcheck.py --include-properties systemProperties --format markdown
```

Property names are read from the resource's custom, system, inherited, and auto property lists. Values are shown in columns named after the requested properties.

Include additional top-level device API fields with `--extra-fields`:

```bash
python3 LM_device_type_healthcheck.py --extra-fields deviceType,enableNetflow,link
python3 LM_device_type_healthcheck.py --extra-fields hostStatus --format csv
```

Review common active module categories with boolean columns:

```bash
python3 LM_device_type_healthcheck.py --review-active-modules --format csv
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

Results are written to a file using `--format {csv,html,markdown}`; the data table is never printed to the screen. Only a write success or failure message with the device count is shown. Markdown and HTML reports also include the device count in the file.

## Version

Current version: `1.1.0`

```bash
python3 LM_device_type_healthcheck.py --version
```

## Note:

Resources with an empty monitoring summary, no active datasources, or unexpected auto-property values should be reviewed in LogicMonitor.



## Author
Ryan Gillan  
Email: ryangillan@gmail.com
