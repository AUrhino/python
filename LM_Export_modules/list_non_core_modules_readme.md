# List Non-Core LogicMonitor Modules

`list_non_core_modules.py` lists LogicMonitor DataSources whose registry status is not `Core` and writes the results to `out_noncore.csv`.

## Requirements

- Python 3.8+
- `requests`
- `python-dotenv`
- LogicMonitor API credentials with permission to list DataSources and read registry metadata

## Setup

Create a `.env` file in this directory. Do not commit it:

```dotenv
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name
```

Activate the shared virtual environment and install dependencies if needed:

```bash
source ~/python/bin/activate
pip install requests python-dotenv
```

## Run

```bash
source ~/python/bin/activate
python list_non_core_modules.py
```

### Command-line examples

```bash
# Show help and examples
python list_non_core_modules.py --help

# Write to a different output path
python list_non_core_modules.py --output reports/noncore.csv

# Show request URLs and HTTP status codes (credentials are not printed)
python list_non_core_modules.py --debug

# Combine options
python list_non_core_modules.py --output reports/noncore.csv --debug
```

The script paginates through `/setting/datasources`, checks each DataSource with `/setting/registry/metadata/datasource/<id>`, excludes rows whose status is exactly `Core`, and creates `out_noncore.csv` in the current directory.

The CSV columns are `id`, `name`, `displayName`, `namespace`, `registryVersion`, `lmLocator`, and `status`.

If a DataSource has no registry metadata, LogicMonitor returns HTTP 404. The script treats that as `Custom or modified` and continues processing the remaining DataSources.

Network connection errors and transient HTTP responses are retried up to three times with a short backoff.
