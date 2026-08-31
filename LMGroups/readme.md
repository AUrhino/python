# LogicMonitor API - Get LM Groups
This Python script uses the LogicMonitor API to show groups.
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
- Use `--markdown FILE` with any view to export the result as GitHub-flavored Markdown.


## Requirements:
- Python 3.8+
- requests, tabulate, python-dotenv


## Examples:
```
- Show help:
    python3 Get-LMGroups.py

- Show device group summary:
    python3 Get-LMGroups.py --show

- Show detailed device group view:
    python3 Get-LMGroups.py --show-detailed

- Show a single device group:
    python3 Get-LMGroups.py --show-single --id 26

- Show device groups as a tree:
    python3 Get-LMGroups.py --tree

- Enable debug output:
    python3 Get-LMGroups.py --show --debug

- Export the detailed view to Markdown:
    python3 Get-LMGroups.py --show-detailed --markdown groups.md

```


## Author
Ryan Gillan  
Email: ryangillan@gmail.com
