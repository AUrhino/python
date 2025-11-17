"""
LogicMonitor API - Get Devices and Related Data
------------------------------------------------
This script retrieves all devices from a LogicMonitor account, displays them in a table,
and optionally allows the user to fetch associated DataSources and their instances.

Requirements:
- Python 3.x
- requests, tabulate, python-dotenv
- .env file with:
ACCESS_ID=your_access_id
ACCESS_KEY=your_access_key
COMPANY=your_company_name

Author: Ryan Gillan
"""

import os
import time
import hmac
import hashlib
import base64
import json
import requests
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()
ACCESS_KEY = os.getenv("ACCESS_KEY")
ACCESS_ID = os.getenv("ACCESS_ID")
COMPANY = os.getenv("COMPANY")

BASE_URL = f"https://{COMPANY}.logicmonitor.com/santaba/rest"


def generate_auth_headers(http_verb: str, resource_path: str, data: str = "") -> dict:
    """
    Generate LogicMonitor API authentication headers.
    """
    epoch = str(int(time.time() * 1000))
    request_vars = http_verb + epoch + data + resource_path
    hmac_hash = hmac.new(ACCESS_KEY.encode(), msg=request_vars.encode(), digestmod=hashlib.sha256).hexdigest()
    signature = base64.b64encode(hmac_hash.encode()).decode()
    auth = f"LMv1 {ACCESS_ID}:{signature}:{epoch}"
    return {"Content-Type": "application/json", "Authorization": auth}


def api_get(resource_path: str) -> dict:
    """
    Perform a GET request to the LogicMonitor API.
    """
    url = BASE_URL + resource_path
    headers = generate_auth_headers("GET", resource_path)
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return {}


def display_table(data: list, headers: list, title: str = ""):
    """
    Display data in a formatted table.
    """
    if title:
        print("\n" + "=" * 50)
        print(title)
        print("=" * 50)
    print(tabulate(data, headers=headers, tablefmt="grid"))


def get_devices():
    """
    Fetch and display all devices.
    """
    print("\nFetching devices...")
    response = api_get("/device/devices")

    if "data" not in response or "items" not in response.get("data", {}):
        print("Invalid or missing data in API response.")
        return []

    devices = response["data"]["items"]
    table_data = []

    for device in devices:
        if not isinstance(device, dict):
            continue
        device_id = device.get("id")
        name = device.get("name")
        display_name = device.get("displayName")

        # Extract properties safely
        auto_props = {prop.get("name"): prop.get("value") for prop in device.get("autoProperties", []) if isinstance(prop, dict)}
        sys_props = {prop.get("name"): prop.get("value") for prop in device.get("systemProperties", []) if isinstance(prop, dict)}

        manufacturer = auto_props.get("auto.endpoint.manufacturer")
        sysinfo = sys_props.get("system.sysinfo")
        description = auto_props.get("auto.entphysical.descr")

        table_data.append([device_id, name, display_name, manufacturer, sysinfo, description])

    headers = ["Device ID", "Name", "Display Name", "Manufacturer", "Sysinfo", "Description"]
    display_table(table_data, headers, "LogicMonitor Devices")

    return devices


def get_device_datasources(device_id: str):
    """
    Fetch and display DataSources for a given device.
    """
    print(f"\nFetching DataSources for device ID: {device_id}")
    response = api_get(f"/device/devices/{device_id}/devicedatasources")

    items = response.get("data", {}).get("items", [])
    if not items:
        print("No DataSources found.")
        return []

    output_data = []
    for item in items:
        if not isinstance(item, dict):
            continue
        graphs = [graph.get("id") for graph in item.get("graphs", []) if isinstance(graph, dict)]
        output_data.append([
            item.get("dataSourceId"),
            item.get("dataSourceName"),
            item.get("deviceName"),
            item.get("deviceDisplayName"),
            graphs
        ])

    headers = ["DataSource ID", "Name", "Device Name", "Display Name", "Graph IDs"]
    display_table(output_data, headers, "Device DataSources")

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/getDeviceDataSources.json", "w") as file:
        json.dump(items, file, indent=4)

    return items


def get_datasource_instances(device_id: str, datasource_id: str):
    """
    Fetch and display instances for a given DataSource.
    """
    print(f"\nFetching instances for DataSource ID: {datasource_id}")
    response = api_get(f"/device/devices/{device_id}/devicedatasources/{datasource_id}/instances")

    items = response.get("data", {}).get("items", [])
    if not items:
        print("No instances found.")
        return

    table_data = []
    for item in items:
        if not isinstance(item, dict):
            continue
        table_data.append([
            item.get("deviceDataSourceId"),
            item.get("name"),
            item.get("deviceDisplayName"),
            item.get("id"),
            item.get("dataSourceId")
        ])

    headers = ["Device DataSource ID", "Name", "Device Display Name", "Instance ID", "DataSource ID"]
    display_table(table_data, headers, "DataSource Instances")

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/output_getDatasourceInstances.json", "w") as file:
        json.dump(items, file, indent=4)


if __name__ == "__main__":
    devices = get_devices()
    if devices and input("\nWould you like to capture datasources and instances for a device? (y/n): ").lower() == "y":
        device_id = input("Enter the device ID: ")
        datasources = get_device_datasources(device_id)

        if datasources and input("\nProceed with getting DataSource instances? (y/n): ").lower() == "y":
            datasource_id = input("Enter the DataSource ID: ")
            get_datasource_instances(device_id, datasource_id)
        else:
            print("Exiting without fetching instances.")
    else:
        print("Exiting without further actions.")
