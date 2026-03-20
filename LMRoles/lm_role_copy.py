#!/usr/bin/env python3
"""
Backup a LogicMonitor role from the source portal to a JSON file,
then create or update that role in the destination portal.

Requirements:
- Python 3.x
- requests
- python-dotenv

Install:

Create a file named .env_multi in the same directory as this script:

SOURCE_COMPANY=portala
SOURCE_ACCESS_ID=source_access_id_here
SOURCE_ACCESS_KEY=source_access_key_here

DEST_COMPANY=portalb
DEST_ACCESS_ID=dest_access_id_here
DEST_ACCESS_KEY=dest_access_key_here

Examples:
    # List all roles in source portal
    python lm_role_copy.py --list

    # List all roles with debug output
    python lm_role_copy.py --list --debug

    # Backup role ID 10 ("Clone") from portala and create it on portalb
    python lm_role_copy.py --role-id 10 --backup-file backups/clone_role.json --debug

    # Backup role ID 10 ("Clone") from portala and update it on portalb if it already exists
    python lm_role_copy.py --role-id 10 --backup-file backups/clone_role.json --overwrite --debug

    # Backup role ID 10 ("Clone") from portala and create it on portalb with a different name
    python lm_role_copy.py --role-id 10 --backup-file backups/clone_role.json --target-role-name "Clone - Imported" --debug

    # Copy role by ID and save backup JSON
    python lm_role_copy.py --role-id 123 --backup-file backups/role_123.json

    # Copy role by name
    python lm_role_copy.py --role-name "NOC Read Only" --backup-file backups/noc_read_only.json

    # Backup on source and create with a new name on destination during create
    python lm_role_copy.py --role-name "NOC Read Only" --backup-file backups/noc_read_only.json --target-role-name "NOC Read Only - Imported"

    # Overwrite existing destination role if it already exists
    python lm_role_copy.py --role-id 123 --backup-file backups/role_123.json --overwrite

    # Keep the same roleGroupId (only if valid in destination)
    python lm_role_copy.py --role-id 123 --backup-file backups/role_123.json --keep-role-group-id

    # Explicitly set destination roleGroupId
    python lm_role_copy.py --role-id 123 --backup-file backups/role_123.json --target-role-group-id 5
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import hmac
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv


ENV_FILE = Path(__file__).with_name(".env_multi")
load_dotenv(dotenv_path=ENV_FILE)


class LMApiError(RuntimeError):
    pass


@dataclass
class PortalCreds:
    company: str
    access_id: str
    access_key: str

    @property
    def base_url(self) -> str:
        return f"https://{self.company}.logicmonitor.com/santaba/rest"


def load_portal_creds(prefix: str) -> PortalCreds:
    company = os.getenv(f"{prefix}_COMPANY")
    access_id = os.getenv(f"{prefix}_ACCESS_ID")
    access_key = os.getenv(f"{prefix}_ACCESS_KEY")

    missing = [
        name
        for name, value in {
            f"{prefix}_COMPANY": company,
            f"{prefix}_ACCESS_ID": access_id,
            f"{prefix}_ACCESS_KEY": access_key,
        }.items()
        if not value
    ]

    if missing:
        print(
            f"Missing required environment variables in {ENV_FILE.name}: "
            f"{', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    return PortalCreds(
        company=company,
        access_id=access_id,
        access_key=access_key,
    )


SOURCE = load_portal_creds("SOURCE")
DEST = load_portal_creds("DEST")


@dataclass
class LMClient:
    creds: PortalCreds
    api_version: str = "3"
    timeout: int = 30
    debug: bool = False

    def _debug_print(self, message: str) -> None:
        if self.debug:
            print(f"DEBUG: {message}", file=sys.stderr)

    def _build_headers(self, method: str, resource_path: str, body: str = "") -> Dict[str, str]:
        epoch = str(int(time.time() * 1000))
        request_vars = method.upper() + epoch + body + resource_path

        digest = hmac.new(
            self.creds.access_key.encode("utf-8"),
            msg=request_vars.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        signature = base64.b64encode(digest.encode("utf-8")).decode("utf-8")
        auth = f"LMv1 {self.creds.access_id}:{signature}:{epoch}"

        return {
            "Authorization": auth,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Version": self.api_version,
        }

    def request(
        self,
        method: str,
        resource_path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        method = method.upper()
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) if payload is not None else ""

        url = f"{self.creds.base_url}{resource_path}"
        if params:
            query = urlencode(params, doseq=True)
            url = f"{url}?{query}"

        # LMv1 signature should use the resource path only, not the query string.
        headers = self._build_headers(method, resource_path, body)

        self._debug_print(f"{method} {url}")

        response = requests.request(
            method=method,
            url=url,
            data=body if body else None,
            headers=headers,
            timeout=self.timeout,
        )

        self._debug_print(f"{method} {url} -> HTTP {response.status_code}")

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            try:
                parsed = response.json()
            except ValueError:
                parsed = response.text
        else:
            parsed = response.text

        if not response.ok:
            raise LMApiError(
                f"{method} {url} failed with HTTP {response.status_code}: "
                f"{json.dumps(parsed, indent=2) if isinstance(parsed, (dict, list)) else parsed}"
            )

        return parsed

    @staticmethod
    def _unwrap(obj: Any) -> Any:
        if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], dict):
            return obj["data"]
        return obj

    def get_role_by_id(self, role_id: int) -> Dict[str, Any]:
        result = self.request("GET", f"/setting/roles/{role_id}")
        result = self._unwrap(result)
        if not isinstance(result, dict):
            raise LMApiError(f"Unexpected role response for role ID {role_id}: {result!r}")
        return result

    def list_roles(self, size: int = 1000, offset: int = 0) -> Dict[str, Any]:
        result = self.request("GET", "/setting/roles", params={"size": size, "offset": offset})
        #result = self.request("GET", "/setting/roles")
        result = self._unwrap(result)
        if not isinstance(result, dict):
            raise LMApiError(f"Unexpected role list response: {result!r}")
        return result

    def list_all_roles(self, size: int = 1000) -> List[Dict[str, Any]]:
        offset = 0
        all_roles: List[Dict[str, Any]] = []

        while True:
            page = self.list_roles(size=size, offset=offset)
            items = page.get("items", [])
            if not isinstance(items, list):
                raise LMApiError(f"Unexpected role list 'items' payload: {items!r}")

            all_roles.extend(item for item in items if isinstance(item, dict))

            total = page.get("total")
            if total is None:
                if len(items) < size:
                    break
            else:
                if offset + len(items) >= int(total):
                    break

            if not items:
                break

            offset += size

        return all_roles

    def find_role_by_name(self, role_name: str) -> Optional[Dict[str, Any]]:
        for item in self.list_all_roles():
            if item.get("name") == role_name:
                return item
        return None

    def create_role(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.request("POST", "/setting/roles", payload=payload)
        result = self._unwrap(result)
        if not isinstance(result, dict):
            raise LMApiError(f"Unexpected create-role response: {result!r}")
        return result

    def update_role(self, role_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.request("PUT", f"/setting/roles/{role_id}", payload=payload)
        result = self._unwrap(result)
        if not isinstance(result, dict):
            raise LMApiError(f"Unexpected update-role response: {result!r}")
        return result


def build_create_payload(
    source_role: Dict[str, Any],
    *,
    target_role_name: Optional[str],
    keep_role_group_id: bool,
    target_role_group_id: Optional[int],
) -> Dict[str, Any]:
    allowed_keys = {
        "name",
        "description",
        "customHelpLabel",
        "customHelpURL",
        "twoFARequired",
        "requireEULA",
        "privileges",
        "roleGroupId",
    }

    payload = {k: copy.deepcopy(v) for k, v in source_role.items() if k in allowed_keys}

    if target_role_name:
        payload["name"] = target_role_name

    if not keep_role_group_id:
        payload.pop("roleGroupId", None)

    if target_role_group_id is not None:
        payload["roleGroupId"] = target_role_group_id

    if "name" not in payload or not payload["name"]:
        raise ValueError("Role payload is missing 'name'.")

    if "privileges" not in payload or not isinstance(payload["privileges"], list):
        raise ValueError("Role payload is missing 'privileges' or it is not a list.")

    return payload


def write_backup_file(
    backup_file: Path,
    *,
    source_company: str,
    source_role: Dict[str, Any],
    create_payload: Dict[str, Any],
) -> None:
    backup_data = {
        "backedUpAtEpochMs": int(time.time() * 1000),
        "sourceCompany": source_company,
        "roleId": source_role.get("id"),
        "roleName": source_role.get("name"),
        "rawRole": source_role,
        "createPayload": create_payload,
    }

    backup_file.parent.mkdir(parents=True, exist_ok=True)
    backup_file.write_text(json.dumps(backup_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def print_roles(roles: List[Dict[str, Any]], company: str) -> None:
    print(f"Roles in source portal: {company}")
    print("-" * 80)
    print(f"{'ID':<10} {'NAME'}")
    print("-" * 80)

    for role in sorted(roles, key=lambda x: str(x.get("name", "")).lower()):
        role_id = role.get("id", "")
        role_name = role.get("name", "")
        print(f"{str(role_id):<10} {role_name}")

    print("-" * 80)
    print(f"Total roles: {len(roles)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backup a LogicMonitor role from SOURCE portal and create/update it in DEST portal using .env_multi."
    )

    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--list", action="store_true", help="List all roles in the source portal and exit")
    action_group.add_argument("--role-id", type=int, help="Source role ID")
    action_group.add_argument("--role-name", help="Source role name")

    parser.add_argument("--backup-file", help="Path to write the role backup JSON")
    parser.add_argument("--target-role-name", help="Optional different role name for destination portal")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Update the destination role if a role with the same name already exists",
    )
    parser.add_argument(
        "--keep-role-group-id",
        action="store_true",
        help="Copy roleGroupId from source to destination. Use only if valid on destination.",
    )
    parser.add_argument(
        "--target-role-group-id",
        type=int,
        help="Explicit roleGroupId to use on destination",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show HTTP method, URL, and response status code",
    )

    args = parser.parse_args()

    if not args.list and not args.backup_file:
        parser.error("--backup-file is required unless --list is used")

    return args


def main() -> int:
    args = parse_args()

    source_client = LMClient(SOURCE, debug=args.debug)
    target_client = LMClient(DEST, debug=args.debug)

    if args.list:
        roles = source_client.list_all_roles()
        print_roles(roles, SOURCE.company)
        return 0

    if args.role_id is not None:
        source_role = source_client.get_role_by_id(args.role_id)
    else:
        source_role = source_client.find_role_by_name(args.role_name)
        if source_role is None:
            raise LMApiError(f"Source role named '{args.role_name}' was not found in {SOURCE.company}.")

        if "id" in source_role:
            source_role = source_client.get_role_by_id(int(source_role["id"]))

    create_payload = build_create_payload(
        source_role,
        target_role_name=args.target_role_name,
        keep_role_group_id=args.keep_role_group_id,
        target_role_group_id=args.target_role_group_id,
    )

    backup_path = Path(args.backup_file)
    write_backup_file(
        backup_path,
        source_company=SOURCE.company,
        source_role=source_role,
        create_payload=create_payload,
    )
    print(f"Backup written to: {backup_path}")

    target_role_name = create_payload["name"]
    existing_target_role = target_client.find_role_by_name(target_role_name)

    if existing_target_role:
        if not args.overwrite:
            raise LMApiError(
                f"Destination role '{target_role_name}' already exists in {DEST.company}. "
                "Re-run with --overwrite to update it."
            )

        target_role_id = int(existing_target_role["id"])
        result = target_client.update_role(target_role_id, create_payload)
        print(
            f"Updated destination role '{result.get('name', target_role_name)}' "
            f"(ID: {result.get('id', target_role_id)}) in {DEST.company}"
        )
    else:
        result = target_client.create_role(create_payload)
        print(
            f"Created destination role '{result.get('name', target_role_name)}' "
            f"(ID: {result.get('id', 'unknown')}) in {DEST.company}"
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (LMApiError, ValueError, requests.RequestException) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)

# EOF
