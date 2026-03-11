#!/usr/bin/env python3

'''
#RUN:
export BASIC_USER="ryan"
export BASIC_PASS="supersecret"
# Optional:
# export INCLUDE_ENV="1"
# export ALLOW_CORS="1"

python3 auth_api_server.py 8000


#Test with curl:
# GET file (if present)
curl -u ryan:supersecret http://localhost:8000/

# POST JSON to API echo endpoint
curl -u ryan:supersecret http://localhost:8000/api/info
curl -u ryan:supersecret -H "Content-Type: application/json" -d '{"ping":"pong"}' http://localhost:8000/api/echo

'''


#!/usr/bin/env python3
import base64
import json
import os
import platform
import socket
import sys
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler

# ---- Config via env ----
USERNAME = os.environ.get("BASIC_USER", "user")
PASSWORD = os.environ.get("BASIC_PASS", "pass")
REALM = os.environ.get("BASIC_REALM", "Restricted")
ALLOW_CORS = os.environ.get("ALLOW_CORS", "1") == "1"
INCLUDE_ENV = os.environ.get("INCLUDE_ENV", "0") == "1"

START_TIME = time.time()

def _auth_ok(header_value: str) -> bool:
    if not header_value or not header_value.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header_value.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False
    return decoded == f"{USERNAME}:{PASSWORD}"

def get_os_info(include_env: bool = False) -> dict:
    info = {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "python": {
            "version": sys.version,
            "executable": sys.executable,
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "platform": platform.platform(),
        },
        "uptime_seconds": round(time.time() - START_TIME, 3),
        "time": {
            "epoch": time.time(),
            "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }

    if include_env:
        # Safer than dumping everything; tweak to your needs.
        allow_prefixes = ("CI", "GITHUB", "BUILD", "HOST", "USER", "SHELL", "LANG")
        env_filtered = {k: v for k, v in os.environ.items() if k.startswith(allow_prefixes)}
        info["env"] = env_filtered

    return info

class AuthApiHandler(SimpleHTTPRequestHandler):
    # ----- CORS -----
    def end_headers(self):
        if ALLOW_CORS:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self):
        # Common dev choice: allow preflight without auth
        self.send_response(204)
        self.end_headers()

    # ----- Auth gate -----
    def _require_auth(self) -> bool:
        if _auth_ok(self.headers.get("Authorization")):
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", f'Basic realm="{REALM}", charset="UTF-8"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps({"error": "auth_required"}).encode("utf-8"))
        return False

    # ----- Response helpers -----
    def _json_response(self, status: int, payload: dict):
        data = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    # ----- Routes -----
    def do_GET(self):
        if self.path == "/api/info":
            if not self._require_auth():
                return
            return self._json_response(200, get_os_info(include_env=INCLUDE_ENV))

        if self.path == "/api/echo":
            if not self._require_auth():
                return
            body = self._read_body()
            try:
                body_json = json.loads(body.decode("utf-8")) if body else None
            except Exception:
                body_json = None

            return self._json_response(200, {
                "method": self.command,
                "path": self.path,
                "headers": {k: v for k, v in self.headers.items()},
                "body_raw": body.decode("utf-8", errors="replace"),
                "body_json": body_json,
            })

        # For non-/api paths: serve files, but require auth
        if not self._require_auth():
            return
        return super().do_GET()

    def do_HEAD(self):
        if not self._require_auth():
            return
        return super().do_HEAD()

    def do_POST(self):
        # For API testing, make POST hit /api/echo (or extend as needed)
        if self.path != "/api/echo":
            self.send_response(404)
            self.end_headers()
            return
        return self.do_GET()

    # If you want PUT/PATCH/DELETE, route them similarly:
    def do_PUT(self): return self.do_POST()
    def do_PATCH(self): return self.do_POST()
    def do_DELETE(self): return self.do_POST()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", "-b", default="", help="Bind address, e.g. 127.0.0.1")
    parser.add_argument("port", nargs="?", type=int, default=8000)
    args = parser.parse_args()

    httpd = HTTPServer((args.bind, args.port), AuthApiHandler)
    print(f"Serving on http://{args.bind or '0.0.0.0'}:{args.port}")
    print("Endpoints: /api/info, /api/echo (Basic Auth required)")
    httpd.serve_forever()

