#!/usr/bin/env python3

'''
export BASIC_USER="ryan"
export BASIC_PASS="supersecret"
python3 auth_http_server.py 8000

Then visit http://localhost:8000 and your browser will prompt for credentials.

'''

import base64
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler

USERNAME = os.environ.get("BASIC_USER", "user")
PASSWORD = os.environ.get("BASIC_PASS", "pass")
REALM = os.environ.get("BASIC_REALM", "Restricted")

def _auth_ok(header_value: str) -> bool:
    if not header_value or not header_value.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header_value.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        return False
    return decoded == f"{USERNAME}:{PASSWORD}"

class BasicAuthHandler(SimpleHTTPRequestHandler):
    def do_HEAD(self):
        if not self._require_auth():
            return
        super().do_HEAD()

    def do_GET(self):
        if not self._require_auth():
            return
        super().do_GET()

    def _require_auth(self) -> bool:
        if _auth_ok(self.headers.get("Authorization")):
            return True
        self.send_response(401)
        self.send_header("WWW-Authenticate", f'Basic realm="{REALM}", charset="UTF-8"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Authentication required.\n")
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", "-b", default="", help="Bind address, e.g. 127.0.0.1")
    parser.add_argument("port", nargs="?", type=int, default=8000)
    args = parser.parse_args()

    httpd = HTTPServer((args.bind, args.port), BasicAuthHandler)
    print(f"Serving on http://{args.bind or '0.0.0.0'}:{args.port} (Basic Auth enabled)")
    httpd.serve_forever()
