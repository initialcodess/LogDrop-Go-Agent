"""Stand-in traces gateway for the proof: keeps every request body; GET /requests returns them (base64 JSON)."""

import base64
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

bodies = []


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        bodies.append({"key": self.headers.get("x-logdrop-ingest-key", ""), "gzip": self.headers.get(
            "Content-Encoding") == "gzip", "body": base64.b64encode(body).decode()})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        data = json.dumps(bodies).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("0.0.0.0", 4318), Handler).serve_forever()
