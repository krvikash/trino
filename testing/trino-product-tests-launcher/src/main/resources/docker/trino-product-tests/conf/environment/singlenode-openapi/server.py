#!/usr/bin/env python

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys

class ResultsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/static/rows":
            self.static_rows()
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
        else:
            self.send_error(404, "Not Found")
            self.end_headers()

    def static_rows(self):
        self.send_response(200)
        payload = [
            { "string": "Hello World!" },
            { "string": "Goodbye World!" },
        ]
        data = json.dumps(payload).encode("utf-8")
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

if __name__ == "__main__":
    server = HTTPServer(("", 3000), ResultsHandler)
    server.serve_forever()
