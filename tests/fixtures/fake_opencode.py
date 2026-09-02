#!/usr/bin/env python3
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

session_count = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, value):
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/global/health"):
            self.send_json({"healthy": True, "version": "fake"})
        elif self.path.startswith("/event"):
            body = b'data: {"type":"server.connected","properties":{}}\n\n'
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self):
        global session_count
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/session?"):
            session_count += 1
            if session_count == 1:
                if body.get("title") != "lab-ob":
                    self.send_error(400)
                    return
                self.send_json({"id": "ses_observer"})
            else:
                if body.get("parentID") != "ses_observer" or "permission" in body:
                    self.send_error(400)
                    return
                self.send_json({"id": "ses_worker"})
        elif self.path.startswith("/session/ses_worker/message?"):
            if not body.get("agent", "").startswith("researcher."):
                self.send_error(400)
                return
            with open(os.path.join(os.getcwd(), "answer.md"), "w", encoding="utf-8") as output:
                output.write("fake answer\n")
            self.send_json({
                "info": {"id": "msg_fake"},
                "parts": [{"type": "text", "text": "完成任务。"}],
            })
        else:
            self.send_error(404)


parser = argparse.ArgumentParser()
parser.add_argument("--hostname", default="127.0.0.1")
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()
with open(os.path.join(os.getcwd(), "backend-starts"), "a", encoding="utf-8") as starts:
    starts.write("started\n")
ThreadingHTTPServer((args.hostname, args.port), Handler).serve_forever()
