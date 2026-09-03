#!/usr/bin/env python3
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

session_count = 0
benchmark_session = None


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
        elif self.path.startswith("/session/ses_respondent/message?"):
            self.send_json([
                {"info": {"id": "msg_respondent_step", "role": "assistant", "parentID": "msg_respondent_user", "time": {"created": 20}}, "parts": [
                    {"type": "reasoning", "time": {"start": 20, "end": 21}},
                    {"type": "tool", "tool": "bash", "state": {
                        "status": "completed", "input": {"command": "just verify"},
                        "time": {"start": 22, "end": 23},
                    }},
                ]},
                {"info": {"id": "msg_respondent", "role": "assistant", "parentID": "msg_respondent_user", "time": {"created": 24}}, "parts": [
                    {"type": "text", "text": "respondent reply"},
                ]},
            ])
        elif self.path.startswith("/session/ses_worker/message?"):
            self.send_json([
                {"info": {"id": "msg_worker_step", "role": "assistant", "parentID": "msg_worker_user", "time": {"created": 10}}, "parts": [
                    {"type": "reasoning", "time": {"start": 10, "end": 11}},
                    {"type": "tool", "tool": "read", "state": {
                        "status": "completed", "input": {"filePath": "goal.md"},
                        "time": {"start": 12, "end": 13},
                    }},
                ]},
                {"info": {"id": "msg_fake", "role": "assistant", "parentID": "msg_worker_user", "time": {"created": 14}}, "parts": [
                    {"type": "text", "text": "完成任务。"},
                ]},
            ])
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
        global session_count, benchmark_session
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/session?"):
            if body.get("title", "").startswith("labflow:bench:"):
                if "parentID" in body or "permission" not in body:
                    self.send_error(400)
                    return
                benchmark_session = "ses_respondent"
                with open(os.path.join(os.getcwd(), "benchmark-session.json"), "w", encoding="utf-8") as output:
                    json.dump(body, output)
                self.send_json({"id": benchmark_session})
                return
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
        elif self.path.startswith("/session/ses_respondent/message?"):
            with open(os.path.join(os.getcwd(), "benchmark-messages.jsonl"), "a", encoding="utf-8") as output:
                output.write(json.dumps(body) + "\n")
            self.send_json({
                "info": {"id": "msg_respondent", "parentID": "msg_respondent_user"},
                "parts": [
                    {"type": "text", "text": "respondent reply"},
                ],
            })
        elif self.path.startswith("/session/ses_worker/message?"):
            if not body.get("agent", "").startswith("researcher."):
                self.send_error(400)
                return
            with open(os.path.join(os.getcwd(), "answer.md"), "w", encoding="utf-8") as output:
                output.write("fake answer\n")
            self.send_json({
                "info": {"id": "msg_fake", "parentID": "msg_worker_user"},
                "parts": [
                    {"type": "text", "text": "完成任务。"},
                ],
            })
        else:
            self.send_error(404)

    def do_DELETE(self):
        global benchmark_session
        if self.path.startswith("/session/ses_respondent?") and benchmark_session:
            benchmark_session = None
            with open(os.path.join(os.getcwd(), "benchmark-deleted"), "w", encoding="utf-8") as output:
                output.write("deleted\n")
            self.send_json({"ok": True})
        else:
            self.send_error(404)

    def do_PATCH(self):
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path.startswith("/session/ses_worker?") and isinstance(body.get("title"), str):
            with open(os.path.join(os.getcwd(), "session-titles.jsonl"), "a", encoding="utf-8") as output:
                output.write(json.dumps(body) + "\n")
            self.send_json({"id": "ses_worker", "title": body["title"]})
        else:
            self.send_error(404)


parser = argparse.ArgumentParser()
parser.add_argument("--hostname", default="127.0.0.1")
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()
with open(os.path.join(os.getcwd(), "backend-starts"), "a", encoding="utf-8") as starts:
    starts.write("started\n")
ThreadingHTTPServer((args.hostname, args.port), Handler).serve_forever()
