#!/usr/bin/env python3
"""Local webhook receiver for Alertmanager — prints every alert payload.

Run:  python3 deploy/alertmanager/webhook_receiver.py
Then point alertmanager.yml at  http://localhost:8088/alert

In production, replace this with a real notifier integration
(DingTalk/Feishu/Slack webhook or an ops queue).
"""
import json
import http.server
import threading


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            print(f"[bad payload] {e}: {body[:200]}")
        else:
            for alert in payload.get("alerts", []):
                labels = alert.get("labels", {})
                annotations = alert.get("annotations", {})
                status = alert.get("status", "?")
                print(
                    f"[{status.upper()}] {labels.get('severity', '?')} "
                    f"{labels.get('alertname', '?')} "
                    f"layer={labels.get('layer', '-')} "
                    f"scope={labels.get('scope', '-')} "
                    f"| {annotations.get('summary', '')}"
                )
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass  # silence default request logging


if __name__ == "__main__":
    server = http.server.ThreadingHTTPServer(("0.0.0.0", 8088), Handler)
    print("webhook receiver listening on :8088/alert")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
