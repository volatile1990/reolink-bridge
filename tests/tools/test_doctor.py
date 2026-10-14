"""Read-only doctor regressions: local files and synthetic loopback peers only."""
import base64
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("reolink_doctor", ROOT / "tools/doctor.py")
doctor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(doctor)
SECRET = "SYNTHETIC_DOCTOR_PASSWORD_SENTINEL"
USERNAME = "SYNTHETIC_DOCTOR_USER_SENTINEL"


class Peer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.requests.append((self.command, self.path))
        if self.path != "/health":
            expected = "Basic " + base64.b64encode((USERNAME + ":" + SECRET).encode()).decode()
            if self.headers.get("Authorization") != expected:
                self.send_response(401); self.end_headers(); return
        if self.server.redirect and self.path == "/metrics":
            self.send_response(302)
            self.send_header("Location", self.server.redirect)
            self.end_headers(); return
        content = {
            "/health": {"status": "ready", "password": SECRET},
            "/metrics": {
                "status": "ready", "password": SECRET, "name": USERNAME,
                "buildInfo": {"project": "reolink-bridge", "version": "1.2.0", "sourceRevision": "a" * 40, "password": SECRET},
                "streams": [{"profile": "main", "codec": "H265", "videoReady": True, "liveVideo": True,
                             "lastVideoAgeMs": 50, "authenticationFailed": False, "address": SECRET,
                             "audio": {"available": True, "metadataCurrent": True, "sampleRate": 16000, "channels": 1, "token": SECRET}},
                            {"profile": "sub", "videoReady": True, "liveVideo": True, "lastVideoAgeMs": 50,
                             "authenticationFailed": False}],
                "source": {"alarmSubscription": {"state": "watching", "confirmedByCameraPush": True, "lastErrorType": SECRET}},
            },
            "/events?after=0&limit=1": {"currentSeq": 15, "oldestSeq": 1, "truncated": False,
                                     "source": {"connected": True, "connectionEpoch": 3, "password": SECRET},
                                     "events": [{"seq": 1, "rawAiTypes": [SECRET], "cameraId": USERNAME}]},
        }.get(self.path, {})
        payload = json.dumps(content).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload))); self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class DoctorTests(unittest.TestCase):
    def setUp(self):
        self.peer = ThreadingHTTPServer(("127.0.0.1", 0), Peer)
        self.peer.requests = []; self.peer.redirect = None
        self.thread = threading.Thread(target=self.peer.serve_forever, daemon=True)
        self.thread.start()
        self.origin = "http://127.0.0.1:%s" % self.peer.server_port

    def tearDown(self):
        self.peer.shutdown(); self.peer.server_close(); self.thread.join(timeout=3)

    def run_doctor(self, *args, input=None, credentials_env=False):
        environment = dict(os.environ)
        environment.pop("REOLINK_BRIDGE_USER", None); environment.pop("REOLINK_BRIDGE_PASSWORD", None)
        if credentials_env:
            environment.update(REOLINK_BRIDGE_USER=USERNAME, REOLINK_BRIDGE_PASSWORD=SECRET)
        result = subprocess.run([sys.executable, str(ROOT / "tools/doctor.py"), *args], input=input, text=True,
                                capture_output=True, env=environment, timeout=20)
        self.assertNotIn(SECRET, result.stdout + result.stderr)
        self.assertNotIn(USERNAME, result.stdout + result.stderr)
        return result, json.loads(result.stdout)

    def test_local_default_never_contacts_a_peer(self):
        result, data = self.run_doctor("--local", credentials_env=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.peer.requests, [])
        self.assertEqual(data["mode"], "read-only")
        self.assertRegex(data["localBuild"]["version"], r"^\d+\.\d+\.\d+")

    def test_only_allowlisted_gets_with_stdin_credentials(self):
        result, data = self.run_doctor("--bridge-url", self.origin, "--credentials-stdin", "--non-interactive",
                                     input=json.dumps({"username": USERNAME, "password": SECRET}))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.peer.requests, [("GET", "/health"), ("GET", "/metrics"), ("GET", "/events?after=0&limit=1")])
        self.assertEqual(data["metrics"]["buildInfo"]["version"], "1.2.0")
        checks = {item["check"]: item["state"] for item in data["checks"]}
        self.assertEqual(checks["HQ"], "pass"); self.assertEqual(checks["LQ"], "pass")
        self.assertEqual(checks["mainAudio"], "pass"); self.assertEqual(checks["subAudio"], "unknown")
        self.assertEqual(data["replay"]["returnedEvents"], 1)

    def test_env_credentials_and_redirect_do_not_follow(self):
        self.peer.redirect = self.origin + "/unexpected"
        result, data = self.run_doctor("--bridge-url", self.origin, "--non-interactive", credentials_env=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(data["metrics"]["httpStatus"], 302)
        self.assertNotIn(("GET", "/unexpected"), self.peer.requests)

    def test_missing_credentials_and_credential_url_are_redacted(self):
        result, data = self.run_doctor("--bridge-url", self.origin, "--non-interactive")
        self.assertEqual(result.returncode, 1); self.assertIn("error", data["bridge"])
        credential_url = self.origin.replace("http://", "http://" + USERNAME + ":" + SECRET + "@")
        result, data = self.run_doctor("--bridge-url", credential_url, "--non-interactive")
        self.assertEqual(result.returncode, 1); self.assertEqual(self.peer.requests, [])

    def test_health_and_provenance_whitelists_preserve_freshness(self):
        now = int(time.time() * 1000)
        health = {"atMs": now - 60000, "healthy": True, "captureHealthy": True, "nativeReady": True,
                  "pending": 0, "nativeError": SECRET, "bridges": {USERNAME: {"lastSuccessMs": now, "password": SECRET}}}
        safe = doctor.events_health(health, now)
        self.assertFalse(safe["healthyNow"])
        self.assertNotIn(SECRET, json.dumps(safe)); self.assertNotIn(USERNAME, json.dumps(safe))
        safe = doctor.build_info({"version": SECRET, "sourceRevision": SECRET, "project": [], "sourceDirty": False,
                                  "artifactSha256": "a" * 64, "sourceSha256": "b" * 64})
        self.assertNotIn("version", safe); self.assertEqual(safe["artifactSha256"], "a" * 64)
        self.assertEqual(doctor.safe_metrics({"status": [], "streams": [{"profile": {}}]}).get("streams"), [])

    def test_private_file_projection_and_bounded_json(self):
        with tempfile.TemporaryDirectory(prefix="synthetic-doctor-") as folder:
            directory = Path(folder); health = directory / "health.json"; manifest = directory / "release.json"
            health.write_text(json.dumps({"atMs": int(time.time() * 1000), "healthy": True, "password": SECRET}), encoding="utf-8")
            manifest.write_text(json.dumps({"schemaVersion": 1, "version": "1.2.0", "sourceSha256": "c" * 64, "password": SECRET}), encoding="utf-8")
            os.chmod(directory, 0o700); os.chmod(health, 0o600)
            result, data = self.run_doctor("--local", "--events-health-file", str(health), "--release-provenance", str(manifest))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(data["eventsHealth"]["healthyNow"])
            with mock.patch.object(doctor, "MAX_JSON_BYTES", 16):
                with self.assertRaises(ValueError): doctor.json_file(manifest)
            if os.name != "nt":
                os.chmod(health, 0o644)
                self.assertFalse(doctor.private_health_file(health))


if __name__ == "__main__":
    unittest.main()
