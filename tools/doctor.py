#!/usr/bin/env python3
"""Read-only local diagnostics; optional bridge GETs never actuate a camera.

Credentials: REOLINK_BRIDGE_USER / REOLINK_BRIDGE_PASSWORD, an interactive
prompt, or --credentials-stdin with one JSON object containing username/password.
Passwords and usernames are never command arguments or diagnostic output.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import getpass
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

MAX_JSON_BYTES = 1024 * 1024
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATES = {"ready", "waiting-for-camera", "authentication-failed", "watching", "retrying",
          "unsupported", "denied", "disconnected", "stopped", "waiting-for-source", "active",
          "starting", "unavailable", "write-failed", "software", "vaapi", "capture", "native"}
ERROR_TYPES = {"IOException", "ObjectDisposedException", "QueueBudgetExceededException",
               "CameraCommandException", "TimeoutException", "OperationCanceledException", "SubscriptionEnded"}


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and abs(value) <= 10**16


def iso_date(value):
    if not isinstance(value, str) or len(value) > 48:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def build_info(data):
    """Only validated provenance values can cross the diagnostic boundary."""
    if not isinstance(data, dict):
        return {}
    result = {}
    for name in ("schemaVersion", "sourceFiles"):
        if isinstance(data.get(name), int) and not isinstance(data[name], bool) and 0 <= data[name] <= 10**7:
            result[name] = data[name]
    if isinstance(data.get("project"), str) and data["project"] in {"reolink-bridge", "reolink-protect-events"}:
        result["project"] = data["project"]
    for name in ("version", "informationalVersion"):
        value = data.get(name)
        if isinstance(value, str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]{1,80})?", value):
            result[name] = value
    for name in ("sourceRevision", "sourceSha256", "artifactSha256"):
        value = data.get(name)
        minimum = 7 if name == "sourceRevision" else 64
        if isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9]{%d,64}" % minimum, value):
            result[name] = value.lower()
    if isinstance(data.get("sourceDirty"), bool):
        result["sourceDirty"] = data["sourceDirty"]
    if (value := iso_date(data.get("buildTime"))) is not None:
        result["buildTime"] = value
    if isinstance(data.get("runtime"), str) and re.fullmatch(r"\.NET(?: Core)? [0-9]+(?:\.[0-9]+){1,3}", data["runtime"]):
        result["runtime"] = data["runtime"]
    return result


def select(data, numeric=(), boolean=(), states=()):
    if not isinstance(data, dict):
        return {}
    result = {name: data[name] for name in numeric if number(data.get(name))}
    result.update({name: data[name] for name in boolean if isinstance(data.get(name), bool)})
    result.update({name: data[name] for name in states if isinstance(data.get(name), str) and data[name] in STATES})
    return result


def safe_metrics(data):
    if not isinstance(data, dict):
        return {}
    result = select(data, ("uptimeSeconds",), states=("status",))
    result["buildInfo"] = build_info(data.get("buildInfo", data.get("build")))
    streams = []
    for stream in data.get("streams", [])[:8] if isinstance(data.get("streams"), list) else []:
        if not isinstance(stream, dict) or not isinstance(stream.get("profile"), str) or stream["profile"] not in {"main", "sub", "extern"}:
            continue
        item = select(stream, ("width", "height", "incomingFrames", "lastVideoAgeMs", "maxArrivalGapMs", "sourceEpoch"),
                      ("videoReady", "liveVideo", "authenticationFailed"))
        item["profile"] = stream["profile"]
        if isinstance(stream.get("codec"), str) and stream["codec"] in {"H264", "H265"}:
            item["codec"] = stream["codec"]
        item["audio"] = select(stream.get("audio"), ("sampleRate", "channels"), ("available", "isAac", "metadataCurrent"))
        streams.append(item)
    result["streams"] = streams
    source = data.get("source") if isinstance(data.get("source"), dict) else {}
    alarm = select(source.get("alarmSubscription"), ("retryCount",), ("confirmedByCameraPush",), ("state",))
    raw_alarm = source.get("alarmSubscription") if isinstance(source.get("alarmSubscription"), dict) else {}
    if isinstance(raw_alarm.get("lastErrorType"), str) and raw_alarm["lastErrorType"] in ERROR_TYPES:
        alarm["lastErrorType"] = raw_alarm["lastErrorType"]
    if (timestamp := iso_date(raw_alarm.get("lastCameraPushUtc"))) is not None:
        alarm["lastCameraPushUtc"] = timestamp
    # .NET record diagnostics retain PascalCase names inside this nested object.
    queue = select(source.get("mediaInputQueue"), ("Bytes", "PeakBytes", "Items", "PeakItems", "OldestAgeMs", "Overflows"))
    result["source"] = {"alarmSubscription": alarm, "mediaInputQueue": queue}
    result["lowQualityEncoder"] = select(data.get("lowQualityEncoder"), ("encodedFrames", "failures", "outputFps", "sourceFps"),
                                         ("softwareFallback", "captureSynchronizationVerified"), ("state", "backend", "decoder"))
    result["lowQualityDecoder"] = select(data.get("lowQualityDecoder"),
                                         boolean=("hardwareDecodeRequested", "runtimeHardwareDecodeVerified", "noSilentFallback"),
                                         states=("backend", "decoderHardwareFormat"))
    result["nativeTalk"] = select(data.get("nativeTalk"), ("acceptedAac", "acceptedRtp", "rejected", "decoderErrors", "decoderRecoveries"), ("ready",))
    result["gopAudio"] = select(data.get("gopAudio"), ("activeSessions",), ("captureSynchronizationVerified",))
    event_data = data.get("events") if isinstance(data.get("events"), dict) else {}
    result["events"] = select(event_data, ("cameraPushes", "subscriptions", "queuedMessages", "queueResets"))
    result["events"]["source"] = select(event_data.get("source"), ("connectionEpoch",), ("connected",))
    result["events"]["replay"] = select(event_data.get("replay"), ("retainedPushes", "retainedBytes", "maxBytes", "failures"), ("enabled",), ("state",))
    return result


def safe_replay(data):
    if not isinstance(data, dict):
        return {}
    result = select(data, ("currentSeq", "oldestSeq"), ("truncated",))
    result["source"] = select(data.get("source"), ("connectionEpoch",), ("connected",))
    # Event contents, IDs, class tokens and source addresses are intentionally excluded.
    if isinstance(data.get("events"), list):
        result["returnedEvents"] = len(data["events"])
    return result


def json_file(path):
    with Path(path).open("rb") as handle:
        content = handle.read(MAX_JSON_BYTES + 1)
    if len(content) > MAX_JSON_BYTES:
        raise ValueError("json-size-limit")
    value = json.loads(content)
    if not isinstance(value, dict):
        raise ValueError("json-object-required")
    return value


def private_health_file(path):
    target = Path(path).absolute()
    for part in (target, *target.parents):
        if part.is_symlink() or getattr(part, "is_junction", lambda: False)():
            return False
    info = target.stat()
    if not stat.S_ISREG(info.st_mode):
        return False
    if os.name != "nt":
        parent = target.parent.stat()
        return info.st_uid == os.getuid() and info.st_mode & 0o077 == 0 and parent.st_mode & 0o077 == 0
    # Read ACLs without changing them; paths enter PowerShell only through an environment value.
    environment = dict(os.environ, REOLINK_DOCTOR_HEALTH_PATH=str(target))
    script = ("$ErrorActionPreference='Stop'; $acl=[System.IO.File]::GetAccessControl($env:REOLINK_DOCTOR_HEALTH_PATH); "
              "$allowed=@([System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value,'S-1-5-18','S-1-5-32-544'); "
              "$owner=$acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value; "
              "if($allowed -notcontains $owner){'unverified';exit}; $allowed+=@('S-1-3-4'); "
              "$grants=@($acl.Access | Where-Object {$_.AccessControlType -eq 'Allow'}); "
              "$other=@($grants | Where-Object {$allowed -notcontains $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value}); "
              "if($grants.Count -gt 0 -and $other.Count -eq 0){'private'}else{'unverified'}")
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
                            env=environment, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10, check=False)
    return result.returncode == 0 and result.stdout.strip() == b"private"


def events_health(data, now_ms):
    result = select(data, ("atMs", "pending", "importErrors", "oldestPendingAgeMs", "retryIntervals", "pendingThumbnails",
                           "failedThumbnails", "nativeWorkerAgeMs", "pollDurationMs"),
                    ("healthy", "captureHealthy", "nativeReady", "workerStopped"), ("mode",))
    age = now_ms - result["atMs"] if "atMs" in result else None
    result["fresh"] = age is not None and -5000 <= age <= 30000
    if age is not None:
        result["ageMs"] = age
    result["healthyNow"] = result["fresh"] and result.get("healthy") is True
    values = data.get("bridges")
    if isinstance(values, dict):
        result["bridges"] = [select(value, ("lastSuccessMs", "consecutiveErrors", "sourceBacklog", "replayHeadroom", "pollDurationMs"))
                             for value in list(values.values())[:64]]
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def bridge_base(value):
    url = urllib.parse.urlsplit(value)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username is not None or url.password is not None:
        raise ValueError("bridge-origin-required")
    if url.path not in {"", "/"} or url.query or url.fragment:
        raise ValueError("bridge-origin-required")
    _ = url.port  # validates the port without ever returning the supplied URL in an error
    return urllib.parse.urlunsplit((url.scheme, url.netloc, "", "", ""))


def credentials(args):
    if args.credentials_stdin:
        content = sys.stdin.buffer.read(4097)
        if len(content) > 4096:
            raise ValueError("credential-input-size")
        supplied = json.loads(content)
        if not isinstance(supplied, dict):
            raise ValueError("credential-input-shape")
        user, password = supplied.get("username"), supplied.get("password")
    else:
        user, password = os.environ.get("REOLINK_BRIDGE_USER"), os.environ.get("REOLINK_BRIDGE_PASSWORD")
        if user is None or password is None:
            if args.non_interactive or not sys.stdin.isatty():
                raise ValueError("credentials-required")
            user = input("Bridge username: ")
            password = getpass.getpass("Bridge password: ")
    if not isinstance(user, str) or not isinstance(password, str) or not user or len(user) > 128 or len(password) > 1024 or ":" in user:
        raise ValueError("credential-input-shape")
    if any(ord(char) < 32 for char in user + password):
        raise ValueError("credential-input-shape")
    return "Basic " + base64.b64encode((user + ":" + password).encode()).decode("ascii")


def request_json(opener, base, endpoint, timeout, authorization=None):
    headers = {"Accept": "application/json"}
    if authorization is not None:
        headers["Authorization"] = authorization
    request = urllib.request.Request(base + endpoint, headers=headers, method="GET")
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = response.read(MAX_JSON_BYTES + 1)
            if len(payload) > MAX_JSON_BYTES:
                return {"httpStatus": response.status, "error": "response-size-limit"}, None
            data = json.loads(payload)
            return {"httpStatus": response.status}, data if isinstance(data, dict) else None
    except urllib.error.HTTPError as error:
        # Readiness can be false with a legitimate HTTP 503 health response.
        if endpoint == "/health" and error.code == 503:
            try:
                data = json.loads(error.read(MAX_JSON_BYTES + 1))
                return {"httpStatus": 503}, data if isinstance(data, dict) else None
            except (ValueError, OSError):
                pass
        return {"httpStatus": error.code, "error": "authentication-required" if error.code in {401, 403} else "http-unavailable"}, None
    except (urllib.error.URLError, OSError, ValueError):
        return {"error": "read-unavailable"}, None


def local_metadata(root):
    result = {"project": "reolink-bridge"}
    try:
        version = ET.parse(root / "src/Neolink.Server/Neolink.Server.csproj").findtext("./PropertyGroup/Version")
        result.update(build_info({"version": version}))
        commit = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, timeout=5, check=False)
        state = subprocess.run(["git", "-C", str(root), "status", "--porcelain"], capture_output=True, timeout=5, check=False)
        if commit.returncode == 0:
            result.update(build_info({"sourceRevision": commit.stdout.decode("ascii", errors="ignore").strip()}))
        if state.returncode == 0:
            result["sourceDirty"] = bool(state.stdout.strip())
    except (OSError, ValueError, ET.ParseError, subprocess.TimeoutExpired):
        result["state"] = "metadata-unavailable"
    return result


def stream_checks(metrics):
    result = []
    for stream in metrics.get("streams", []):
        state = "pass" if stream.get("videoReady") is True and stream.get("liveVideo") is True and number(stream.get("lastVideoAgeMs")) \
            and 0 <= stream["lastVideoAgeMs"] <= 5000 and stream.get("authenticationFailed") is False else "warning"
        result.append({"check": "HQ" if stream["profile"] == "main" else "LQ" if stream["profile"] == "sub" else "external", "state": state})
        audio = stream.get("audio", {})
        result.append({"check": stream["profile"] + "Audio", "state": "pass" if audio.get("available") is True and audio.get("metadataCurrent") is True
                       else "warning" if audio.get("available") is False else "unknown"})
    alarm = metrics.get("source", {}).get("alarmSubscription", {})
    result.append({"check": "alarmSubscription", "state": "pass" if alarm.get("state") == "watching" and alarm.get("confirmedByCameraPush") is True
                   else "unknown" if alarm.get("state") == "watching" else "warning"})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="Read local metadata only; never make network requests")
    parser.add_argument("--bridge-url", help="Explicit HTTP(S) bridge origin, without embedded credentials")
    parser.add_argument("--credentials-stdin", action="store_true", help="Read a single synthetic/private credential JSON object from stdin")
    parser.add_argument("--non-interactive", action="store_true", help="Use only environment/stdin credentials")
    parser.add_argument("--events-health-file", type=Path, help="Optional private local adapter health.json; output is allowlisted")
    parser.add_argument("--build-manifest", type=Path)
    parser.add_argument("--release-provenance", type=Path)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args(argv)
    if args.local and args.bridge_url:
        parser.error("--local cannot be combined with --bridge-url")
    if not math.isfinite(args.timeout) or not 0.1 <= args.timeout <= 30:
        parser.error("--timeout must be between 0.1 and 30 seconds")
    report = {"schemaVersion": 1, "mode": "read-only", "localBuild": local_metadata(PROJECT_ROOT), "checks": []}
    for name, path in (("buildManifest", args.build_manifest), ("releaseProvenance", args.release_provenance)):
        if path is not None:
            try:
                report[name] = build_info(json_file(path))
            except (OSError, ValueError):
                report[name] = {"error": "metadata-unavailable"}
    if args.events_health_file is not None:
        try:
            if not private_health_file(args.events_health_file):
                raise ValueError("private-health-file-required")
            report["eventsHealth"] = events_health(json_file(args.events_health_file), int(time.time() * 1000))
            report["checks"].append({"check": "eventsAdapter", "state": "pass" if report["eventsHealth"]["healthyNow"] else "warning"})
        except (OSError, ValueError, subprocess.TimeoutExpired):
            report["eventsHealth"] = {"error": "private-health-file-unavailable"}
            report["checks"].append({"check": "eventsAdapter", "state": "warning"})
    if args.bridge_url:
        try:
            base = bridge_base(args.bridge_url)
            authorization = credentials(args)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
            summary, data = request_json(opener, base, "/health", args.timeout)
            report["health"] = summary | select(data, states=("status",))
            report["checks"].append({"check": "bridgeHealth", "state": "pass" if summary.get("httpStatus") == 200 else "warning"})
            summary, data = request_json(opener, base, "/metrics", args.timeout, authorization)
            report["metrics"] = summary | safe_metrics(data)
            report["checks"].extend(stream_checks(report["metrics"]))
            summary, data = request_json(opener, base, "/events?after=0&limit=1", args.timeout, authorization)
            report["replay"] = summary | safe_replay(data)
            report["checks"].append({"check": "eventReplay", "state": "pass" if summary.get("httpStatus") == 200 else "warning"})
        except (OSError, ValueError):
            report["bridge"] = {"error": "bridge-origin-or-credentials-unavailable"}
            report["checks"].append({"check": "bridgeConnection", "state": "warning"})
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if any(check["state"] == "warning" for check in report["checks"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
