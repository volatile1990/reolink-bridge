"""Real Synology Docker/Compose adapter; secrets stay in process memory/private receipts."""
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import time
import uuid
import zipfile

from reolink_deploy_engine import GuardError, digest, encoded, need

try:
    import fcntl
except ImportError:
    fcntl = None

DEFAULT_IPS = tuple("192.168.30.%d" % value for value in range(210, 216))


class DockerNas:
    def __init__(self, settings=None):
        settings = settings or {}
        self.scope = Path(settings.get("scope", "/volume1/docker/reolink-bridge-pilot"))
        self.ips = tuple(settings.get("ips", DEFAULT_IPS))
        need(self.ips == DEFAULT_IPS, "six_existing_camera_scope_required")
        self.project = settings.get("project", "reolink-bridge-pilot")
        need(self.project == "reolink-bridge-pilot", "existing_project_required")
        self.base = self.scope / "pilot-compose.yaml"
        self.state_dir = Path(settings.get("watchdogDirectory", "/var/lib/reolink-camera-resilience"))
        self.runtime_uid = settings.get("runtimeUid", 1026)
        need(type(self.runtime_uid) is int and self.runtime_uid > 0, "runtime_uid_invalid")
        need(self.scope.is_absolute() and self.scope.is_dir() and not self.scope.is_symlink(), "existing_scope_required")
        self.docker = shutil.which("docker") or "/usr/local/bin/docker"
        self._compose = None

    def command(self, args, data=None, cwd=None, timeout=60):
        result = subprocess.run(args, input=data, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        need(result.returncode == 0, "subprocess_failed_" + Path(args[0]).name)
        return result.stdout

    def scoped(self, path):
        candidate = Path(path)
        need(not candidate.is_symlink(), "scope_symlink_rejected")
        resolved, root = candidate.resolve(), self.scope.resolve()
        need(resolved == root or root in resolved.parents, "path_outside_deployment_scope")
        return resolved

    def compose(self, entry=None):
        if self._compose is None:
            for executable in ([self.docker, "compose"], ["/usr/local/bin/docker-compose"], ["docker-compose"]):
                try:
                    self.command(executable + ["version"], timeout=10)
                    self._compose = executable
                    break
                except (GuardError, OSError):
                    pass
            need(self._compose is not None, "docker_compose_required")
        return self._compose + ["--project-directory", str(self.scope), "-p", self.project, "-f", str(self.base)]

    def image(self, reference):
        rows = json.loads(self.command([self.docker, "image", "inspect", reference]))
        need(len(rows) == 1, "single_image_required")
        return rows[0]

    def resolved(self, entry=None):
        return json.loads(self.command(self.compose() + ["config", "--format", "json"], cwd=str(self.scope)))

    def inventory(self):
        ids = self.command([self.docker, "ps", "-q", "--filter", "label=com.docker.compose.project=" + self.project]).decode().split()
        items = json.loads(self.command([self.docker, "inspect"] + ids)) if ids else []
        resolved = self.resolved()
        rows = []
        compose_hash = digest(self.read_compose())
        extra = self.scope / ".env"
        hashes = {str(self.base): compose_hash}
        if extra.is_file():
            hashes[str(extra)] = digest(self.scoped(extra).read_bytes())
        for item in items:
            matches = [network for network in item.get("NetworkSettings", {}).get("Networks", {}).values() if network.get("IPAddress") in self.ips]
            if not matches:
                continue
            need(len(matches) == 1, "ambiguous_camera_network")
            network = matches[0]
            labels = item["Config"].get("Labels") or {}
            need(labels.get("com.docker.compose.project") == self.project, "container_project_drift")
            need(labels.get("com.docker.compose.project.config_files") == str(self.base), "single_existing_compose_required")
            need(self.scoped(labels.get("com.docker.compose.project.working_dir", "")) == self.scope.resolve(), "compose_directory_drift")
            service_name = labels.get("com.docker.compose.service")
            service = resolved.get("services", {}).get(service_name)
            need(isinstance(service, dict), "compose_service_missing")
            mounts = [mount for mount in item.get("Mounts", []) if mount.get("Destination") == "/run/secrets/bridge-config.json"]
            need(len(mounts) == 1 and mounts[0].get("Type") == "bind" and mounts[0].get("RW") is False, "readonly_config_mount_required")
            path = self.scoped(mounts[0]["Source"])
            raw = path.read_bytes()
            config = json.loads(raw)
            onvif = config.get("onvif") or {}
            mac = network.get("MacAddress", "").lower()
            need(onvif.get("advertised_host") == network["IPAddress"] and onvif.get("mac", "").lower() == mac, "configuration_identity_drift")
            need(len(config.get("cameras", [])) == 1 and config.get("users"), "single_authenticated_camera_required")
            host = item["HostConfig"]
            need(item["State"].get("Running") is True and host.get("ReadonlyRootfs") is True and host.get("Privileged") is False, "running_unprivileged_readonly_required")
            need(host.get("RestartPolicy", {}).get("Name") == "always" and service.get("restart") == "always", "existing_restart_policy_required")
            need(str(item["Config"].get("User", "")).split(":")[0] == str(self.runtime_uid), "runtime_uid_drift")
            need(service.get("read_only") is True and service.get("image") == item["Config"]["Image"], "resolved_image_or_readonly_drift")
            static_ips = [value.get("ipv4_address") for value in (service.get("networks") or {}).values() if isinstance(value, dict)]
            need(network["IPAddress"] in static_ips, "compose_static_ip_drift")
            declared = [value for value in service.get("volumes", []) if value.get("target") == "/run/secrets/bridge-config.json"]
            need(len(declared) == 1 and self.scoped(declared[0]["source"]) == path and declared[0].get("read_only") is True, "compose_config_mount_drift")
            for declared_key, actual_key in (("command", "Cmd"), ("entrypoint", "Entrypoint"), ("user", "User")):
                if service.get(declared_key) is not None:
                    need(str(service[declared_key]) == str(item["Config"].get(actual_key)), "compose_runtime_drift")
            actual_env = dict(value.split("=", 1) for value in item["Config"].get("Env", []) if "=" in value)
            need(all(actual_env.get(key) == str(value) for key, value in (service.get("environment") or {}).items()), "compose_environment_drift")
            driver = json.loads(self.command([self.docker, "network", "inspect", network["NetworkID"]]))[0].get("Driver")
            need(driver == "macvlan", "existing_macvlan_required")
            if onvif.get("low_quality_encoder") == "vaapi":
                device = onvif.get("low_quality_device", "/dev/dri/renderD128")
                need(re.fullmatch(r"/dev/dri/renderD[0-9]{1,4}", device), "render_device_invalid")
                need(any(value.get("PathOnHost") == device and value.get("PathInContainer") == device and value.get("CgroupPermissions") == "rwm" for value in host.get("Devices", [])), "existing_render_device_required")
                need("937" in (host.get("GroupAdd") or []), "existing_render_group_required")
            rows.append({"ip": network["IPAddress"], "id": item["Id"], "name": item["Name"].lstrip("/"), "imageId": item["Image"], "imageRef": item["Config"]["Image"],
                         "directory": str(self.scope), "compose": [str(self.base)], "project": self.project, "service": service_name, "config": str(path), "mac": mac,
                         "network": network["NetworkID"], "inspect": item, "configuration": config, "resolved": resolved, "configSha256": digest(raw), "composeSha256": dict(hashes)})
        need(len(rows) == len(self.ips) and {row["ip"] for row in rows} == set(self.ips), "six_running_bridges_required")
        return sorted(rows, key=lambda row: row["ip"])

    def sample(self, entry):
        config = entry["configuration"]
        user = config["users"][0]
        auth = base64.b64encode((user["name"] + ":" + user["pass"]).encode()).decode()
        curl = 'url = "http://127.0.0.1:%d/metrics"\nheader = "Authorization: Basic %s"\n' % (config["onvif"].get("port", 8080), auth)
        raw = self.command([self.docker, "exec", "-i", entry["id"], "curl", "--silent", "--show-error", "--fail", "--max-time", "8", "--config", "-"], curl.encode(), timeout=15)
        need(len(raw) <= 2 * 1024 * 1024, "metrics_budget_exceeded")
        metrics = json.loads(raw)
        profiles = config["onvif"]["profiles"]
        expected = ["main"] if len(profiles) == 1 else ["main", "sub"]
        by_token = {stream["profile"]: stream for stream in metrics.get("streams", [])}
        need(metrics.get("status") == "ready" and set(by_token) == set(expected), "source_not_ready")
        need(entry["inspect"]["State"].get("Health", {}).get("Status") == "healthy", "container_not_healthy")
        streams = []
        for token, profile in zip(expected, profiles):
            stream = by_token[token]
            audio = stream.get("audio") or {}
            need(stream.get("liveVideo") is True and stream.get("videoReady") is True and stream.get("authenticationFailed") is not True and 0 <= stream.get("lastVideoAgeMs", 999999) <= 5000, "source_video_not_fresh")
            need(stream.get("codec") == profile["codec"] and stream.get("width") == profile["width"] and stream.get("height") == profile["height"], "source_geometry_or_codec_drift")
            need(audio.get("available") is True and audio.get("metadataCurrent") is True and audio.get("isAac") is True and audio.get("sampleRate") == 16000 and audio.get("channels") == 1, "source_aac_not_fresh")
            need(type(stream.get("totalAccessUnits")) is int, "source_counter_invalid")
            streams.append({"profile": token, "totalAccessUnits": stream["totalAccessUnits"]})
        encoder, decoder = metrics.get("lowQualityEncoder"), metrics.get("lowQualityDecoder")
        if config["onvif"].get("low_quality_encoder") == "vaapi":
            need(isinstance(encoder, dict) and encoder.get("state") == "active" and encoder.get("encoder") == "hevc_vaapi" and encoder.get("failures") == 0 and encoder.get("softwareFallback") is False, "hardware_encoder_not_ready")
            if config["onvif"].get("low_quality_decoder", "software") == "vaapi":
                need(isinstance(decoder, dict) and decoder.get("backend") == "vaapi" and decoder.get("hardwareDecodeRequested") is True and decoder.get("noSilentFallback") is True and encoder.get("decoder") == "vaapi", "hardware_decoder_selection_drift")
        else:
            need(encoder is None and decoder is None, "native_passthrough_conversion_present")
        replay = (metrics.get("events") or {}).get("replay") or {}
        if config["onvif"].get("event_replay_path"):
            need(replay.get("enabled") is True and replay.get("state") == "ready" and replay.get("failures") == 0, "durable_replay_not_ready")
        need((metrics.get("events") or {}).get("subscriptions") == 1, "event_subscription_not_ready")
        return {"at": self.monotonic(), "streams": streams}

    def read_compose(self):
        return self.scoped(self.base).read_bytes()

    def read_config(self, entry):
        return self.scoped(entry["config"]).read_bytes()

    def atomic(self, path, value, expected):
        path = self.scoped(path)
        need(path.is_file() and not path.is_symlink() and path.read_bytes() == expected, "atomic_file_cas_failed")
        metadata = path.stat()
        temporary = path.with_name("." + path.name + ".deploy-" + uuid.uuid4().hex)
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(value); stream.flush(); os.fsync(stream.fileno())
            os.chown(str(temporary), metadata.st_uid, metadata.st_gid)
            os.chmod(str(temporary), stat.S_IMODE(metadata.st_mode))
            need(path.read_bytes() == expected, "atomic_file_concurrent_drift")
            os.replace(str(temporary), str(path))
        finally:
            if temporary.exists():
                temporary.unlink()

    def write_compose(self, value, expected):
        self.atomic(self.base, value, expected)

    def recreate(self, entry):
        self.command(self.compose() + ["up", "-d", "--no-deps", "--no-build", "--pull", "never", "--force-recreate", entry["service"]], cwd=str(self.scope), timeout=120)

    @contextlib.contextmanager
    def maintenance(self):
        need(fcntl is not None, "linux_flock_required")
        need(self.state_dir.is_dir() and not self.state_dir.is_symlink() and self.state_dir.stat().st_uid == 0, "watchdog_directory_invalid")
        lock_path, marker = self.state_dir / "watchdog.lock", self.state_dir / "maintenance"
        need(not lock_path.is_symlink(), "watchdog_lock_symlink")
        with lock_path.open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            token = uuid.uuid4().hex.encode()
            fd = None
            try:
                need(not marker.exists() and not marker.is_symlink(), "maintenance_already_owned")
                fd = os.open(str(marker), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    fd = None; stream.write(token); stream.flush(); os.fsync(stream.fileno())
                yield
            finally:
                if fd is not None:
                    os.close(fd)
                if marker.is_file() and not marker.is_symlink() and marker.read_bytes() == token:
                    marker.unlink()
                fcntl.flock(handle, fcntl.LOCK_UN)

    def create_receipt(self, purpose):
        path = self.scope / (".reolink-deploy-" + uuid.uuid4().hex)
        path.mkdir(mode=0o700)
        os.chmod(str(path), 0o700)
        need(path.stat().st_uid == 0 and path.stat().st_gid == 0, "root_owned_receipt_required")
        return str(path)

    def receipt_path(self, receipt, filename):
        root = Path(receipt)
        need(root.parent == self.scope and re.fullmatch(r"\.reolink-deploy-[0-9a-f]{32}", root.name), "receipt_outside_scope")
        need(root.is_dir() and not root.is_symlink(), "receipt_directory_invalid")
        meta = root.stat()
        need(meta.st_uid == 0 and meta.st_gid == 0 and stat.S_IMODE(meta.st_mode) == 0o700, "private_receipt_permissions")
        need(isinstance(filename, str) and re.fullmatch(r"[A-Za-z0-9_.-]+", filename), "receipt_filename_invalid")
        return root / filename

    def write_private(self, receipt, filename, value):
        path = self.receipt_path(receipt, filename)
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(value); stream.flush(); os.fsync(stream.fileno())

    def read_private(self, receipt, filename):
        path = self.receipt_path(receipt, filename)
        need(path.is_file() and not path.is_symlink(), "private_receipt_file_invalid")
        meta = path.stat()
        need(meta.st_uid == 0 and meta.st_gid == 0 and stat.S_IMODE(meta.st_mode) == 0o600, "private_receipt_file_permissions")
        return path.read_bytes()

    def read_json(self, receipt, filename):
        return json.loads(self.read_private(receipt, filename))

    def replace_private(self, receipt, filename, value, expected):
        path = self.receipt_path(receipt, filename)
        need(self.read_private(receipt, filename) == expected, "private_checkpoint_cas_failed")
        self.atomic(path, value, expected)

    def monotonic(self):
        return time.monotonic()

    def sleep(self, seconds):
        time.sleep(seconds)

    def build(self, payload):
        """Run the owned isolated audit before returning a deployable descriptor."""
        from reolink_deploy_build import AuditBuilder
        return AuditBuilder(self).run(payload)
