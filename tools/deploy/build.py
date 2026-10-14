"""Frozen-source build and isolated Linux/GPU audit; Python 3.8 standard library."""
import base64
import hashlib
import io
import json
import os
from pathlib import PurePosixPath
import re
import shutil
import stat
import subprocess
import uuid
import zipfile

from reolink_deploy_engine import GuardError, digest, encoded, need, projected

OWNER_LABEL = "reolink.deploy.owner"


def frozen_archive(payload):
    source = dict(payload.get("sourceManifest") or payload.get("source") or {})
    if "sourceManifest" in payload:
        need(source.get("sourceDirty") is False and source.get("canonicalLineEndings") is True, "clean_canonical_source_manifest_required")
    source["archiveSha256"] = payload.get("archiveSha256", source.get("archiveSha256"))
    source["imageTag"] = payload.get("imageTag", source.get("imageTag") or "reolink-bridge:managed-" + str(source.get("sourceSha256", ""))[:12])
    need(isinstance(source.get("version"), str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?", source["version"]), "source_version_invalid")
    for key in ("sourceSha256", "archiveSha256"):
        need(isinstance(source.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", source[key]), "source_hash_invalid")
    need(isinstance(source.get("imageTag"), str) and re.fullmatch(r"reolink-bridge:[A-Za-z0-9_.-]{1,100}", source["imageTag"]), "source_image_tag_invalid")
    need(type(source.get("sourceFiles")) is int and 1 <= source["sourceFiles"] <= 10000, "source_file_count_invalid")
    if "sourceRevision" in source:
        need(isinstance(source["sourceRevision"], str) and re.fullmatch(r"[0-9a-f]{40}", source["sourceRevision"]), "source_git_revision_invalid")
    try:
        raw = base64.b64decode(payload["archiveBase64"], validate=True)
    except (KeyError, ValueError, TypeError):
        raise GuardError("source_archive_encoding_invalid") from None
    need(len(raw) <= 32 * 1024 * 1024 and digest(raw) == source["archiveSha256"], "source_archive_hash_mismatch")
    forbidden_parts = {".git", "secrets", "state", "recordings", "snapshots", "diagnostics", "bin", "obj", "__pycache__"}
    forbidden_names = {".env", "config.json", "config.toml", "users.json", "settings.json", "secret.key"}
    files = []
    hasher = hashlib.sha256()
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            infos = archive.infolist()
            need(len(infos) == source["sourceFiles"] and len({info.filename for info in infos}) == len(infos), "source_archive_members_mismatch")
            need(sum(info.file_size for info in infos) <= 128 * 1024 * 1024, "source_archive_expansion_budget")
            for info in sorted(infos, key=lambda row: row.filename):
                path = PurePosixPath(info.filename)
                need(bool(path.parts) and not path.is_absolute() and ".." not in path.parts and "\\" not in info.filename and ":" not in info.filename and not info.is_dir(), "unsafe_source_archive_member")
                need(not set(path.parts) & forbidden_parts and path.name not in forbidden_names and (not path.name.startswith(".env.") or path.name == ".env.example"), "private_source_archive_member")
                need(stat.S_IFMT(info.external_attr >> 16) in (0, stat.S_IFREG), "source_archive_link_rejected")
                data = archive.read(info)
                hasher.update(info.filename.encode("utf-8") + b"\0"); hasher.update(hashlib.sha256(data).digest())
                files.append((info.filename, data))
    except (zipfile.BadZipFile, UnicodeError):
        raise GuardError("invalid_source_archive") from None
    need(hasher.hexdigest() == source["sourceSha256"], "exact_source_digest_mismatch")
    names = {name for name, _ in files}
    need({"Dockerfile", "tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj", "examples/config.pilot.json"} <= names, "audit_source_files_missing")
    return source, files


def audit_recipe(original, owner):
    need(re.fullmatch(r"[0-9a-f]{32}", owner), "audit_owner_invalid")
    copy_marker = "COPY src/ ./src/"
    build_marker = "# The upstream project references Microsoft.AspNetCore.App, even in headless mode."
    need(original.count(copy_marker) == 1 and original.count(build_marker) == 1 and "AS audit_tests" not in original, "dockerfile_audit_anchor_changed")
    recipe = original.replace(copy_marker, copy_marker + "\nCOPY tests/ ./tests/\nCOPY examples/config.pilot.json ./examples/config.pilot.json")
    recipe = recipe.replace(build_marker, "RUN dotnet build tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release "
                            "-p:UseAppHost=false -p:Version=${BRIDGE_VERSION} -p:SourceRevisionId=${BRIDGE_SOURCE_REVISION}\n\n" + build_marker)
    return recipe + "\nFROM runtime AS audit_tests\nUSER root\nCOPY --from=build /src/tests/Neolink.ProtectTests/bin/Release/net10.0/ /tests/\n" \
        "COPY --from=build /src/examples/config.pilot.json /examples/config.pilot.json\nLABEL " + OWNER_LABEL + '=\"' + owner + '\"\n' \
        "USER $APP_UID\nENTRYPOINT [\"dotnet\"]\nCMD [\"/tests/Neolink.ProtectTests.dll\"]\n"


def isolated_command(docker, owner, name, image, command, detached=True):
    result = [docker, "run", "--detach" if detached else "--rm", "--name", name, "--label", OWNER_LABEL + "=" + owner,
              "--network", "none", "--memory", "1g", "--cpuset-cpus", "0,1", "--pids-limit", "256", "--ulimit", "nproc=256:256", "--user", "1026:100",
              "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--tmpfs", "/tmp:rw,nosuid,nodev,size=512m,mode=1777",
              "--no-healthcheck", "--device", "/dev/dri/renderD128:/dev/dri/renderD128", "--group-add", "937",
              "--log-driver", "json-file", "--log-opt", "max-size=10m", "--log-opt", "max-file=1", "--entrypoint", command[0], image]
    return result + command[1:]


def process_limiter(host):
    pids = host.get("PidsLimit")
    if type(pids) is int and pids == 256:
        return "cgroup_pids"
    limits = [row for row in (host.get("Ulimits") or []) if row.get("Name") == "nproc"]
    need((pids is None or type(pids) is int and pids == 0) and len(limits) == 1
         and type(limits[0].get("Soft")) is int and limits[0]["Soft"] == 256
         and type(limits[0].get("Hard")) is int and limits[0]["Hard"] == 256,
         "audit_process_limit_unavailable")
    return "rlimit_nproc"


def kernel_process_limit(output):
    rows = [line for line in output.splitlines() if line.strip().startswith("Max processes")]
    need(len(rows) == 1 and re.fullmatch(r"\s*Max processes\s+256\s+256\s+processes\s*", rows[0]),
         "audit_kernel_process_limit_unconfirmed")
    return 256


def isolation(info, owner):
    host = info.get("HostConfig") or {}
    config = info.get("Config") or {}
    need((config.get("Labels") or {}).get(OWNER_LABEL) == owner, "audit_container_owner_mismatch")
    need(host.get("NetworkMode") == "none" and not host.get("PortBindings") and host.get("PublishAllPorts") is not True, "audit_network_not_isolated")
    need(host.get("Memory") == 1024 ** 3 and host.get("CpusetCpus") == "0,1", "audit_resource_limits_changed")
    process_limiter(host)
    need(host.get("ReadonlyRootfs") is True and host.get("Privileged") is False and config.get("User") == "1026:100", "audit_runtime_privileges_changed")
    need("ALL" in (host.get("CapDrop") or []) and "no-new-privileges:true" in (host.get("SecurityOpt") or []), "audit_capabilities_changed")
    need(not any(mount.get("Type") in ("bind", "volume") for mount in info.get("Mounts", [])), "audit_private_mount_present")
    devices = host.get("Devices") or []
    need(len(devices) == 1 and devices[0].get("PathOnHost") == "/dev/dri/renderD128" and devices[0].get("PathInContainer") == "/dev/dri/renderD128" and devices[0].get("CgroupPermissions") == "rwm" and "937" in (host.get("GroupAdd") or []), "audit_render_access_changed")
    return True


def passed_summary(kind, output, code):
    need(code == 0, "audit_test_failed_" + kind)
    patterns = {"selftests": r"(\d+) passed, (\d+) failed", "contracts": r"(\d+) contract tests passed; (\d+) failed"}
    if kind in patterns:
        matches = re.findall(patterns[kind], output)
        need(matches and int(matches[-1][0]) > 0 and int(matches[-1][1]) == 0, "audit_summary_missing_" + kind)
        return {"passed": True, "exitCode": code, "passedTests": int(matches[-1][0]), "failedTests": 0, "isolationVerified": True}
    matches = re.findall(r"(\d+) GPU low-quality regressions passed:", output)
    need(matches and int(matches[-1]) > 0 and "vaapi surfaces confirmed by FFmpeg get_format" in output, "audit_gpu_runtime_or_summary_missing")
    return {"passed": True, "exitCode": code, "gpuRegressionGroups": int(matches[-1]), "runtimeVaapiSurfacesObserved": True, "isolationVerified": True}


class AuditBuilder:
    def __init__(self, backend, runner=None):
        self.backend = backend
        self.runner = runner or subprocess.run
        self.owner = uuid.uuid4().hex
        self.containers = []
        self.images = []

    def call(self, args, timeout=60, environment=None):
        return self.runner(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, env=environment)

    def inspect_container(self, name):
        value = self.call([self.backend.docker, "inspect", name], 20)
        if value.returncode:
            return None
        rows = json.loads(value.stdout)
        need(len(rows) == 1, "single_audit_container_required")
        return rows[0]

    def remove_container(self, name):
        info = self.inspect_container(name)
        if info is not None:
            need((info["Config"].get("Labels") or {}).get(OWNER_LABEL) == self.owner, "cleanup_container_owner_mismatch")
            need(self.call([self.backend.docker, "rm", "--force", name], 30).returncode == 0, "owned_container_cleanup_failed")
        need(self.inspect_container(name) is None, "owned_container_cleanup_unconfirmed")

    def build_image(self, root, recipe, image, source, target):
        environment = dict(os.environ, DOCKER_BUILDKIT="0")
        arguments = [self.backend.docker, "build", "--force-rm", "--cpuset-cpus", "0,1", "--memory", "1g", "--target", target,
                     "--label", OWNER_LABEL + "=" + self.owner, "--build-arg", "BRIDGE_VERSION=" + source["version"],
                     "--build-arg", "BRIDGE_SOURCE_REVISION=sha256-" + source["sourceSha256"], "-f", str(root / recipe), "-t", image, str(root)]
        result = self.call(arguments, 1800, environment)
        self.backend.write_private(self.receipt, target + "-build.private.log", result.stdout)
        need(result.returncode == 0, "owned_image_build_failed")
        info = self.backend.image(image)
        # Register the exact ID before validating labels, so a failed provenance
        # check cannot falsely report cleanup while leaving its created image.
        self.images.append((image, info["Id"]))
        labels = info["Config"].get("Labels") or {}
        need(labels.get(OWNER_LABEL) == self.owner and labels.get("org.opencontainers.image.revision") == "sha256-" + source["sourceSha256"] and labels.get("org.opencontainers.image.version") == source["version"], "built_image_provenance_mismatch")
        return info["Id"]

    def test(self, kind, image, command):
        name = "reolink-deploy-test-" + self.owner[:16] + "-" + kind
        self.containers.append(name)
        started = self.call(isolated_command(self.backend.docker, self.owner, name, image, ["dotnet"] + command), 30)
        need(started.returncode == 0, "audit_container_start_failed")
        isolation(self.inspect_container(name) or {}, self.owner)
        finished = self.call([self.backend.docker, "wait", name], 300)
        need(finished.returncode == 0, "audit_wait_failed")
        log = self.call([self.backend.docker, "logs", name], 30)
        need(log.returncode == 0, "audit_log_read_failed")
        self.backend.write_private(self.receipt, kind + ".private.log", log.stdout)
        code = int(finished.stdout.decode().strip())
        result = passed_summary(kind, log.stdout.decode(errors="replace"), code)
        self.remove_container(name)
        return result

    def process_limits(self, image):
        name = "reolink-deploy-test-" + self.owner[:16] + "-limits"
        self.containers.append(name)
        started = self.call(isolated_command(self.backend.docker, self.owner, name, image,
                                            ["cat", "/proc/self/limits"]), 30)
        need(started.returncode == 0, "audit_limits_container_start_failed")
        info = self.inspect_container(name) or {}
        isolation(info, self.owner)
        limiter = process_limiter(info["HostConfig"])
        finished = self.call([self.backend.docker, "wait", name], 30)
        log = self.call([self.backend.docker, "logs", name], 30)
        need(finished.returncode == 0 and finished.stdout.strip() == b"0" and log.returncode == 0,
             "audit_kernel_process_limit_read_failed")
        self.backend.write_private(self.receipt, "limits.private.log", log.stdout)
        limit = kernel_process_limit(log.stdout.decode(errors="replace"))
        self.remove_container(name)
        return {"processLimiter": limiter, "actualKernelMaxProcesses": limit,
                "kernelProcessLimitVerified": True, "cgroupPidsLimit": limiter == "cgroup_pids",
                "additionalRlimitScope": "shared_runtime_uid"}

    def dll(self, image, path, label):
        name = "reolink-deploy-test-" + self.owner[:16] + "-" + label
        self.containers.append(name)
        # Hash containers have the same isolation as tests; inspect before wait.
        result = self.call(isolated_command(self.backend.docker, self.owner, name, image, ["sha256sum", path]), 30)
        need(result.returncode == 0, "application_hash_container_start_failed")
        isolation(self.inspect_container(name) or {}, self.owner)
        waited = self.call([self.backend.docker, "wait", name], 30)
        logs = self.call([self.backend.docker, "logs", name], 30)
        need(waited.returncode == 0 and waited.stdout.strip() == b"0" and logs.returncode == 0, "application_hash_read_failed")
        value = logs.stdout.decode().split()[0]
        need(re.fullmatch(r"[0-9a-f]{64}", value), "application_hash_invalid")
        self.remove_container(name)
        return value

    def cleanup(self, root, keep_image_id=None):
        good = True
        for name in self.containers:
            try:
                self.remove_container(name)
            except Exception:
                good = False
        for reference, image_id in self.images:
            if image_id == keep_image_id:
                continue
            try:
                info = self.backend.image(reference)
                need(info["Id"] == image_id and (info["Config"].get("Labels") or {}).get(OWNER_LABEL) == self.owner, "cleanup_image_owner_mismatch")
                need(self.call([self.backend.docker, "image", "rm", image_id], 60).returncode == 0, "owned_image_cleanup_failed")
            except Exception:
                good = False
        try:
            receipt_root = self.backend.receipt_path(self.receipt, "owner.json").parent.resolve()
            need(root.resolve().parent == receipt_root and root.name == "source" and not root.is_symlink(), "cleanup_source_scope_mismatch")
            owner = self.backend.read_json(self.receipt, "owner.json")
            need(owner["runId"] == self.owner, "cleanup_source_owner_mismatch")
            if root.exists():
                shutil.rmtree(str(root))
            need(not root.exists(), "owned_source_cleanup_failed")
        except Exception:
            good = False
        return good

    def run(self, payload):
        source, files = frozen_archive(payload)
        before = self.backend.inventory()
        need(all(entry["imageRef"] != source["imageTag"] for entry in before), "build_tag_is_running_reference")
        existing = self.call([self.backend.docker, "image", "inspect", source["imageTag"]], 20)
        need(existing.returncode != 0, "fresh_production_image_tag_required")
        self.receipt = self.backend.create_receipt("build")
        self.backend.write_private(self.receipt, "owner.json", encoded({"runId": self.owner}))
        self.backend.write_private(self.receipt, "before.private.json", encoded(before))
        root = self.backend.receipt_path(self.receipt, "source")
        root.mkdir(mode=0o700)
        production_id = None
        result = {"ok": False, "mode": "build", "receipt": self.receipt, "version": source["version"], "sourceSha256": source["sourceSha256"], "archiveSha256": source["archiveSha256"],
                  "sourceFiles": source["sourceFiles"], "imageTag": source["imageTag"], "testsPassed": False, "testedApplicationDllIdentical": False, "productionChanges": 0, "credentialsPrinted": False}
        try:
            for name, data in files:
                destination = root.joinpath(*PurePosixPath(name).parts)
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with destination.open("xb") as stream:
                    stream.write(data)
                os.chmod(str(destination), 0o644)
            recipe = audit_recipe((root / "Dockerfile").read_text(encoding="utf-8"), self.owner)
            with (root / "Dockerfile.audit").open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(recipe)
            audit_tag = "reolink-bridge:audit-" + self.owner
            audit_id = self.build_image(root, "Dockerfile.audit", audit_tag, source, "audit_tests")
            limits = self.process_limits(audit_id)
            result.update(limits)
            outcomes = {}
            for kind, command in (("selftests", ["/app/neolink.net.dll", "selftest"]), ("contracts", ["/tests/Neolink.ProtectTests.dll"]), ("vaapi", ["/tests/Neolink.ProtectTests.dll", "--hardware-low-quality-only"])):
                outcomes[kind] = self.test(kind, audit_id, command)
            production_id = self.build_image(root, "Dockerfile", source["imageTag"], source, "runtime")
            audit_dll = self.dll(audit_id, "/app/neolink.net.dll", "audit-app")
            contract_dll = self.dll(audit_id, "/tests/neolink.net.dll", "audit-dependency")
            production_dll = self.dll(production_id, "/app/neolink.net.dll", "production-app")
            need(audit_dll == contract_dll == production_dll, "tested_application_dll_mismatch")
            after = self.backend.inventory()
            need(projected(before) == projected(after), "production_inventory_changed_during_audit")
            result.update(ok=True, imageId=production_id, testsPassed=True, testedApplicationDllIdentical=True, applicationDllSha256=production_dll,
                          results=outcomes, productionInventoryPreserved=True, syntheticOnly=True,
                          isolation={"network":"none", "publishedPorts":False, "cpus":"0,1", "memoryMiB":1024, "runtimeUser":"1026:100", "renderDevice":"/dev/dri/renderD128", "renderGroup":"937"})
            result["isolation"].update(limits)
            if "sourceRevision" in source:
                result["sourceRevision"] = source["sourceRevision"]
        except Exception as error:
            result.update(ok=False, testsPassed=False, testedApplicationDllIdentical=False, errorCode=str(error) if isinstance(error, GuardError) else "owned_build_audit_failed", errorType=type(error).__name__)
        finally:
            cleanup = self.cleanup(root, production_id if result["ok"] else None)
            result["cleanupConfirmed"] = cleanup
            if not cleanup:
                result.update(ok=False, testsPassed=False, testedApplicationDllIdentical=False, errorCode="owned_build_cleanup_unconfirmed")
            if result["ok"]:
                result["release"] = {key: result[key] for key in ("version", "sourceSha256", "imageId", "imageTag", "archiveSha256", "applicationDllSha256", "testsPassed", "testedApplicationDllIdentical")}
                if "sourceRevision" in result:
                    result["release"]["sourceRevision"] = result["sourceRevision"]
            self.backend.write_private(self.receipt, "build-result.json", encoded(result))
        return result
