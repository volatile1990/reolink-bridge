"""Frozen-source/build isolation contracts; no NAS or Docker daemon contacted."""
import base64
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]
for name in ("engine", "build"):
    key = "reolink_deploy_" + name
    if key not in sys.modules:
        spec = importlib.util.spec_from_file_location(key, ROOT / ("tools/deploy/" + name + ".py"))
        module = importlib.util.module_from_spec(spec); sys.modules[key] = module; spec.loader.exec_module(module)
engine, build = sys.modules["reolink_deploy_engine"], sys.modules["reolink_deploy_build"]
RECIPE = "FROM sdk AS build\nCOPY src/ ./src/\nARG BRIDGE_VERSION\nARG BRIDGE_SOURCE_REVISION\nRUN dotnet publish\n# The upstream project references Microsoft.AspNetCore.App, even in headless mode.\nFROM runtime AS runtime\n"


def package(extra=None):
    files = {"Dockerfile": RECIPE.encode(), "tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj": b"<Project/>\n", "examples/config.pilot.json": b"{}\n"}
    files.update(extra or {})
    raw = io.BytesIO(); digest = hashlib.sha256()
    with zipfile.ZipFile(raw, "w") as archive:
        for name, value in sorted(files.items()):
            archive.writestr(name, value)
            digest.update(name.encode() + b"\0"); digest.update(hashlib.sha256(value).digest())
    data = raw.getvalue()
    return {"mode": "build", "archiveBase64": base64.b64encode(data).decode(), "archiveSha256": engine.digest(data),
            "sourceManifest": {"version": "1.4.0", "sourceRevision": "c" * 40, "sourceSha256": digest.hexdigest(), "sourceFiles": len(files), "sourceDirty": False, "canonicalLineEndings": True}}


def isolated_info(owner):
    return {"Config": {"User": "1026:100", "Labels": {build.OWNER_LABEL: owner}}, "Mounts": [],
            "HostConfig": {"NetworkMode": "none", "PortBindings": None, "PublishAllPorts": False, "Memory": 1024 ** 3, "CpusetCpus": "0,1", "PidsLimit": 256,
                           "Ulimits": [{"Name": "nproc", "Soft": 256, "Hard": 256}],
                           "ReadonlyRootfs": True, "Privileged": False, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges:true"], "GroupAdd": ["937"],
                           "Devices": [{"PathOnHost": "/dev/dri/renderD128", "PathInContainer": "/dev/dri/renderD128", "CgroupPermissions": "rwm"}]}}


class BuildBackend:
    docker = "synthetic-docker"

    def __init__(self, root):
        self.root = Path(root); self.images = {}; self.containers = {}; self.calls = []
        self.fail_contract = False; self.dll_mismatch = False
        self.pids_limit = 256
        self.limits_output = b"Limit                     Soft Limit           Hard Limit           Units\nMax processes             256                  256                  processes\n"
        self.entries = [{"ip": "192.168.30.%d" % i, "id": "a" * 64, "name": "synthetic", "imageId": "sha256:" + "a" * 64, "imageRef": "reolink-bridge:existing",
                         "configSha256": "d" * 64, "composeSha256": {"/private/compose": "e" * 64}, "mac": "02:00:00:00:00:01", "network": "f" * 64, "project": "synthetic", "service": "camera"} for i in range(210,216)]

    def inventory(self):
        return copy.deepcopy(self.entries)

    def create_receipt(self, kind):
        path = self.root / ("receipt-%d" % len(list(self.root.iterdir()))); path.mkdir()
        return str(path)

    def receipt_path(self, receipt, name):
        return Path(receipt) / name

    def write_private(self, receipt, name, value):
        with self.receipt_path(receipt, name).open("xb") as stream:
            stream.write(value)

    def read_json(self, receipt, name):
        return json.loads(self.receipt_path(receipt, name).read_bytes())

    def image(self, reference):
        if reference in self.images:
            return copy.deepcopy(self.images[reference])
        return copy.deepcopy(next(value for value in self.images.values() if value["Id"] == reference))

    def run(self, args, **kwargs):
        self.calls.append(args)
        def result(code=0, value=b""):
            return SimpleNamespace(returncode=code, stdout=value)
        if args[1:3] == ["image", "inspect"]:
            return result(0, json.dumps([self.images[args[3]]]).encode()) if args[3] in self.images else result(1)
        if args[1] == "build":
            tag = args[args.index("-t")+1]; owner = args[args.index("--label")+1].split("=",1)[1]
            arguments = [args[index+1] for index,value in enumerate(args) if value == "--build-arg"]
            values = dict(value.split("=",1) for value in arguments)
            self.images[tag] = {"Id": "sha256:" + hashlib.sha256(tag.encode()).hexdigest(), "Config": {"Labels": {build.OWNER_LABEL: owner, "org.opencontainers.image.version": values["BRIDGE_VERSION"], "org.opencontainers.image.revision": values["BRIDGE_SOURCE_REVISION"]}}}
            return result(value=b"synthetic build\n")
        if args[1] == "run":
            name = args[args.index("--name")+1]; owner = args[args.index("--label")+1].split("=",1)[1]
            self.containers[name] = isolated_info(owner)
            self.containers[name]["HostConfig"]["PidsLimit"] = self.pids_limit
            return result(value=name.encode())
        if args[1] == "inspect":
            return result(value=json.dumps([self.containers[args[2]]]).encode()) if args[2] in self.containers else result(1)
        if args[1] == "wait":
            return result(value=b"1" if self.fail_contract and args[2].endswith("-contracts") else b"0")
        if args[1] == "logs":
            name = args[2]
            if name.endswith("-limits"):return result(value=self.limits_output)
            if name.endswith("-selftests"):return result(value=b"159 passed, 0 failed\n")
            if name.endswith("-contracts"):return result(value=b"136 contract tests passed; 0 failed\n")
            if name.endswith("-vaapi"):return result(value=b"PASS vaapi surfaces confirmed by FFmpeg get_format\n4 GPU low-quality regressions passed: synthetic\n")
            return result(value=(("b" if self.dll_mismatch and name.endswith("-production-app") else "a")*64+" /app/neolink.net.dll\n").encode())
        if args[1] == "rm":
            self.containers.pop(args[-1],None); return result()
        if args[1:3] == ["image","rm"]:
            for reference in list(self.images):
                if self.images[reference]["Id"] == args[-1]:self.images.pop(reference)
            return result()
        raise AssertionError("unexpected_synthetic_command")


class BuildTests(unittest.TestCase):
    def test_frozen_source_verified_exactly(self):
        source, files = build.frozen_archive(package())
        self.assertEqual(len(files),3); self.assertEqual(source["imageTag"], "reolink-bridge:managed-"+source["sourceSha256"][:12])

    def test_dirty_manifest_rejected(self):
        payload = package(); payload["sourceManifest"]["sourceDirty"] = True
        with self.assertRaisesRegex(engine.GuardError,"clean_canonical_source_manifest_required"):build.frozen_archive(payload)

    def test_noncanonical_manifest_rejected(self):
        payload = package(); payload["sourceManifest"]["canonicalLineEndings"] = False
        with self.assertRaises(engine.GuardError):build.frozen_archive(payload)

    def test_archive_hash_mismatch_rejected(self):
        payload=package();payload["archiveSha256"]="f"*64
        with self.assertRaisesRegex(engine.GuardError,"source_archive_hash_mismatch"):build.frozen_archive(payload)

    def test_raw_source_hash_mismatch_rejected(self):
        payload=package();payload["sourceManifest"]["sourceSha256"]="f"*64
        with self.assertRaisesRegex(engine.GuardError,"exact_source_digest_mismatch"):build.frozen_archive(payload)

    def test_private_file_rejected(self):
        with self.assertRaisesRegex(engine.GuardError,"private_source_archive_member"):build.frozen_archive(package({"secrets/private.json":b"synthetic"}))

    def test_traversal_rejected(self):
        with self.assertRaisesRegex(engine.GuardError,"unsafe_source_archive_member"):build.frozen_archive(package({"../outside":b"synthetic"}))

    def test_audit_recipe_preserves_original_publish_options(self):
        value=build.audit_recipe(RECIPE,"a"*32)
        self.assertIn("RUN dotnet publish",value);self.assertIn("-p:Version=${BRIDGE_VERSION}",value);self.assertIn("/tests/",value)
        self.assertEqual(RECIPE.count("AS audit_tests"),0)

    def test_changed_dockerfile_anchor_rejected(self):
        with self.assertRaisesRegex(engine.GuardError,"dockerfile_audit_anchor_changed"):build.audit_recipe("unrecognized", "a"*32)

    def test_runtime_isolation(self):
        self.assertTrue(build.isolation(isolated_info("a"*32),"a"*32))

    def test_nproc_limit_is_requested_for_every_isolated_container(self):
        args=build.isolated_command("docker","a"*32,"owned","synthetic",["cat","/proc/self/limits"])
        self.assertEqual(args[args.index("--ulimit")+1],"nproc=256:256")
        self.assertEqual(args[args.index("--pids-limit")+1],"256")

    def test_fallback_requires_exact_nproc_limit_when_pids_controller_absent(self):
        for missing in (None,0):
            value=isolated_info("a"*32);value["HostConfig"]["PidsLimit"]=missing
            self.assertTrue(build.isolation(value,"a"*32))
            self.assertEqual(build.process_limiter(value["HostConfig"]),"rlimit_nproc")

    def test_fallback_missing_wrong_or_duplicate_nproc_rejected(self):
        invalid=[None,[],[{"Name":"nproc","Soft":256,"Hard":257}],
                 [{"Name":"nproc","Soft":"256","Hard":256}],
                 [{"Name":"nproc","Soft":256,"Hard":256}]*2]
        for limits in invalid:
            with self.subTest(limits=limits):
                value=isolated_info("a"*32);value["HostConfig"].update(PidsLimit=None,Ulimits=limits)
                with self.assertRaisesRegex(engine.GuardError,"audit_process_limit_unavailable"):
                    build.isolation(value,"a"*32)

    def test_unexpected_cgroup_limit_never_uses_fallback(self):
        for limit in (128,257,-1,"256",False):
            value=isolated_info("a"*32);value["HostConfig"]["PidsLimit"]=limit
            with self.assertRaisesRegex(engine.GuardError,"audit_process_limit_unavailable"):
                build.isolation(value,"a"*32)

    def test_kernel_limits_require_exact_unique_process_limits_line(self):
        self.assertEqual(build.kernel_process_limit("Max processes  256  256  processes\n"),256)
        for output in ("", "claimed Max processes 256 256 processes", "Max processes 256 unlimited processes",
                       "Max processes 257 256 processes", "Max processes 256 256 processes trailing",
                       "Max processes 256 256 processes\nMax processes 256 256 processes"):
            with self.subTest(output=output):
                with self.assertRaisesRegex(engine.GuardError,"audit_kernel_process_limit_unconfirmed"):
                    build.kernel_process_limit(output)

    def test_bound_volume_rejected(self):
        value=isolated_info("a"*32);value["Mounts"]=[{"Type":"bind","Source":"/private"}]
        with self.assertRaisesRegex(engine.GuardError,"audit_private_mount_present"):build.isolation(value,"a"*32)

    def test_network_access_rejected(self):
        value=isolated_info("a"*32);value["HostConfig"]["NetworkMode"]="bridge"
        with self.assertRaisesRegex(engine.GuardError,"audit_network_not_isolated"):build.isolation(value,"a"*32)

    def test_gpu_runtime_evidence_required(self):
        with self.assertRaisesRegex(engine.GuardError,"audit_gpu_runtime_or_summary_missing"):build.passed_summary("vaapi","4 GPU low-quality regressions passed:",0)

    def test_test_exit_failure_rejected(self):
        with self.assertRaisesRegex(engine.GuardError,"audit_test_failed_contracts"):build.passed_summary("contracts","136 contract tests passed; 0 failed",1)

    def test_build_and_audit_return_deployable_identical_binary(self):
        with tempfile.TemporaryDirectory() as root:
            backend=BuildBackend(root);before=backend.inventory()
            result=build.AuditBuilder(backend,backend.run).run(package())
            self.assertTrue(result["ok"]);self.assertTrue(result["cleanupConfirmed"])
            self.assertTrue(result["release"]["testsPassed"]);self.assertTrue(result["release"]["testedApplicationDllIdentical"])
            self.assertEqual(result["processLimiter"],"cgroup_pids")
            self.assertEqual(result["actualKernelMaxProcesses"],256)
            self.assertTrue(result["kernelProcessLimitVerified"])
            self.assertEqual(engine.release_descriptor(result["release"])["version"],"1.4.0")
            self.assertEqual(before,backend.inventory());self.assertFalse(backend.containers)
            self.assertEqual(len(backend.images),1);self.assertFalse((Path(result["receipt"])/"source").exists())
            builds=[args for args in backend.calls if args[1]=="build"]
            self.assertEqual(len(builds),2)
            self.assertTrue(all("--memory"in args and args[args.index("--memory")+1]=="1g" for args in builds))
            runs=[args for args in backend.calls if args[1]=="run"]
            self.assertTrue(all(args[args.index("--ulimit")+1]=="nproc=256:256" for args in runs))
            limits=[args for args in runs if args[args.index("--name")+1].endswith("-limits")]
            self.assertEqual(len(limits),1);self.assertEqual(limits[0][-1],"/proc/self/limits")
            self.assertEqual(limits[0][limits[0].index("--entrypoint")+1],"cat")

    def test_dsm_fallback_build_proves_actual_kernel_nproc_and_reports_scope(self):
        with tempfile.TemporaryDirectory() as root:
            backend=BuildBackend(root);backend.pids_limit=None
            result=build.AuditBuilder(backend,backend.run).run(package())
            self.assertTrue(result["ok"]);self.assertEqual(result["processLimiter"],"rlimit_nproc")
            self.assertFalse(result["cgroupPidsLimit"]);self.assertEqual(result["actualKernelMaxProcesses"],256)
            self.assertEqual(result["additionalRlimitScope"],"shared_runtime_uid")
            self.assertTrue(result["isolation"]["kernelProcessLimitVerified"])
            self.assertFalse(backend.containers)

    def test_fabricated_kernel_proof_blocks_tests_and_deployable_release(self):
        with tempfile.TemporaryDirectory() as root:
            backend=BuildBackend(root);backend.pids_limit=None;backend.limits_output=b"configured nproc=256:256\n"
            result=build.AuditBuilder(backend,backend.run).run(package())
            self.assertFalse(result["ok"]);self.assertNotIn("release",result)
            self.assertEqual(result["errorCode"],"audit_kernel_process_limit_unconfirmed")
            self.assertTrue(result["cleanupConfirmed"]);self.assertFalse(backend.images);self.assertFalse(backend.containers)
            tests=[args for args in backend.calls if args[1]=="run" and args[args.index("--name")+1].endswith("-selftests")]
            self.assertFalse(tests)

    def test_failed_test_never_produces_a_deployable_release(self):
        with tempfile.TemporaryDirectory() as root:
            backend=BuildBackend(root);backend.fail_contract=True
            result=build.AuditBuilder(backend,backend.run).run(package())
            self.assertFalse(result["ok"]);self.assertFalse(result["testsPassed"]);self.assertNotIn("release",result)
            self.assertTrue(result["cleanupConfirmed"]);self.assertFalse(backend.images);self.assertFalse(backend.containers)

    def test_changed_tested_dll_rejected_and_owned_images_removed(self):
        with tempfile.TemporaryDirectory() as root:
            backend=BuildBackend(root);backend.dll_mismatch=True
            result=build.AuditBuilder(backend,backend.run).run(package())
            self.assertFalse(result["ok"]);self.assertEqual(result["errorCode"],"tested_application_dll_mismatch")
            self.assertFalse(backend.images);self.assertFalse(backend.containers)


if __name__=="__main__":unittest.main()
