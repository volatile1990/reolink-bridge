"""Offline deployment contracts. No NAS, network, Docker daemon or credentials."""
import contextlib
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("reolink_deploy_engine", ROOT / "tools/deploy/engine.py")
if SPEC.name not in sys.modules:
    engine = importlib.util.module_from_spec(SPEC)
    sys.modules[SPEC.name] = engine
    SPEC.loader.exec_module(engine)
else:
    engine = sys.modules[SPEC.name]
SECRET = "SYNTHETIC_DEPLOY_PRIVATE_SECRET"
IPS = tuple("192.168.30.%d" % value for value in range(210, 216))
OLD_ID = "sha256:" + "a" * 64
NEW_ID = "sha256:" + "b" * 64
OLD_TAG = "reolink-bridge:old-frozen"
NEW_TAG = "reolink-bridge:new-frozen"
RELEASE = {"version": "1.4.0", "sourceSha256": "b" * 64, "imageId": NEW_ID, "imageTag": NEW_TAG,
           "testsPassed": True, "testedApplicationDllIdentical": True, "sourceRevision": "c" * 40}


class MemoryNas:
    ips = IPS

    def __init__(self):
        self.clock = 0
        self.private = {}
        self.receipt_counter = 0
        self.container_counter = 0
        self.owned = False
        self.recreated = []
        self.fail_new = None
        self.concurrent_config = None
        self.stop_rollback_ip = None
        self.base = "/volume1/docker/reolink-bridge-pilot/pilot-compose.yaml"
        labels_old = {"org.opencontainers.image.version": "1.3.0", "org.opencontainers.image.revision": "sha256-" + "a" * 64}
        labels_new = {"org.opencontainers.image.version": "1.4.0", "org.opencontainers.image.revision": "sha256-" + "b" * 64}
        common = {"User": "1654", "Env": ["DOTNET_RUNNING_IN_CONTAINER=true"], "Entrypoint": ["dotnet", "neolink.net.dll"], "Cmd": ["rtsp", "--config", "/run/secrets/bridge-config.json"],
                  "WorkingDir": "/app", "Healthcheck": {"Test": ["CMD", "curl", "http://127.0.0.1:8080/health"]}}
        self.images = {OLD_ID: {"Id": OLD_ID, "Config": dict(copy.deepcopy(common), Labels=labels_old)}, NEW_ID: {"Id": NEW_ID, "Config": dict(copy.deepcopy(common), Labels=labels_new)}}
        self.tags = {OLD_TAG: OLD_ID, NEW_TAG: NEW_ID}
        self.entries = []
        self.configs = {}
        compose = {"services": {}}
        for index, ip in enumerate(IPS):
            ident = hashlib.sha256(ip.encode()).hexdigest()
            profiles = [{"stream": "mainStream", "codec": "H265", "width": 4512, "height": 2512, "fps": 20, "bitrate": 8192}]
            if index < 5:
                profiles.append({"stream": "subStream", "codec": "H265", "width": 896, "height": 512, "fps": 20, "bitrate": 1536})
            config = {"onvif": {"advertised_host": ip, "mac": "02:42:00:00:00:%02x" % index, "profiles": profiles, "event_replay_path": "/private/replay.json"}, "users": [{"name": "synthetic", "pass": SECRET}], "cameras": [{"address": ip, "password": SECRET}]}
            if index < 4:
                config["onvif"].update(low_quality_encoder="vaapi", low_quality_decoder="vaapi", low_quality_transcode="H265")
            path = "/volume1/docker/reolink-bridge-pilot/config-%d.json" % index
            self.configs[path] = engine.encoded(config)
            service = "camera-%d" % index
            compose["services"][service] = {"image": OLD_TAG, "read_only": True, "restart": "always", "labels": {}}
            entry = {"ip": ip, "id": ident, "name": "synthetic-" + service, "imageId": OLD_ID, "imageRef": OLD_TAG, "directory": "/volume1/docker/reolink-bridge-pilot", "compose": [self.base], "config": path,
                     "project": "reolink-bridge-pilot", "service": service, "mac": config["onvif"]["mac"], "network": "d" * 64, "configuration": config,
                     "inspect": {"Config": {"Image": OLD_TAG, "User": "1026:100", "Hostname": ident[:12], "Env": ["SYNTHETIC_ENV=" + SECRET], "Labels": dict(labels_old)},
                                 "HostConfig": {"Mounts": [{"Type": "bind", "Source": path, "Target": "/run/secrets/bridge-config.json", "ReadOnly": True}, {"Type": "bind", "Source": "/private/replay", "Target": "/var/lib/reolink-bridge-events", "ReadOnly": False, "BindOptions": {"Propagation": "rprivate"}}],
                                                "Binds": None, "Dns": None, "ReadonlyRootfs": True, "Privileged": False, "RestartPolicy": {"Name": "always"}, "GroupAdd": ["937"], "Devices": [{"PathOnHost": "/dev/dri/renderD128", "PathInContainer": "/dev/dri/renderD128", "CgroupPermissions": "rwm"}]},
                                 "State": {"StartedAt": "original", "RestartCount": 0}}}
            self.entries.append(entry)
        self.compose_bytes = engine.encoded(compose)

    def inventory(self):
        rows = copy.deepcopy(self.entries)
        for entry in rows:
            entry["configuration"] = json.loads(self.configs[entry["config"]])
            entry["configSha256"] = engine.digest(self.configs[entry["config"]])
            entry["composeSha256"] = {self.base: engine.digest(self.compose_bytes)}
            entry["resolved"] = json.loads(self.compose_bytes)
        return rows

    def image(self, reference):
        return copy.deepcopy(self.images[self.tags.get(reference, reference)])

    def read_compose(self):
        return self.compose_bytes

    def read_config(self, entry):
        return self.configs[entry["config"]]

    def write_compose(self, value, expected):
        engine.need(self.owned and self.compose_bytes == expected, "atomic_file_cas_failed")
        self.compose_bytes = value

    def resolved(self, entry):
        return json.loads(self.compose_bytes)

    def sample(self, entry):
        engine.need(not (entry["ip"] == self.fail_new and entry["imageId"] == NEW_ID), "encoder_stopped")
        profiles = entry["configuration"]["onvif"]["profiles"]
        return {"at": self.clock, "streams": [{"profile": "main" if i == 0 else "sub", "totalAccessUnits": int(self.clock * profile["fps"]) + 100} for i, profile in enumerate(profiles)]}

    def monotonic(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds

    @contextlib.contextmanager
    def maintenance(self):
        engine.need(not self.owned, "maintenance_already_owned")
        self.owned = True
        try:
            yield
        finally:
            self.owned = False

    def recreate(self, selected):
        assert self.owned
        entry = engine.target(self.entries, selected["ip"])
        new_tag = json.loads(self.compose_bytes)["services"][entry["service"]]["image"]
        if self.stop_rollback_ip == entry["ip"] and new_tag == OLD_TAG:
            self.stop_rollback_ip = None
            raise engine.GuardError("synthetic_recreate_interruption")
        new_id = self.tags[new_tag]
        self.container_counter += 1
        ident = hashlib.sha256((entry["ip"] + str(self.container_counter)).encode()).hexdigest()
        entry.update(id=ident, imageId=new_id, imageRef=new_tag)
        entry["inspect"]["Config"].update(Image=new_tag, Hostname=ident[:12], Labels=self.image(new_id)["Config"]["Labels"])
        entry["inspect"]["State"]["StartedAt"] = str(self.container_counter)
        entry["inspect"]["HostConfig"]["Mounts"].reverse()
        self.recreated.append(entry["ip"])
        if self.concurrent_config == entry["ip"] and new_id == NEW_ID:
            current = json.loads(self.configs[entry["config"]]); current["operatorSetting"] = "concurrent-owner"
            self.configs[entry["config"]] = engine.encoded(current)

    def create_receipt(self, purpose):
        self.receipt_counter += 1
        name = "/private/receipt-%d" % self.receipt_counter
        self.private[name] = {}
        return name

    def write_private(self, receipt, filename, value):
        if filename in self.private[receipt]:
            raise FileExistsError(filename)
        self.private[receipt][filename] = value

    def read_private(self, receipt, filename):
        return self.private[receipt][filename]

    def read_json(self, receipt, filename):
        return json.loads(self.read_private(receipt, filename))

    def replace_private(self, receipt, filename, value, expected):
        engine.need(self.private[receipt][filename] == expected, "private_checkpoint_cas_failed")
        self.private[receipt][filename] = value


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.nas = MemoryNas()
        self.deploy = engine.DeployEngine(self.nas)

    def planned(self):
        return self.deploy.plan(RELEASE)

    def apply_next(self, baseline, ip):
        return self.deploy.apply({"release": RELEASE, "baseline": baseline, "ip": ip, "expected": engine.projected(self.nas.inventory())})

    def complete(self):
        plan = self.planned()
        for ip in IPS:
            self.assertTrue(self.apply_next(plan["baseline"], ip)["ok"])
        return plan["baseline"]

    def test_release_valid(self):
        self.assertEqual(self.deploy.release(RELEASE)["version"], "1.4.0")

    def test_release_bad_source(self):
        with self.assertRaises(engine.GuardError):
            self.deploy.release(dict(RELEASE, sourceSha256="bad"))

    def test_release_bad_tag(self):
        with self.assertRaises(engine.GuardError):
            self.deploy.release(dict(RELEASE, imageTag="$(untrusted)"))

    def test_release_requires_tests(self):
        with self.assertRaisesRegex(engine.GuardError, "tested_release_required"):
            self.deploy.release(dict(RELEASE, testsPassed=False))

    def test_release_requires_same_binary(self):
        with self.assertRaisesRegex(engine.GuardError, "tested_release_required"):
            self.deploy.release(dict(RELEASE, testedApplicationDllIdentical=False))

    def test_release_tag_wrong_id(self):
        self.nas.tags[NEW_TAG] = OLD_ID
        with self.assertRaisesRegex(engine.GuardError, "release_tag_image_mismatch"):
            self.deploy.release(RELEASE)

    def test_plan_rejects_retagged_running_release_reference(self):
        self.nas.tags[OLD_TAG] = NEW_ID
        with self.assertRaisesRegex(engine.GuardError, "release_tag_reuses_running_image_reference"):
            self.deploy.plan(dict(RELEASE, imageTag=OLD_TAG))

    def test_plan_requires_original_rollback_reference(self):
        self.nas.tags[OLD_TAG] = NEW_ID
        with self.assertRaisesRegex(engine.GuardError, "original_image_tag_drift"):
            self.planned()

    def test_release_source_wrong_label(self):
        self.nas.images[NEW_ID]["Config"]["Labels"]["org.opencontainers.image.revision"] = "wrong"
        with self.assertRaisesRegex(engine.GuardError, "release_source_label_mismatch"):
            self.deploy.release(RELEASE)

    def test_release_version_wrong_label(self):
        self.nas.images[NEW_ID]["Config"]["Labels"]["org.opencontainers.image.version"] = "9.0.0"
        with self.assertRaisesRegex(engine.GuardError, "release_version_label_mismatch"):
            self.deploy.release(RELEASE)

    def test_inventory_incomplete(self):
        self.nas.entries.pop()
        with self.assertRaises(engine.GuardError):
            self.deploy.inventory()

    def test_plan_defaults_drift(self):
        self.nas.images[NEW_ID]["Config"]["User"] = "root"
        with self.assertRaisesRegex(engine.GuardError, "image_default_drift_User"):
            self.planned()

    def test_plan_does_not_change_runtime_or_configuration(self):
        before, configs = self.nas.inventory(), dict(self.nas.configs)
        plan = self.planned()
        self.assertEqual(before, self.nas.inventory()); self.assertEqual(configs, self.nas.configs)
        self.assertEqual(plan["pendingIps"], list(IPS)); self.assertFalse(self.nas.recreated)

    def test_mount_order_permutation(self):
        before, after = self.nas.inventory(), self.nas.inventory()
        after[0]["inspect"]["HostConfig"]["Mounts"].reverse()
        engine.preserve(before, after, IPS[0], self.nas.read_compose(), OLD_TAG, OLD_ID, self.nas.image)

    def test_mount_inputs_unchanged(self):
        before, after = self.nas.inventory(), self.nas.inventory()
        after[0]["inspect"]["HostConfig"]["Mounts"].reverse()
        original = copy.deepcopy((before, after))
        engine.preserve(before, after, IPS[0], self.nas.read_compose(), OLD_TAG, OLD_ID, self.nas.image)
        self.assertEqual((before, after), original)

    def test_private_values_never_in_public_results(self):
        plan = self.planned(); applied = self.apply_next(plan["baseline"], IPS[0])
        resumed = self.deploy.resume({"baseline": plan["baseline"], "release": RELEASE})
        self.assertNotIn(SECRET, json.dumps([plan, applied, resumed]))

    def test_ordered_skip_rejected(self):
        plan = self.planned()
        with self.assertRaisesRegex(engine.GuardError, "ordered_next_target_required"):
            self.apply_next(plan["baseline"], IPS[1])

    def test_successful_canary_resume(self):
        plan = self.planned(); self.assertTrue(self.apply_next(plan["baseline"], IPS[0])["ok"])
        resumed = self.deploy.resume({"baseline": plan["baseline"], "release": RELEASE})
        self.assertEqual(resumed["pendingIps"], list(IPS[1:])); self.assertEqual(len(resumed["receipts"]), 1)

    def test_failed_canary_restores_and_resumes(self):
        plan = self.planned(); self.nas.fail_new = IPS[0]
        failed = self.apply_next(plan["baseline"], IPS[0])
        self.assertFalse(failed["ok"]); self.assertTrue(failed["rollbackRestored"])
        resumed = self.deploy.resume({"baseline": plan["baseline"], "release": RELEASE})
        self.assertEqual(resumed["pendingIps"], list(IPS)); self.assertEqual(len(resumed["attempts"]), 1)
        self.nas.fail_new = None
        self.assertTrue(self.apply_next(plan["baseline"], IPS[0])["ok"])

    def test_full_six_final(self):
        baseline = self.complete()
        result = self.deploy.final({"baseline": baseline, "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["exactConfigurationsPreserved"]); self.assertEqual(result["finiteRollbackSteps"], 6)

    def test_final_requires_complete_receipts(self):
        plan = self.planned()
        with self.assertRaisesRegex(engine.GuardError, "complete_ordered_receipts_required"):
            self.deploy.final({"baseline": plan["baseline"], "release": RELEASE, "expected": engine.projected(self.nas.inventory())})

    def test_final_rejects_broken_compose_chain(self):
        baseline = self.complete(); state = self.nas.read_json(baseline, "state.private.json")
        receipt = state["receipts"][2]
        value = json.loads(self.nas.private[receipt]["compose-before.private.json"])
        value["extra"] = "changed"
        self.nas.private[receipt]["compose-before.private.json"] = engine.encoded(value)
        with self.assertRaises(engine.GuardError):
            self.deploy.final({"baseline": baseline, "release": RELEASE, "expected": engine.projected(self.nas.inventory())})

    def test_full_rollback(self):
        baseline = self.complete()
        result = self.deploy.rollback_all({"baseline": baseline, "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["ok"]); self.assertEqual(len(result["steps"]), 6)
        self.assertEqual(self.nas.recreated[-6:], list(reversed(IPS)))
        self.assertTrue(all(entry["imageId"] == OLD_ID for entry in self.nas.inventory()))

    def test_rollback_stale_inventory_rejected(self):
        baseline = self.complete(); expected = engine.projected(self.nas.inventory()); expected[0]["id"] = "stale"
        with self.assertRaisesRegex(engine.GuardError, "fresh_inventory_required"):
            self.deploy.rollback_all({"baseline": baseline, "release": RELEASE, "expected": expected})

    def test_rollback_resume_after_verified_checkpoint(self):
        baseline = self.complete(); self.nas.stop_rollback_ip = IPS[-2]
        with self.assertRaises(engine.GuardError):
            self.deploy.rollback_all({"baseline": baseline, "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        state = self.nas.read_json(baseline, "state.private.json")
        self.assertEqual(len(state["rollbackSteps"]), 1)
        # An interrupted mutation is deliberately blocked until its exact previous
        # composition is restored; the confirmed first step remains recoverable.
        self.nas.compose_bytes = self.nas.read_private(state["rollbackReceipts"][-2], "compose-after.private.json")
        result = self.deploy.rollback_all({"baseline": baseline, "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["ok"]); self.assertEqual(len(result["steps"]), 6)

    def test_partial_prefix_rolls_back_without_touching_unapplied_services(self):
        plan = self.planned()
        for ip in IPS[:3]:
            self.assertTrue(self.apply_next(plan["baseline"], ip)["ok"])
        untouched = {entry["ip"]: entry["id"] for entry in self.nas.inventory() if entry["ip"] in IPS[3:]}
        result = self.deploy.rollback_all({"baseline": plan["baseline"], "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["allPreviousImagesRestored"]); self.assertEqual(len(result["steps"]), 3)
        resumed = self.deploy.resume({"baseline": plan["baseline"], "release": RELEASE})
        self.assertEqual(resumed["receipts"], []); self.assertEqual(resumed["phase"], "rolled-back"); self.assertTrue(resumed["allPreviousImagesRestored"])
        for entry in self.nas.inventory():
            if entry["ip"] in untouched:
                self.assertEqual(entry["id"], untouched[entry["ip"]])

    def test_empty_prefix_rollback_is_readonly_for_running_services(self):
        plan = self.planned()
        result = self.deploy.rollback_all({"baseline": plan["baseline"], "release": RELEASE, "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["allPreviousImagesRestored"]); self.assertEqual(result["steps"], []); self.assertFalse(self.nas.recreated)

    def test_concurrent_configuration_is_not_overwritten(self):
        plan = self.planned(); self.nas.concurrent_config = IPS[0]
        result = self.apply_next(plan["baseline"], IPS[0])
        self.assertFalse(result["ok"]); self.assertFalse(result["rollbackRestored"])
        self.assertIn(b"concurrent-owner", self.nas.configs[engine.target(self.nas.entries, IPS[0])["config"]])
        self.assertNotIn(SECRET, json.dumps(result))

    def test_external_media_gate_rollback_allows_canary_retry(self):
        plan = self.planned(); accepted = self.apply_next(plan["baseline"], IPS[0])
        result = self.deploy.rollback({"baseline": plan["baseline"], "release": RELEASE, "receipt": accepted["receipt"], "expected": engine.projected(self.nas.inventory())})
        self.assertTrue(result["rollbackRestored"])
        resumed = self.deploy.resume({"baseline": plan["baseline"], "release": RELEASE})
        self.assertEqual(resumed["pendingIps"], list(IPS)); self.assertEqual(resumed["receipts"], [])
        self.assertTrue(self.nas.read_json(accepted["receipt"], "result.json")["ok"])
        self.assertTrue(self.nas.read_json(accepted["receipt"], "rollback-result.json")["ok"])
        self.assertTrue(self.apply_next(plan["baseline"], IPS[0])["ok"])

    def test_external_media_gate_cannot_rollback_an_unrelated_step(self):
        baseline = self.complete()
        first = self.nas.read_json(baseline, "state.private.json")["receipts"][0]
        with self.assertRaisesRegex(engine.GuardError, "last_successful_receipt_required"):
            self.deploy.rollback({"baseline": baseline, "release": RELEASE, "receipt": first, "expected": engine.projected(self.nas.inventory())})


def drift_case(name, mutate):
    def test(self):
        before, after = self.nas.inventory(), self.nas.inventory()
        mutate(after[0], after[1])
        with self.assertRaises(engine.GuardError):
            engine.preserve(before, after, IPS[0], self.nas.read_compose(), OLD_TAG, OLD_ID, self.nas.image)
    test.__name__ = "test_drift_" + name
    return test


DRIFTS = {
    "selected_configuration": lambda selected, other: selected["configuration"]["onvif"].update(port=9999),
    "selected_config_hash": lambda selected, other: selected.update(configSha256="f" * 64),
    "unrelated_id": lambda selected, other: other.update(id="f" * 64),
    "unrelated_image": lambda selected, other: other.update(imageId=NEW_ID),
    "unrelated_configuration": lambda selected, other: other["configuration"]["onvif"].update(port=9999),
    "mac": lambda selected, other: selected.update(mac="02:00:00:00:00:ff"),
    "network": lambda selected, other: selected.update(network="f" * 64),
    "runtime_uid": lambda selected, other: selected["inspect"]["Config"].update(User="0"),
    "environment": lambda selected, other: selected["inspect"]["Config"]["Env"].append("UNEXPECTED=1"),
    "restart": lambda selected, other: selected["inspect"]["HostConfig"]["RestartPolicy"].update(Name="no"),
    "readonly": lambda selected, other: selected["inspect"]["HostConfig"].update(ReadonlyRootfs=False),
    "privileged": lambda selected, other: selected["inspect"]["HostConfig"].update(Privileged=True),
    "render_device": lambda selected, other: selected["inspect"]["HostConfig"]["Devices"][0].update(PathInContainer="/changed"),
    "render_group": lambda selected, other: selected["inspect"]["HostConfig"].update(GroupAdd=["0"]),
    "mount_source": lambda selected, other: selected["inspect"]["HostConfig"]["Mounts"][0].update(Source="/changed"),
    "mount_readonly": lambda selected, other: selected["inspect"]["HostConfig"]["Mounts"][0].update(ReadOnly=False),
    "mount_propagation": lambda selected, other: selected["inspect"]["HostConfig"]["Mounts"][1]["BindOptions"].update(Propagation="shared"),
    "mount_duplicate": lambda selected, other: selected["inspect"]["HostConfig"]["Mounts"].append(copy.deepcopy(selected["inspect"]["HostConfig"]["Mounts"][0])),
    "unrelated_restart": lambda selected, other: other["inspect"]["State"].update(RestartCount=1),
    "extra_compose_file": lambda selected, other: selected["composeSha256"].update({"/unexpected": "f" * 64}),
}
for case, mutation in DRIFTS.items():
    setattr(DeployTests, "test_drift_" + case, drift_case(case, mutation))


if __name__ == "__main__":
    unittest.main()
