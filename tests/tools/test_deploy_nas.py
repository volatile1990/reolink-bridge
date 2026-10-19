"""Offline adapter tests use synthetic responses and temporary local files only."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
for name in ("engine", "nas"):
    module_name = "reolink_deploy_" + name
    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(module_name, ROOT / ("tools/deploy/" + name + ".py"))
        module = importlib.util.module_from_spec(spec); sys.modules[module_name] = module; spec.loader.exec_module(module)
engine, nas = sys.modules["reolink_deploy_engine"], sys.modules["reolink_deploy_nas"]
SECRET = "SYNTHETIC_ADAPTER_SECRET"


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.adapter = nas.DockerNas.__new__(nas.DockerNas)
        self.adapter.docker = "synthetic-docker"
        self.adapter.event_delivery_mode = "onvif-pullpoint"
        self.adapter.monotonic = lambda: 20
        self.commands = []
        self.entry = {"id": "a" * 64, "configuration": {"users": [{"name": "synthetic", "pass": SECRET}], "onvif": {
            "profiles": [{"codec": "H265", "width": 896, "height": 512}], "low_quality_encoder": "vaapi", "low_quality_decoder": "vaapi", "event_replay_path": "/private/replay.json"}},
            "inspect": {"State": {"Health": {"Status": "healthy"}}}}
        self.metrics = {"status": "ready", "streams": [{"profile": "main", "codec": "H265", "width": 896, "height": 512, "liveVideo": True, "videoReady": True, "lastVideoAgeMs": 20, "totalAccessUnits": 100,
                        "audio": {"available": True, "metadataCurrent": True, "isAac": True, "sampleRate": 16000, "channels": 1}}],
                        "lowQualityEncoder": {"state": "active", "encoder": "hevc_vaapi", "failures": 0, "softwareFallback": False, "decoder": "vaapi"},
                        "lowQualityDecoder": {"backend": "vaapi", "hardwareDecodeRequested": True, "noSilentFallback": True},
                        "events": {"subscriptions": 1, "source": {"connected": True},
                            "replay": {"enabled": True, "state": "ready", "failures": 0}}, "privateValue": SECRET}
        def command(args, data=None, **kwargs):
            self.commands.append((args, data))
            return json.dumps(self.metrics).encode()
        self.adapter.command = command

    def test_authenticated_metadata_probe_hides_secrets(self):
        result = self.adapter.sample(self.entry)
        self.assertNotIn(SECRET, json.dumps(result))
        args, data = self.commands[0]
        self.assertNotIn(SECRET, json.dumps(args)); self.assertEqual(args[-2:], ["--config", "-"])
        self.assertNotIn("-L", args); self.assertIn(b"Authorization: Basic", data)

    def test_geometry_change_blocks(self):
        self.metrics["streams"][0]["width"] = 640
        with self.assertRaisesRegex(engine.GuardError, "source_geometry_or_codec_drift"):
            self.adapter.sample(self.entry)

    def test_missing_audio_blocks(self):
        self.metrics["streams"][0]["audio"]["available"] = False
        with self.assertRaisesRegex(engine.GuardError, "source_aac_not_fresh"):
            self.adapter.sample(self.entry)

    def test_stale_video_blocks(self):
        self.metrics["streams"][0]["lastVideoAgeMs"] = 6000
        with self.assertRaisesRegex(engine.GuardError, "source_video_not_fresh"):
            self.adapter.sample(self.entry)

    def test_encoder_failure_blocks(self):
        self.metrics["lowQualityEncoder"]["failures"] = 1
        with self.assertRaisesRegex(engine.GuardError, "hardware_encoder_not_ready"):
            self.adapter.sample(self.entry)

    def test_decoder_selection_change_blocks(self):
        self.metrics["lowQualityDecoder"]["backend"] = "software"
        with self.assertRaisesRegex(engine.GuardError, "hardware_decoder_selection_drift"):
            self.adapter.sample(self.entry)

    def test_native_passthrough_has_no_conversion(self):
        self.entry["configuration"]["onvif"].pop("low_quality_encoder")
        self.entry["configuration"]["onvif"].pop("low_quality_decoder")
        self.metrics["lowQualityEncoder"] = None; self.metrics["lowQualityDecoder"] = None
        self.assertEqual(self.adapter.sample(self.entry)["streams"][0]["profile"], "main")

    def test_native_passthrough_rejects_unrequested_decoder(self):
        self.entry["configuration"]["onvif"].pop("low_quality_encoder")
        with self.assertRaisesRegex(engine.GuardError, "native_passthrough_conversion_present"):
            self.adapter.sample(self.entry)

    def test_replay_failure_blocks(self):
        self.metrics["events"]["replay"]["failures"] = 1
        with self.assertRaisesRegex(engine.GuardError, "durable_replay_not_ready"):
            self.adapter.sample(self.entry)

    def test_missing_event_subscription_blocks(self):
        self.metrics["events"]["subscriptions"] = 0
        with self.assertRaisesRegex(engine.GuardError, "event_subscription_not_ready"):
            self.adapter.sample(self.entry)

    def test_native_replay_accepts_zero_external_pullpoints_and_bounded_counts(self):
        self.adapter.event_delivery_mode = "native-replay"
        for count in (0, 1, 8):
            with self.subTest(count=count):
                self.metrics["events"]["subscriptions"] = count
                result = self.adapter.sample(self.entry)
                self.assertEqual(result["streams"][0]["profile"], "main")
                self.assertNotIn(SECRET, json.dumps(result))

    def test_native_replay_requires_existing_replay_configuration(self):
        self.adapter.event_delivery_mode = "native-replay"; self.metrics["events"]["subscriptions"] = 0
        for value in (None, "", False, 123):
            with self.subTest(path_shape=type(value).__name__):
                self.entry["configuration"]["onvif"]["event_replay_path"] = value
                with self.assertRaisesRegex(engine.GuardError, "native_replay_configuration_required"):
                    self.adapter.sample(self.entry)
        self.entry["configuration"]["onvif"].pop("event_replay_path")
        with self.assertRaisesRegex(engine.GuardError, "native_replay_configuration_required"):
            self.adapter.sample(self.entry)

    def test_native_replay_requires_connected_event_publisher(self):
        self.adapter.event_delivery_mode = "native-replay"; self.metrics["events"]["subscriptions"] = 0
        for source in (None, {}, {"connected": False}, {"connected": 1}, {"connected": "true"}, "connected"):
            with self.subTest(source_shape=type(source).__name__):
                self.metrics["events"]["source"] = source
                with self.assertRaisesRegex(engine.GuardError, "native_event_source_not_connected"):
                    self.adapter.sample(self.entry)

    def test_native_replay_requires_enabled_ready_replay_without_failures(self):
        self.adapter.event_delivery_mode = "native-replay"; self.metrics["events"]["subscriptions"] = 0
        for replay in ({}, {"enabled": False, "state": "ready", "failures": 0},
                       {"enabled": True, "state": "loading", "failures": 0},
                       {"enabled": True, "state": "ready", "failures": 1},
                       {"enabled": True, "state": "ready", "failures": False}):
            with self.subTest(replay=replay):
                self.metrics["events"]["replay"] = replay
                with self.assertRaisesRegex(engine.GuardError, "durable_replay_not_ready"):
                    self.adapter.sample(self.entry)

    def test_native_replay_refuses_invalid_external_subscription_counts(self):
        self.adapter.event_delivery_mode = "native-replay"
        for count in (-1, 9, True, False, 1.0, "1", None):
            with self.subTest(count=count):
                self.metrics["events"]["subscriptions"] = count
                with self.assertRaisesRegex(engine.GuardError, "event_subscription_count_invalid"):
                    self.adapter.sample(self.entry)

    def test_native_replay_preserves_video_audio_and_hardware_readiness_guards(self):
        self.adapter.event_delivery_mode = "native-replay"; self.metrics["events"]["subscriptions"] = 0
        original = copy.deepcopy(self.metrics)
        for mutate, expected in (
            (lambda value: value["streams"][0].update(lastVideoAgeMs=6000), "source_video_not_fresh"),
            (lambda value: value["streams"][0]["audio"].update(available=False), "source_aac_not_fresh"),
            (lambda value: value["lowQualityEncoder"].update(failures=1), "hardware_encoder_not_ready")):
            self.metrics = copy.deepcopy(original); mutate(self.metrics)
            with self.assertRaisesRegex(engine.GuardError, expected): self.adapter.sample(self.entry)


class DedicatedComposeHostTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.scope = Path(self.temp.name) / 'bridges'
        self.scope.mkdir()
        self.adapter = nas.DockerNas({'scope': str(self.scope), 'runtimeUid': 1026, 'renderGroup': 992,
            'restartPolicy': 'unless-stopped', 'maintenanceMode': 'deployment-lock'})
        self.items, self.services = [], {}
        for index, ip in enumerate(nas.DEFAULT_IPS):
            name = 'camera-' + str(index)
            path = self.scope / (name + '.json')
            mac = '02:42:c0:a8:1e:%02x' % (210 + index)
            path.write_text(json.dumps({'onvif': {'advertised_host': ip, 'mac': mac,
                'low_quality_encoder': 'vaapi'}, 'users': [{'name': 'synthetic', 'pass': SECRET}],
                'cameras': [{'address': 'synthetic-source'}]}))
            mount = {'Type': 'bind', 'Source': str(path), 'Destination': '/run/secrets/bridge-config.json', 'RW': False}
            self.services[name] = {'image': 'synthetic-image', 'read_only': True, 'restart': 'unless-stopped',
                'networks': {'camera-network': {'ipv4_address': ip}}, 'volumes': [
                    {'source': str(path), 'target': mount['Destination'], 'read_only': True}]}
            self.items.append({'Id': str(index) * 64, 'Name': '/' + name, 'Image': 'synthetic-image-id',
                'Config': {'Image': 'synthetic-image', 'User': '1026:100', 'Env': [], 'Labels': {
                    'com.docker.compose.project': self.adapter.project,
                    'com.docker.compose.project.config_files': str(self.adapter.base),
                    'com.docker.compose.project.working_dir': str(self.scope), 'com.docker.compose.service': name}},
                'HostConfig': {'ReadonlyRootfs': True, 'Privileged': False,
                    'RestartPolicy': {'Name': 'unless-stopped'}, 'GroupAdd': ['992'], 'Devices': [
                        {'PathOnHost': '/dev/dri/renderD128', 'PathInContainer': '/dev/dri/renderD128', 'CgroupPermissions': 'rwm'}]},
                'State': {'Running': True}, 'Mounts': [mount], 'NetworkSettings': {'Networks': {
                    'camera-network': {'IPAddress': ip, 'MacAddress': mac, 'NetworkID': 'synthetic-network'}}}})
        self.adapter.base.write_text(json.dumps({'services': self.services}))
        self.adapter.resolved = lambda: {'services': self.services}
        def command(args, **kwargs):
            if args[1:3] == ['ps', '-q']:
                return b'synthetic-ids'
            if args[1] == 'inspect':
                return json.dumps(self.items).encode()
            if args[1:3] == ['network', 'inspect']:
                return b'[{"Driver":"macvlan"}]'
            raise AssertionError('unexpected non-read-only command')
        self.adapter.command = command

    def test_clean_scope_native_gid_and_unless_stopped_are_supported(self):
        result = self.adapter.inventory()
        self.assertEqual(len(result), 6)
        self.assertTrue(all(Path(row['directory']) == self.scope for row in result))
        self.assertNotIn(SECRET, json.dumps(engine.projected(result)))

    def test_legacy_gid_or_restart_policy_drift_is_not_silently_adopted(self):
        self.items[0]['HostConfig']['GroupAdd'] = ['937']
        with self.assertRaisesRegex(engine.GuardError, 'existing_render_group_required'):
            self.adapter.inventory()
        self.items[0]['HostConfig']['GroupAdd'] = ['992']
        self.services['camera-0']['restart'] = 'always'
        with self.assertRaisesRegex(engine.GuardError, 'existing_restart_policy_required'):
            self.adapter.inventory()

    def test_dedicated_release_uses_only_local_lock_without_watchdog_marker(self):
        lock = Mock()
        stub = types.SimpleNamespace(LOCK_EX=2, LOCK_NB=4, flock=lock)
        with patch.object(nas, 'fcntl', stub):
            with self.adapter.maintenance():
                self.assertTrue((self.scope / '.reolink-release.lock').is_file())
                self.assertFalse((self.scope / 'maintenance').exists())
        lock.assert_called_once()

    def test_legacy_defaults_stay_compatible_and_invalid_settings_fail(self):
        legacy = nas.DockerNas({'scope': str(self.scope)})
        self.assertEqual((legacy.runtime_uid, legacy.render_group, legacy.restart_policy, legacy.maintenance_mode, legacy.event_delivery_mode),
                         (1026, 937, 'always', 'watchdog', 'onvif-pullpoint'))
        self.assertEqual(self.adapter.event_delivery_mode, 'onvif-pullpoint')
        for key, value in (('renderGroup', True), ('renderGroup', -1), ('restartPolicy', 'on-failure'),
                           ('maintenanceMode', 'none'), ('eventDeliveryMode', 'unknown'), ('eventDeliveryMode', None)):
            with self.subTest(key=key, value=value), self.assertRaises(engine.GuardError):
                nas.DockerNas({'scope': str(self.scope), key: value})

    def test_native_event_delivery_requires_explicit_setting_independent_of_maintenance(self):
        for maintenance in ('watchdog', 'deployment-lock'):
            with self.subTest(maintenance=maintenance):
                explicit = nas.DockerNas({'scope': str(self.scope), 'maintenanceMode': maintenance,
                    'eventDeliveryMode': 'native-replay'})
                self.assertEqual(explicit.event_delivery_mode, 'native-replay')
                implicit = nas.DockerNas({'scope': str(self.scope), 'maintenanceMode': maintenance})
                self.assertEqual(implicit.event_delivery_mode, 'onvif-pullpoint')


if __name__ == "__main__":
    unittest.main()
