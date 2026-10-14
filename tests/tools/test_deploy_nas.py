"""Offline adapter tests use synthetic responses and temporary local files only."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import unittest

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
        self.adapter.monotonic = lambda: 20
        self.commands = []
        self.entry = {"id": "a" * 64, "configuration": {"users": [{"name": "synthetic", "pass": SECRET}], "onvif": {
            "profiles": [{"codec": "H265", "width": 896, "height": 512}], "low_quality_encoder": "vaapi", "low_quality_decoder": "vaapi", "event_replay_path": "/private/replay.json"}},
            "inspect": {"State": {"Health": {"Status": "healthy"}}}}
        self.metrics = {"status": "ready", "streams": [{"profile": "main", "codec": "H265", "width": 896, "height": 512, "liveVideo": True, "videoReady": True, "lastVideoAgeMs": 20, "totalAccessUnits": 100,
                        "audio": {"available": True, "metadataCurrent": True, "isAac": True, "sampleRate": 16000, "channels": 1}}],
                        "lowQualityEncoder": {"state": "active", "encoder": "hevc_vaapi", "failures": 0, "softwareFallback": False, "decoder": "vaapi"},
                        "lowQualityDecoder": {"backend": "vaapi", "hardwareDecodeRequested": True, "noSilentFallback": True},
                        "events": {"subscriptions": 1, "replay": {"enabled": True, "state": "ready", "failures": 0}}, "privateValue": SECRET}
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


if __name__ == "__main__":
    unittest.main()
