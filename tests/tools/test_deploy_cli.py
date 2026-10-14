"""Connector transport checks using a synthetic local callback, never a NAS."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("owned_deploy_cli", ROOT / "tools/deploy/cli.py")
cli = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(cli)


class CliTests(unittest.TestCase):
    def test_exact_owned_module_names(self):
        self.assertEqual(set(cli.source_bundle()), {"engine", "nas", "build", "worker"})

    def test_frozen_bundle_prevents_later_source_reads(self):
        with tempfile.TemporaryDirectory() as folder:
            connector = Path(folder)/"protected_callback.py"
            connector.write_text("import json\ndef nas_execute(command,payload,sudo,timeout):\n b=json.loads(payload)\n return {'exit_code':0,'stdout':json.dumps({'ok':True,'mode':b['payload']['mode'],'moduleSnapshot':b['sources']})}\n", encoding="utf-8")
            frozen = {name:"# frozen public "+name for name in cli.MODULE_NAMES}
            with mock.patch.object(cli,"source_bundle",side_effect=AssertionError("source_read_after_freeze")):
                result = cli.invoke({"mode":"inventory"},connector,sources=frozen)
            self.assertEqual(result["moduleSnapshot"],frozen)

    def test_unknown_module_name_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            connector=Path(folder)/"protected_callback.py"
            connector.write_text("def nas_execute(*args,**kwargs):\n raise AssertionError('must_not_execute')\n",encoding="utf-8")
            frozen={name:"# public"for name in cli.MODULE_NAMES};frozen["unrelated"]="# rejected"
            with self.assertRaisesRegex(ValueError,"owned_worker_module_bundle_required"):
                cli.invoke({"mode":"inventory"},connector,sources=frozen)


if __name__=="__main__":unittest.main()
