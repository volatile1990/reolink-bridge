import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

FILE = Path(__file__).resolve().parents[2] / "tools/release_manifest.py"
spec = importlib.util.spec_from_file_location("release_manifest", FILE)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def test_release_build_excludes_private_config_and_refuses_existing_output(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            before = {"sourceSha256": "a" * 64, "version": "1.2.0"}
            with patch.object(release.subprocess, "run") as run:
                artifact = release.build_release(root, before, "synthetic-dotnet")
                self.assertIn("-p:ExcludePrivateConfig=true", run.call_args.args[0])
                self.assertIn("-p:SourceRevisionId=sha256-" + "a" * 64, run.call_args.args[0])
                artifact.parent.mkdir(parents=True)
                (artifact.parent / "config.json").write_text("synthetic", encoding="utf-8")
                with self.assertRaises(FileExistsError):
                    release.build_release(root, before, "synthetic-dotnet")
                self.assertEqual(run.call_count, 1)

    def test_private_files_excluded_and_digest_tracks_contents(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            for name in ("safe.py", "config.json", ".env", "secrets/private", "src/obj/private"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("synthetic", encoding="utf-8")
            files = release.source_files(root)
            self.assertEqual([name for name, _ in files], ["safe.py"])
            before = release.source_digest(files)
            (root / "safe.py").write_text("changed", encoding="utf-8")
            self.assertNotEqual(before, release.source_digest(files))

    def test_archives_are_deterministic_and_refuse_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / "source.py"
            path.write_text("synthetic", encoding="utf-8")
            files = [("source.py", path)]
            one, two = root / "one.zip", root / "two.zip"
            release.archive_sources(files, one)
            release.archive_sources(files, two)
            self.assertEqual(one.read_bytes(), two.read_bytes())
            with zipfile.ZipFile(one) as archive:
                self.assertEqual(archive.namelist(), ["source.py"])
            with self.assertRaises(FileExistsError):
                release.archive_sources(files, one)


if __name__ == "__main__":
    unittest.main()
