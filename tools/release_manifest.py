"""Describe and optionally build the exact source tree; exclude private runtime files."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_NAMES = {"config.json", "config.toml", "users.json", "settings.json", "secret.key", ".env"}
PRIVATE_PARTS = {"secrets", "recordings", "snapshots", "diagnostics", "state", "bin", "obj", ".git", "__pycache__"}


def source_files(root):
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    result = []
    for name in sorted(set(names.decode("utf-8").split("\0"))):
        if not name:
            continue
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or set(relative.parts) & PRIVATE_PARTS or relative.name in PRIVATE_NAMES:
            continue
        if relative.name.startswith(".env.") and relative.name != ".env.example":
            continue
        path = root / name
        try:
            path.resolve().relative_to(root.resolve())
        except ValueError:
            raise ValueError("Source resolves outside its repository") from None
        if path.is_symlink():
            raise ValueError("Source symlink is not a reproducible local file")
        if path.is_file():
            result.append((name, path))
    return result


def source_digest(files):
    digest = hashlib.sha256()
    for name, path in files:
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def normalized_content(content):
    """Normalize only UTF-8 text line endings; every other byte remains significant."""
    if b"\0" in content:
        return content
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return content
    return content.replace(b"\r\n", b"\n")


def normalized_source_digest(files):
    digest = hashlib.sha256()
    for name, path in files:
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(hashlib.sha256(normalized_content(path.read_bytes())).digest())
    return digest.hexdigest()


def manifest(root, artifact=None, canonical=False):
    files = source_files(root)
    version = ET.parse(root / "src/Neolink.Server/Neolink.Server.csproj").findtext("./PropertyGroup/Version")
    revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"]))
    return {"schemaVersion": 1, "project": "reolink-bridge", "version": version,
            "sourceRevision": revision, "sourceDirty": dirty,
            "sourceSha256": normalized_source_digest(files) if canonical else source_digest(files),
            "workingTreeSourceSha256": source_digest(files), "normalizedSourceSha256": normalized_source_digest(files),
            "canonicalLineEndings": canonical,
            "sourceFiles": len(files), "buildTime": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "artifactSha256": hashlib.sha256(artifact.read_bytes()).hexdigest() if artifact else None}


def archive_sources(files, destination, canonical=False):
    with zipfile.ZipFile(destination, "x", zipfile.ZIP_DEFLATED) as archive:
        for name, path in files:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            content = path.read_bytes()
            archive.writestr(info, normalized_content(content) if canonical else content)


def build_release(root, before, dotnet):
    publish = root / "artifacts" / ("release-" + before["sourceSha256"][:16])
    try:
        publish.resolve().relative_to(root.resolve())
    except ValueError:
        raise ValueError("Release output resolves outside its repository") from None
    if publish.exists() or publish.is_symlink():
        raise FileExistsError("Use a fresh release directory; existing output may contain runtime files")
    subprocess.run([dotnet, "publish", str(root / "src/Neolink.Server/Neolink.Server.csproj"),
                    "--configuration", "Release", "--output", str(publish),
                    "-p:Version=" + before["version"], "-p:SourceRevisionId=sha256-" + before["sourceSha256"],
                    "-p:ExcludePrivateConfig=true", "-p:UseAppHost=false", "-p:PublishSingleFile=false",
                    "-p:SelfContained=false"], check=True)
    return publish / "neolink.net.dll"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--canonical", action="store_true", help="Freeze UTF-8 sources with Git-compatible LF line endings")
    parser.add_argument("--dotnet", default=shutil.which("dotnet"))
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink() or args.archive and (args.archive.exists() or args.archive.is_symlink()):
        parser.error("Use fresh output paths")
    if args.canonical and args.build:
        parser.error("Build the canonical source archive in the deployment worker")
    before = manifest(ROOT, canonical=args.canonical)
    artifact = args.artifact
    if args.build:
        if not args.dotnet:
            parser.error(".NET 10 SDK is required")
        artifact = build_release(ROOT, before, args.dotnet)
    value = manifest(ROOT, artifact, canonical=args.canonical)
    if value["sourceSha256"] != before["sourceSha256"]:
        raise SystemExit("Sources changed during release preparation")
    if args.archive:
        args.archive.parent.mkdir(parents=True, exist_ok=True)
        archive_sources(source_files(ROOT), args.archive, canonical=args.canonical)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(value))


if __name__ == "__main__":
    main()
