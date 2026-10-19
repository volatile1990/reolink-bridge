# Owned NAS deployment engine

This package updates only the image of the six existing bridge services at
`192.168.30.210` through `.215`. It does not add replay settings, change decoder
selection, edit camera credentials, change stream profiles, update DSM or Protect,
poll external releases, or schedule work. Existing configuration bytes, UID,
render-device access, network identities and runtime settings are preserved.

The Python 3.8 worker uses only the standard library. The local CLI transports the
owned modules in memory through an external protected connector exposing
`nas_execute(command, payload=..., sudo=True, timeout=...)`. Configure its absolute
path with `--connector` or `REOLINK_NAS_CONNECTOR`; the connector and its protected
storage must remain outside this repository. No credentials are needed in CLI
requests or command arguments.

```console
python tools/deploy/cli.py --payload request.json --output fresh-result.json
```

Every request is JSON. `inventory` needs only `{"mode":"inventory"}`. Deployment
requests carry a frozen `release` descriptor:

```json
{
  "version": "1.4.0",
  "sourceSha256": "<64 lowercase hex characters>",
  "imageId": "sha256:<64 lowercase hex characters>",
  "imageTag": "reolink-bridge:<immutable release tag>",
  "testsPassed": true,
  "testedApplicationDllIdentical": true
}
```

Optional descriptor fields are `sourceRevision` (40 hex characters),
`archiveSha256` and `applicationDllSha256` (64 hex characters). The image ID,
tag-to-ID mapping, source-digest OCI label and version label are checked. The
coordinator supplies test gates only after its actual build/audit passes; declaring
these fields does not run tests or validate Protect playback.

The default settings use the existing `/volume1/docker/reolink-bridge-pilot`
directory, its JSON-formatted `pilot-compose.yaml`, the existing project, runtime
UID `1026`, render group `937` and watchdog directory
`/var/lib/reolink-camera-resilience`. Optional `settings` can supply `scope`,
`runtimeUid` and `watchdogDirectory`; the current six IPs and project remain exact.

1. `plan`: send `mode` and `release`. The response contains `baseline`, projected
   `bridges` and ordered `pendingIps`. Root-private baseline/config/Compose copies
   and a checkpoint stay on the NAS, with directory mode 0700 and file mode 0600.
2. `apply`: send `baseline`, `release`, the next ordered `ip`, and `expected` equal
   to a fresh inventory's `bridges`. Exactly one service is recreated under the
   existing watchdog lock and an owned maintenance marker. Source video, AAC,
   frame cadence and existing GPU/replay settings must recover. Failure restores
   the selected previous image if no concurrent file drift prevents recovery.
3. After each accepted NAS step, the coordinator must run its independent Protect
   live/Automatic/recording gates. If one fails, `rollback` with that last successful
   `receipt`, `baseline`, `release` and fresh `expected` restores that step. The
   original success/failure receipts are retained; the active chain is checkpointed.
4. `resume`: send `baseline` and `release`. It verifies the exact last confirmed
   state and returns `receipts`, `attempts`, `pendingIps` and fresh `bridges`.
   A verified automatic rollback may have changed only its selected container ID;
   this is represented in the private checkpoint rather than silently ignored.
5. `final`: send `baseline`, `release` and fresh `expected`, optionally the exact
   `receipts` list. All six successful, ordered receipts and the byte-exact Compose
   chain are required. The response exposes `rollbackManifest` and safe metadata.
6. `rollback-all`: send `baseline`, `release` and fresh `expected`. The verified
   successful prefix of zero through six steps is restored in reverse order,
   with a verified checkpoint after every step. Its initial receipt chain is
   frozen privately; `resume` exposes only the remaining active successful
   receipts and a `rollback-pending`/`rolled-back` phase. A completed rollback
   proves every baseline image/configuration and exact Compose bytes restored.
   An unverified interrupted mutation blocks continuation until investigated.

Mount comparisons sort complete members only when targets are distinct. Source,
target, permissions, options and multiplicity remain significant. Auto-generated
container hostname and known Compose lifecycle labels are normalized; complete
runtime/HostConfig checks remain strict. Main source failures are never repaired
by changing camera settings, and no implicit software fallback is introduced.

Image environment entries are compared as unique key/value mappings; malformed
entries and duplicate keys are refused. The only permitted value change is a
matching `DOTNET_VERSION` / `ASPNET_VERSION` patch upgrade within the same major
and minor version. All other environment keys/values and image defaults remain
exact. Runtime expectations update only values proven inherited from the old
image; explicit Compose environment overrides stay unchanged even when they
equal that old default. A verified receipt rollback may restore the original
framework patch pair; planning a downgrade is refused.

`build` accepts `sourceManifest` (`version`, `sourceRevision`, `sourceSha256`,
`sourceFiles`, `sourceDirty: false`, `canonicalLineEndings: true`),
`archiveSha256` and `archiveBase64`. Optional `imageTag` defaults to
`reolink-bridge:managed-<source digest prefix>`. It validates each allowed ZIP entry and the exact
raw-byte source digest, then builds audit and production images from those exact
sources without updating a running service. Selftests, Protect contracts and the
GPU-only regressions run with no network, no bound volumes, UID 1026:100, render
group 937, two CPU cores and a 1 GiB memory budget. The actual application DLL in
the selftest image, contract dependency and production image must match exactly.
Owned containers, the audit image and temporary source directory are removed;
private logs and evidence remain in the receipt. Only a successful audit with
confirmed cleanup returns a nested deployable `release` descriptor with
`testsPassed` and `testedApplicationDllIdentical` as true.

Every audit container requests both a 256-task cgroup limit and
`RLIMIT_NPROC` with soft/hard limits of 256. The DSM kernel measured on this NAS
has no pids controller: Docker reports `PidsLimit: null`. That specific fallback
is accepted only when the inspected `nproc` limit is exactly 256/256. Before any
application tests, an identically isolated owned container reads
`/proc/self/limits`; its actual kernel `Max processes` must also be 256/256.
The public result reports `processLimiter: "rlimit_nproc"`, the kernel proof and
`cgroupPidsLimit: false`. This process/thread limit counts the shared runtime UID
1026 across containers and is stricter than a per-container allowance. CPU and
memory guards remain unchanged. Unsupported or unproven limits abort the audit.

An orchestrator can freeze the public worker code once with `cli.source_bundle()`
and pass it to every `cli.invoke(payload, connector, sources=frozen_modules)` call.
The CLI equivalent is `--sources frozen-worker.json`. Exact module-name checks
prevent loading unrelated names. Applying and recovering use the same frozen
worker even if a local checkout changes later.

Offline contracts exercise drift, mount permutations, real state transitions,
canary recovery, resuming, finite receipt chains and reverse restoration. They do
not contact a NAS or a Docker daemon:

```console
python -m unittest discover -s tests/tools -p "test_deploy*.py"
```
