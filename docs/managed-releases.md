# Managed own-code releases

The maintained deployment engine lives in `tools/deploy`. It updates the explicit
six-camera Synology scope one image at a time. The companion
[Events release controller](https://github.com/volatile1990/reolink-protect-events/blob/main/docs/operations-and-releases.md)
runs tests, media gates, backups, release installation and guarded restoration.
UniFi Protect remains the central recording and playback system.

This is an update pipeline for the owner's newly developed code, not a recurring
download of third-party repository updates. The local `main` push hook installed
by Events triggers it automatically on the configured home computer.

## Worker contract

`tools/deploy/cli.py` ships its own Python 3.8-compatible modules through stdin to
an external protected NAS connector. The connector exposes `nas_execute`; keep
it and its credential store outside the repository. Public JSON requests contain
no camera credentials. The worker reads existing private NAS config only there.

The `build` mode accepts a frozen ZIP, its SHA-256 and a clean canonical source
manifest. It validates paths, bytes and content hashes; builds audit/production
images with the same version/source identity; runs resource-limited selftests,
Protect tests and an actual VAAPI low-quality test; and compares all application
DLL copies. Only an identical tested production DLL yields a deployable release.
Build receipts require cleanup of owned temporary containers and preservation of
the complete production inventory.

`plan` captures the exact six-container baseline, Compose bytes and configuration
in a private checkpoint. `apply` requires that baseline and the expected current
inventory; `resume` returns the active successful receipt chain and pending IPs.
`final` verifies all six deployment receipts. `rollback` restores the most recent
successful step, while `rollback-all` handles any successful prefix of zero to
six cameras and resumes an interrupted reverse sequence.

Configuration files, immutable container settings, user/group/device identity,
replay mounts and unrelated Compose fields are compared in full. Docker Mounts
members are canonicalized by order only: no field is discarded and duplicate
members retain their multiplicity. Health requires fresh actual video progress,
measured against the baseline, so night-time camera frame rates are supported.

Source freezing normalizes only UTF-8 CRLF to LF. The manifest keeps working-tree
and canonical source hashes; binary contents remain exact. Thus Git checkout line
endings cannot silently change the source identity of the image that was tested.
