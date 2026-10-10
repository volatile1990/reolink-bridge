# Protect ONVIF contract checks

Run with the .NET 10 SDK:

```sh
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
```

This console runner uses no third-party testing framework. It starts the actual
ONVIF HTTP listener on an ephemeral loopback port and sends SOAP requests using
synthetic credentials. Its media hubs report controlled states without opening
any physical camera, UDP discovery listener or recording file. The XML
entity and configuration tests create and remove synthetic temporary files.

The assertions check the externally visible adoption contract: unauthenticated
clock access, authenticated device identity, both media service versions, honest
H.265/H.264 profile descriptions, separate main/sub RTSP URLs, WSSE SHA-1 digest
authentication, clock bounds and replay protection, HTTP Basic authentication,
source readiness and safe faults for malformed XML or unsupported operations.
The shipped pilot example is loaded strictly, and incompatible camera/session,
identity, port and authentication configurations must be rejected.

Metrics checks use the actual `StreamHub` with an injected monotonic clock. They
cover the first frame at timestamp zero, frame/byte counters, arrival gaps,
source reconnects and an HTTP health failure after five seconds without video.
No waiting is needed to simulate a stalled source. The `/metrics` endpoint must
require valid HTTP Basic credentials and expose only the documented diagnostic
fields, with no source addresses, camera names or authentication details.

The authentication-parking checks use a synthetic Baichuan TCP camera on
loopback and the actual `BcCamera` and `CameraService`. They distinguish an
explicit phase-two 401, an older empty modern refusal and a non-authentication
500 reply. Both authentication refusals must make just one login attempt during
a parallel 32-second observation window, longer than the former retry delay.
RTSP and ONVIF remain responsive, health reports failure, authenticated metrics
identify the failure, and cancellation still completes promptly. A transport
disconnect must continue to reconnect. These checks add about 33 seconds to the
contract test run and do not contact physical cameras.

The video-buffer checks distinguish bundled pictures from continuation slices
and metadata-only buffers. The optional GOP playout checks use controlled source
arrival clocks and the actual loopback RTSP/TCP server. They verify AU markers,
outbound timestamps, parameter/SEI preservation and byte-identical encoded NALs
after RTP fragmentation, both aliases, video-only policy, bounded buffering,
clock wrap, real queue gaps and cancellation/recovery. The default RTSP path
must still forward the original buffer and timestamp. No real media is used.

Snapshot contracts exercise Media1/Media2 URI replies and the binary HTTP endpoint,
including authentication before capture, profile lookup, unavailable or stale
sources, source-epoch changes, byte limits and deadlines. Native provider checks
cover the monotonic five-second cache, shared capture cancellation, session changes,
invalid JPEGs and host shutdown. Synthetic Baichuan peers verify the bounded native
109 command, chunk assembly, FullAES replies and rejection of late cancelled data.
They do not open an additional stream or contact a real camera.

Snapshot-only Digest checks cover MD5 with `qop=auth`, the exact GET target,
monotonic nonce expiry, bounded nonce/replay state, atomic request-count advancement,
wrong credentials and malformed or duplicate directives. Preemptive Basic remains
available; SOAP and metrics retain their existing authentication contracts.

Event contracts use the actual HTTP listener and Baichuan alarm XML parser.
They cover authenticated event discovery, camera-originated motion and AI state,
subscription ownership, WSSE replay refusal, topic filters, synchronization,
renewal, unsubscribe, long-poll wake/cancel, monotonic expiry, stale/source-loss
clears, bounded subscriptions and queue overflow resynchronization. No AI class
is advertised before a real camera push has supplied that class.

The protected JSON replay checks retain a short AI-only start/stop pulse,
preserve bounded raw alarm tokens and status, reject invalid cursors, verify
Basic/Digest authentication, report ring gaps and distinguish process restarts.
Bridge housekeeping and outside controls never enter the camera replay ring.
These checks do not prove that a particular camera emits alarm pushes or that
Protect turns a specific class into a native smart detection.

Motion-policy checks exercise `all`, `classified` and `none`. The default keeps
ordinary movement; `classified` gates it on real camera verdicts; `none` keeps
ordinary ONVIF motion false while preserving class topics and the complete raw
camera replay. Wire checks cover synchronization, repeated activity, real class
endings, stale clears, source resets and connection epochs without fabricated
replay entries.

These checks establish protocol behavior only. They do not establish that a
specific camera's Baichuan video is decodable, that UniFi Protect will render
H.265 in a particular browser, or that an actual deployment sustains recording.
The Synology pilot must verify those separately, including whether each camera's
firmware supports native snapshots and Protect accepts the advertised JPEG URI.
