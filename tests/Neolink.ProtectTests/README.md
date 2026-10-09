# Protect ONVIF contract checks

Run with the .NET 10 SDK:

```sh
dotnet run --project tests/Neolink.ProtectTests/Neolink.ProtectTests.csproj --configuration Release
```

This console runner uses no third-party testing framework. It starts the actual
ONVIF HTTP listener on an ephemeral loopback port and sends SOAP requests using
synthetic credentials. Its media hubs report controlled states without opening
any camera, RTSP session, UDP discovery listener or recording file. The XML
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

These checks establish protocol behavior only. They do not establish that a
specific camera's Baichuan video is decodable, that UniFi Protect will render
H.265 in a particular browser, or that an actual deployment sustains recording.
The Synology pilot must verify those separately.
