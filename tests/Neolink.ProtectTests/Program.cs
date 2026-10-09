using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using System.Xml;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Streaming;

return await ContractTests.RunAsync();

internal static class ContractTests
{
    private const string Device = "http://www.onvif.org/ver10/device/wsdl";
    private const string Media1 = "http://www.onvif.org/ver10/media/wsdl";
    private const string Media2 = "http://www.onvif.org/ver20/media/wsdl";
    private const string Wsse = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd";
    private const string Wsu = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd";
    private const string User = "protect-test";
    private const string Password = "Synthetic-test-only<&\"password";

    public static async Task<int> RunAsync()
    {
        var main = new TestHub("test/mainStream");
        var sub = new TestHub("test/subStream");
        int port = FreePort();
        var config = new BridgeOnvifConfig
        {
            Port = port, Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1",
            Mac = "02:54:45:53:54:01", Name = "Synthetic test camera", Model = "Test bridge",
            Uuid = "dfc2d5fb-35e0-4595-96b8-d25bc16d19f1",
            Discovery = false,
            Profiles =
            [
                new BridgeOnvifProfile { Stream = "mainStream", Codec = "H265", Width = 4512, Height = 2512, Fps = 20, Bitrate = 8192 },
                new BridgeOnvifProfile { Stream = "subStream", Codec = "H264", Width = 896, Height = 512, Fps = 15, Bitrate = 1024 }
            ]
        };
        var users = new Dictionary<string, string> { [User] = Password };
        var streams = new[]
        {
            new ProtectOnvifStream("main", "/test/mainStream", main),
            new ProtectOnvifStream("sub", "/test/subStream", sub)
        };
        var server = new ProtectOnvifServer(config, users, streams, 18554);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        Task serving = server.RunAsync(cancellation.Token);
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}"), Timeout = TimeSpan.FromSeconds(3) };
        int failures = 0, passed = 0;
        async Task Test(string name, Func<Task> run)
        {
            try { await run(); passed++; Console.WriteLine($"PASS {name}"); }
            catch (Exception ex) { failures++; Console.Error.WriteLine($"FAIL {name}: {ex.Message}"); }
        }
        async Task<Reply> Send(string body, string ns = Device, string? security = null, string? authorization = null)
        {
            var envelope = $"<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Header>{security}</s:Header><s:Body>{body}</s:Body></s:Envelope>";
            using var message = new HttpRequestMessage(HttpMethod.Post, ns == Device ? "/onvif/device_service" : ns == Media2 ? "/onvif/media2_service" : "/onvif/media_service");
            message.Content = new StringContent(envelope, Encoding.UTF8, "application/soap+xml");
            if (authorization != null) message.Headers.TryAddWithoutValidation("Authorization", authorization);
            using var response = await http.SendAsync(message);
            return new Reply((int)response.StatusCode, await response.Content.ReadAsStringAsync());
        }
        Task<Reply> Call(string operation, string ns = Device, string extra = "", bool authenticate = true) =>
            Send($"<m:{operation} xmlns:m=\"{ns}\">{extra}</m:{operation}>", ns, authenticate ? Token() : null);
        try
        {
            await WaitForServer(http, serving);
            string example = FindExample();
            await Test("shipped pilot config loads strictly without enabling recording or camera controls", () =>
            {
                var pilot = NeolinkConfig.Load(example, strict: true);
                Assert(pilot.Onvif != null && pilot.Cameras.Count == 1, "pilot must expose one virtual camera");
                Assert(pilot.Cameras[0].Stream == "mainStream" && pilot.Cameras[0].AlwaysOn == true && !pilot.Cameras[0].Udp && !pilot.Cameras[0].UdpProbe, "pilot must use one continuous TCP Baichuan stream");
                Assert(pilot.Recording == null && pilot.Mqtt == null && pilot.WakeHints == null && pilot.WebPort == 0 && pilot.PtzPort == 0, "pilot enables an unrelated service");
                return Task.CompletedTask;
            });
            async Task RejectConfig(Action<JsonObject> mutate)
            {
                var input = JsonNode.Parse(await File.ReadAllTextAsync(example))!.AsObject();
                mutate(input);
                string path = Path.Combine(Path.GetTempPath(), "neolink-config-test-" + Guid.NewGuid().ToString("N") + ".json");
                await File.WriteAllTextAsync(path, input.ToJsonString());
                try
                {
                    bool rejected = false;
                    try { NeolinkConfig.Load(path, strict: true); }
                    catch (FormatException) { rejected = true; }
                    Assert(rejected, "unsafe bridge configuration was accepted");
                }
                finally { File.Delete(path); }
            }
            await Test("bridge config rejects two cameras in one virtual identity", () => RejectConfig(input =>
            {
                var second = input["cameras"]![0]!.DeepClone();
                second["name"] = "second-camera";
                input["cameras"]!.AsArray().Add(second);
            }));
            await Test("bridge config rejects extra source sessions and UDP probes", async () =>
            {
                await RejectConfig(input => input["cameras"]![0]!["stream"] = "both");
                await RejectConfig(input => input["cameras"]![0]!["udp"] = true);
                await RejectConfig(input => input["cameras"]![0]!["udp_probe"] = true);
            });
            await Test("bridge config rejects overlapping ports and missing persistent identity", async () =>
            {
                await RejectConfig(input => input["onvif"]!["port"] = input["bind_port"]!.DeepClone());
                await RejectConfig(input => input["onvif"]!["uuid"] = "");
                await RejectConfig(input => input["onvif"]!["mac"] = "ff:ff:ff:ff:ff:ff");
            });
            await Test("bridge config rejects unauthenticated service users", () => RejectConfig(input => input["users"]![0]!["pass"] = ""));
            await Test("health refuses a cold source", async () => Assert((int)(await http.GetAsync("/health")).StatusCode == 503, "cold source must be 503"));
            await Test("metrics rejects anonymous and incorrect Basic requests", async () =>
            {
                using var anonymous = await http.GetAsync("/metrics");
                Assert((int)anonymous.StatusCode == 401, "anonymous metrics must be rejected");
                using var request = new HttpRequestMessage(HttpMethod.Get, "/metrics");
                request.Headers.TryAddWithoutValidation("Authorization", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":incorrect-metrics-password")));
                using var rejected = await http.SendAsync(request);
                Assert((int)rejected.StatusCode == 401, "incorrect metrics login must be rejected");
                string body = await rejected.Content.ReadAsStringAsync();
                Assert(!body.Contains(User) && !body.Contains(Password) && !body.Contains("incorrect-metrics-password"), "metrics rejection reflects credentials");
            });
            await Test("authenticated metrics reports only safe stream diagnostics", async () =>
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, "/metrics");
                request.Headers.TryAddWithoutValidation("Authorization", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password)));
                using var response = await http.SendAsync(request);
                Assert((int)response.StatusCode == 200, "valid Basic login cannot read metrics");
                Assert(response.Content.Headers.ContentType?.MediaType == "application/json", "metrics must be JSON");
                string body = await response.Content.ReadAsStringAsync();
                using var document = JsonDocument.Parse(body);
                var root = document.RootElement;
                Assert(root.GetProperty("status").GetString() == "waiting-for-camera", "cold metrics status incorrect");
                Assert(root.GetProperty("uptimeSeconds").GetDouble() >= 0, "negative endpoint uptime");
                var diagnostics = root.GetProperty("streams").EnumerateArray().ToArray();
                Assert(diagnostics.Length == 2, "metrics lost a profile");
                var fields = new HashSet<string>(["profile", "codec", "width", "height", "incomingFrames", "incomingVideoBytes", "lastVideoAgeMs", "maxArrivalGapMs", "sourceUptimeSeconds", "videoReady", "liveVideo", "authenticationFailed", "viewers",
                    "totalAccessUnits", "multiAccessUnitBuffers", "maxAccessUnitsPerBuffer", "maxVideoBufferBytes", "keyframeCount", "keyframeAgeMs", "gopBytes", "gopPackets", "gopBuffered", "gopCacheEvictions",
                    "lastCameraTimestampDeltaUs", "maxCameraTimestampDeltaUs", "cameraTimestampZeroDeltas", "cameraTimestampBackwardCandidates"]);
                foreach (var item in diagnostics)
                {
                    Assert(fields.SetEquals(item.EnumerateObject().Select(x => x.Name)), "unexpected or missing stream metric fields");
                    Assert(item.GetProperty("lastVideoAgeMs").ValueKind == JsonValueKind.Null, "cold source must not pretend to have arrived recently");
                    Assert(item.GetProperty("incomingFrames").GetInt64() == 0 && item.GetProperty("incomingVideoBytes").GetInt64() == 0, "cold source counters must start at zero");
                }
                Assert(!body.Contains(User) && !body.Contains(Password) && !body.Contains(config.Mac) && !body.Contains(config.Uuid)
                    && !body.Contains(config.AdvertisedHost) && !body.Contains(config.Name) && !body.Contains(main.Name), "metrics exposes credentials or endpoint identities");
            });
            await Test("actual StreamHub counts video at timestamp zero and uses monotonic age and gaps", () =>
            {
                var clock = new ManualTimeProvider();
                var hub = new StreamHub("synthetic-counter-source", clock);
                var empty = hub.GetVideoDiagnostics();
                Assert(empty.IncomingFrames == 0 && empty.IncomingVideoBytes == 0 && empty.LastVideoAgeMs == null, "fresh hub diagnostics incorrect");
                var first = SyntheticKeyframe();
                hub.PublishInfo(new MediaInfo(1920, 1080, 20));
                hub.PublishVideo(first);
                var zero = hub.GetVideoDiagnostics();
                Assert(zero.IncomingFrames == 1 && zero.IncomingVideoBytes == first.Data.Length && zero.LastVideoAgeMs == 0, "first frame at monotonic timestamp zero was lost");
                Assert(zero.MaxArrivalGapMs == 0 && zero.UptimeSeconds == 0, "first frame invents an arrival gap");
                clock.Advance(TimeSpan.FromSeconds(3));
                var predicted = new VideoFrame(VideoCodec.H264, false, 3000000, null, [0, 0, 1, 0x41, 1, 2]);
                hub.PublishVideo(predicted);
                clock.Advance(TimeSpan.FromMilliseconds(250));
                var later = hub.GetVideoDiagnostics();
                Assert(later.IncomingFrames == 2 && later.IncomingVideoBytes == first.Data.Length + predicted.Data.Length, "video counters incorrect");
                Assert(later.MaxArrivalGapMs == 3000 && later.LastVideoAgeMs == 250 && later.UptimeSeconds == 3.25, "age, gap or uptime ignores injected monotonic clock");
                clock.WallClock = DateTimeOffset.UtcNow.AddDays(-10);
                Assert(hub.GetVideoDiagnostics() == later, "wall-clock change affects video arrival diagnostics");
                return Task.CompletedTask;
            });
            await Test("H265 buffer diagnostics distinguish pictures from slices and metadata", VideoBufferDiagnosticsTests.PictureBoundaries);
            await Test("keyframe age and current GOP cache are independent of lifetime counters", VideoBufferDiagnosticsTests.KeyframeAndCache);
            await Test("GOP byte and packet evictions count once without interrupting delivery", VideoBufferDiagnosticsTests.CacheEvictions);
            await Test("camera timestamp diagnostics distinguish wrap zero reversal and reconnect", VideoBufferDiagnosticsTests.TimestampDeltas);
            await Test("protected HTTP metrics expose buffer diagnostics without encoded data or secrets", VideoBufferDiagnosticsTests.HttpMetrics);
            await Test("GOP playout corrects camera wall drift and retains variable cadence", GopVideoPlayoutTests.DriftAndPacing);
            await Test("GOP playout wraps continuously and cancels its reserve promptly", GopVideoPlayoutTests.WrapAndCancel);
            await Test("GOP playout bounds memory frames time and recovers only at complete keyframes", GopVideoPlayoutTests.BoundsAndRecovery);
            await Test("GOP playout splits pictures while preserving parameter slices and SEI", GopVideoPlayoutTests.MultiAuAndMetadata);
            await Test("GOP playout retains valid pictures when source clocks or AU bundles need uniform timing", GopVideoPlayoutTests.SourceClockFallback);
            await Test("GOP config defaults aliases and source arrival metadata are correct", GopPlayoutWireTests.ConfigAndMetadata);
            await Test("actual RTSP aliases normalize AU timestamps markers and FU bytes", GopPlayoutWireTests.WireAliasesAndPictures);
            await Test("default RTSP is unchanged and GOP mounts explicitly refuse audio", GopPlayoutWireTests.DefaultAndAudioScope);
            await Test("actual RTSP PAUSE TEARDOWN and source resets cancel delayed old GOPs", GopPlayoutWireTests.CancelAndEpochRecovery);
            await Test("actual RTSP queue gaps discard partial GOPs and recover at a fresh keyframe", GopPlayoutWireTests.QueueGapRecovery);
            await Test("actual RTSP retains all encoded pictures across a twelve-second raw clock jump", GopPlayoutWireTests.RawClockJumpPictures);
            await Test("stopping and resuming a source preserves lifetime diagnostic counters", () =>
            {
                var clock = new ManualTimeProvider();
                var hub = new StreamHub("synthetic-reconnect-source", clock);
                var frame = SyntheticKeyframe();
                hub.PublishInfo(new MediaInfo(1920, 1080, 20));
                hub.PublishVideo(frame);
                clock.Advance(TimeSpan.FromSeconds(3));
                hub.PublishVideo(frame);
                var before = hub.GetVideoDiagnostics();
                hub.SourceStopped();
                Assert(hub.GetVideoDiagnostics() == before && !hub.LiveVideo, "source stop discarded lifetime diagnostics or retained live state");
                clock.Advance(TimeSpan.FromSeconds(2));
                hub.PublishVideo(frame);
                var resumed = hub.GetVideoDiagnostics();
                Assert(resumed.IncomingFrames == 3 && resumed.IncomingVideoBytes == frame.Data.Length * 3L && resumed.MaxArrivalGapMs == 3000 && resumed.LastVideoAgeMs == 0, "reconnect reset counters, gap or last arrival");
                return Task.CompletedTask;
            });
            await Test("HTTP health rejects stale actual video after five seconds without waiting", async () =>
            {
                var clock = new ManualTimeProvider();
                var hub = new StreamHub("synthetic-freshness-source", clock);
                var freshConfig = new BridgeOnvifConfig
                {
                    Port = FreePort(), Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Mac = "02:54:45:53:54:02",
                    Uuid = "1d2ab3c4-5150-4214-937c-a91c4d0b6c8f", Name = "Synthetic freshness test", Model = "Test bridge", Discovery = false,
                    Profiles = [new BridgeOnvifProfile { Stream = "mainStream", Codec = "H264", Width = 1920, Height = 1080, Fps = 20, Bitrate = 4096 }]
                };
                var freshServer = new ProtectOnvifServer(freshConfig, users, [new ProtectOnvifStream("main", "/fresh/mainStream", hub)], 18554);
                using var freshStop = new CancellationTokenSource();
                Task freshServing = freshServer.RunAsync(freshStop.Token);
                using var freshHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{freshConfig.Port}"), Timeout = TimeSpan.FromSeconds(3) };
                try
                {
                    await WaitForServer(freshHttp, freshServing);
                    hub.PublishInfo(new MediaInfo(1920, 1080, 20));
                    hub.PublishVideo(SyntheticKeyframe());
                    Assert(hub.VideoReady && hub.LiveVideo, "synthetic source failed to establish readiness");
                    Assert((int)(await freshHttp.GetAsync("/health")).StatusCode == 200, "recent actual video must be healthy");
                    clock.Advance(TimeSpan.FromSeconds(5));
                    Assert((int)(await freshHttp.GetAsync("/health")).StatusCode == 200, "five-second boundary must remain healthy");
                    clock.Advance(TimeSpan.FromMilliseconds(1));
                    Assert(hub.VideoReady && hub.LiveVideo, "test must preserve cached readiness while arrival stops");
                    Assert((int)(await freshHttp.GetAsync("/health")).StatusCode == 503, "stale video must fail health despite cached codec and live flags");
                    hub.PublishVideo(SyntheticKeyframe());
                    Assert((int)(await freshHttp.GetAsync("/health")).StatusCode == 200, "new video must restore health");
                }
                finally
                {
                    freshStop.Cancel();
                    try { await freshServing.WaitAsync(TimeSpan.FromSeconds(5)); } catch (OperationCanceledException) { }
                }
            });
            await Test("Protect clock probe works before authentication", async () =>
            {
                var reply = await Call("GetSystemDateAndTime", authenticate: false);
                var xml = Success(reply, "GetSystemDateAndTimeResponse", Device);
                var utc = Node(xml, "UTCDateTime");
                int Part(string name) => int.Parse(Node(utc, name).Value, CultureInfo.InvariantCulture);
                var clock = new DateTime(Part("Year"), Part("Month"), Part("Day"), Part("Hour"), Part("Minute"), Part("Second"), DateTimeKind.Utc);
                Assert(Math.Abs((clock - DateTime.UtcNow).TotalSeconds) < 5, "UTC clock must be current");
            });
            await Test("media requires authentication", async () => Unauthorized(await Call("GetProfiles", Media2, authenticate: false)));
            await Test("incorrect WSSE digest is rejected without secret reflection", async () => Unauthorized(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, Token(password: "incorrect-synthetic-secret"))));
            await Test("stale and future signed clocks are rejected", async () =>
            {
                Unauthorized(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, Token(created: DateTime.UtcNow.AddMinutes(-10))));
                Unauthorized(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, Token(created: DateTime.UtcNow.AddMinutes(10))));
            });
            await Test("WSSE nonce replay is rejected", async () =>
            {
                string token = Token();
                Success(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, token), "GetProfilesResponse", Media2);
                Unauthorized(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, token));
            });
            await Test("HTTP Basic remains compatible with configured RTSP users", async () =>
            {
                string basic = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password));
                Success(await Send($"<m:GetProfiles xmlns:m=\"{Media2}\"/>", Media2, authorization: basic), "GetProfilesResponse", Media2);
            });
            await Test("snapshot Media1 and Media2 expose stable authenticated credential-free JPEG URIs", SnapshotContractTests.SoapUriContracts);
            await Test("snapshot capability is absent without a provider", SnapshotContractTests.CapabilityWithoutProvider);
            await Test("snapshot HTTP authentication and profile checks precede provider work", SnapshotContractTests.AuthBeforeWork);
            await Test("snapshot HTTP preserves binary JPEG and enforces body bounds", SnapshotContractTests.BinaryResponseAndBounds);
            await Test("snapshot HTTP rejects stale or replaced source sessions", SnapshotContractTests.SourceFreshnessAndEpoch);
            await Test("snapshot provider failures and timeouts become bounded generic 503 replies", SnapshotContractTests.ProviderFailureAndBudget);
            await Test("native snapshot cache expires monotonically and rejects stale or replaced sessions", NativeProtectSnapshotTests.CacheAndFreshness);
            await Test("native snapshot single-flight isolates one HTTP caller's cancellation", NativeProtectSnapshotTests.SharedCaptureCancellation);
            await Test("native snapshot validates JPEG structure and invalidates capture across epochs", NativeProtectSnapshotTests.InvalidAndChangedSession);
            await Test("native snapshot capture has a total deadline and stops on host shutdown", NativeProtectSnapshotTests.TotalBudgetAndLifetime);
            await Test("actual Baichuan snapshot reassembles JPEG and caps announced and received bytes", NativeProtectSnapshotTests.WireReassemblyAndLimits);
            await Test("actual Baichuan cancellation rejects late old snapshot fragments", NativeProtectSnapshotTests.WireCancelledAndLateReply);
            await Test("actual FullAES snapshot uses the existing login and preserves JPEG chunks", NativeProtectSnapshotTests.WireFullAesSnapshot);
            await Test("Protect identity has an enabled interface and valid stable MAC", async () =>
            {
                var xml = Success(await Call("GetNetworkInterfaces"), "GetNetworkInterfacesResponse", Device);
                var net = Node(xml, "NetworkInterfaces");
                Assert(Node(net, "Enabled").Value == "true", "interface must be enabled");
                Assert(Node(net, "HwAddress").Value.Equals(config.Mac, StringComparison.OrdinalIgnoreCase), "configured MAC must be preserved");
            });
            await Test("persistent UUID and serial survive a fresh server instance", async () =>
            {
                var first = Success(await Call("GetDeviceInformation"), "GetDeviceInformationResponse", Device);
                var restarted = new ProtectOnvifServer(config, users, streams, 18554);
                string soap = $"<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Header>{Token()}</s:Header><s:Body><m:GetDeviceInformation xmlns:m=\"{Device}\"/></s:Body></s:Envelope>";
                var result = await restarted.HandleAsync(soap, null, cancellation.Token);
                var second = Success(new Reply(result.Status, result.Body), "GetDeviceInformationResponse", Device);
                Assert(Node(first, "SerialNumber").Value == Node(second, "SerialNumber").Value, "serial changed after restart");
                Assert(Node(first, "SerialNumber").Value == Guid.Parse(config.Uuid).ToString("N"), "serial does not represent persistent UUID");
                Assert(config.EndpointReference == "urn:uuid:" + config.Uuid, "invalid WS-Addressing identity");
                var endpoint = Success(await Call("GetEndpointReference"), "GetEndpointReferenceResponse", Device);
                Assert(Node(endpoint, "GUID").Value == config.EndpointReference, "published UUID differs from persistent identity");
            });
            await Test("service advertisement exposes Media1 and Media2 without phantom PTZ or events", async () =>
            {
                var xml = Success(await Call("GetServices", extra: "<m:IncludeCapability>false</m:IncludeCapability>"), "GetServicesResponse", Device);
                var services = xml.Descendants().Where(x => x.Name.LocalName == "Service").ToArray();
                Assert(services.Any(x => Node(x, "Namespace").Value == Media1), "Media1 service missing");
                Assert(services.Any(x => Node(x, "Namespace").Value == Media2), "Media2 service missing");
                foreach (var service in services)
                {
                    string advertised = Node(service, "Namespace").Value;
                    Assert(!advertised.Contains("events") && !advertised.Contains("ptz"), "unsupported feature advertised");
                    Assert(new Uri(Node(service, "XAddr").Value).Host == "127.0.0.1", "incorrect advertised address");
                }
            });
            await Test("Media1 has two distinct correctly described profiles", async () => CheckProfiles(Success(await Call("GetProfiles", Media1), "GetProfilesResponse", Media1), 4512, 2512));
            await Test("Media2 has H265 main and H264 sub with valid configurations", async () => CheckProfiles(Success(await Call("GetProfiles", Media2), "GetProfilesResponse", Media2), 4512, 2512));
            await Test("both media versions return exact credential-free main and sub RTSP URIs", async () =>
            {
                foreach (string ns in new[] { Media1, Media2 })
                    foreach (string token in new[] { "main", "sub" })
                    {
                        string setup = ns == Media1 ? "<m:StreamSetup><tt:Stream xmlns:tt=\"http://www.onvif.org/ver10/schema\">RTP-Unicast</tt:Stream><tt:Transport xmlns:tt=\"http://www.onvif.org/ver10/schema\"><tt:Protocol>RTSP</tt:Protocol></tt:Transport></m:StreamSetup>" : "<m:Protocol>RTSP</m:Protocol>";
                        var xml = Success(await Call("GetStreamUri", ns, setup + $"<m:ProfileToken>{token}</m:ProfileToken>"), "GetStreamUriResponse", ns);
                        Assert(Node(xml, "Uri").Value == $"rtsp://127.0.0.1:18554/test/{(token == "main" ? "mainStream" : "subStream")}", "incorrect RTSP URI");
                    }
            });
            await Test("unknown profile produces a SOAP fault", async () => Fault(await Call("GetStreamUri", Media2, "<m:Protocol>RTSP</m:Protocol><m:ProfileToken>unknown-profile</m:ProfileToken>")));
            await Test("healthy streams publish actual resolution rather than fallback", async () =>
            {
                main.Set(VideoCodec.H265, 3840, 2160, true);
                sub.Set(VideoCodec.H264, 896, 512, true);
                Assert((int)(await http.GetAsync("/health")).StatusCode == 200, "ready sources must be healthy");
                CheckProfiles(Success(await Call("GetProfiles", Media2), "GetProfilesResponse", Media2), 3840, 2160);
            });
            await Test("codec mismatch cannot silently advertise the wrong decoder", async () =>
            {
                main.Set(VideoCodec.H264, 3840, 2160, true);
                Fault(await Call("GetProfiles", Media2));
                main.Set(VideoCodec.H265, 3840, 2160, true);
            });
            await Test("known codec without a live source fails health", async () =>
            {
                main.Set(VideoCodec.H265, 3840, 2160, false);
                Assert((int)(await http.GetAsync("/health")).StatusCode == 503, "stopped source must be unhealthy even with cached codec");
            });
            await Test("malformed XML yields a bounded fault and server remains usable", async () =>
            {
                using var response = await http.PostAsync("/onvif/device_service", new StringContent("<invalid", Encoding.UTF8, "application/soap+xml"));
                Fault(new Reply((int)response.StatusCode, await response.Content.ReadAsStringAsync()));
                Success(await Call("GetNetworkInterfaces"), "GetNetworkInterfacesResponse", Device);
            });
            await Test("external XML entities cannot read a local file", async () =>
            {
                string path = Path.Combine(Path.GetTempPath(), "neolink-test-" + Guid.NewGuid().ToString("N") + ".txt");
                const string marker = "SYNTHETIC-XXE-FILE-CONTENT";
                await File.WriteAllTextAsync(path, marker);
                try
                {
                    string malicious = $"<!DOCTYPE x [<!ENTITY e SYSTEM \"{new Uri(path).AbsoluteUri}\">]><s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Body><GetDeviceInformation xmlns=\"{Device}\">&e;</GetDeviceInformation></s:Body></s:Envelope>";
                    using var request = new HttpRequestMessage(HttpMethod.Post, "/onvif/device_service");
                    request.Content = new StringContent(malicious, Encoding.UTF8, "application/soap+xml");
                    request.Headers.TryAddWithoutValidation("Authorization", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password)));
                    using var response = await http.SendAsync(request);
                    var reply = new Reply((int)response.StatusCode, await response.Content.ReadAsStringAsync());
                    Assert(!reply.Body.Contains(marker), "external entity content leaked");
                    Fault(reply);
                }
                finally { File.Delete(path); }
            });
            await Test("unsupported mutation produces a fault", async () => Fault(await Call("SetSystemFactoryDefault", extra: "<m:FactoryDefault>Hard</m:FactoryDefault>")));
            await Test("nonempty Baichuan phase-two 401 is an authentication failure", AuthParkingTests.Explicit401IsAuthenticationFailure);
            await Test("existing empty Baichuan refusal remains an authentication failure", AuthParkingTests.ExistingEmptyReplyRemainsAuthenticationFailure);
            await Test("Baichuan phase-two XML 500 remains a protocol failure", AuthParkingTests.OtherXmlFailureRemainsProtocolFailure);
            await Test("RelayOnly transport failure still reconnects", AuthParkingTests.TransportFailureStillReconnects);
            await Test("RelayOnly authentication failures park for more than 30 seconds while listeners remain alive", AuthParkingTests.AuthenticationFailuresParkWithoutRetry);
        }
        finally
        {
            cancellation.Cancel();
            try { await serving.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) { }
        }
        Console.WriteLine($"{passed} contract tests passed; {failures} failed. No cameras or network devices contacted.");
        return failures == 0 ? 0 : 1;
    }

    private static string Token(string password = Password, DateTime? created = null)
    {
        byte[] nonce = RandomNumberGenerator.GetBytes(20);
        string date = (created ?? DateTime.UtcNow).ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);
        byte[] suffix = Encoding.UTF8.GetBytes(date + password);
        byte[] material = new byte[nonce.Length + suffix.Length];
        nonce.CopyTo(material, 0); suffix.CopyTo(material, nonce.Length);
        string digest = Convert.ToBase64String(SHA1.HashData(material));
        return $"<wsse:Security xmlns:wsse=\"{Wsse}\" xmlns:wsu=\"{Wsu}\"><wsse:UsernameToken><wsse:Username>{User}</wsse:Username><wsse:Password Type=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest\">{digest}</wsse:Password><wsse:Nonce EncodingType=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary\">{Convert.ToBase64String(nonce)}</wsse:Nonce><wsu:Created>{date}</wsu:Created></wsse:UsernameToken></wsse:Security>";
    }

    private static XElement Success(Reply reply, string response, string ns)
    {
        Assert(reply.Status == 200, $"expected HTTP 200, got {reply.Status}");
        AssertNoCredentialReflection(reply);
        var xml = Parse(reply.Body);
        return xml.Descendants(XName.Get(response, ns)).SingleOrDefault() ?? throw new Exception($"missing {response} in correct namespace");
    }
    private static void Unauthorized(Reply reply)
    {
        Assert(reply.Status is 400 or 401 or 403, $"invalid login status {reply.Status}");
        AssertNoCredentialReflection(reply);
        Assert(reply.Body.Contains("NotAuthorized", StringComparison.OrdinalIgnoreCase), "missing ONVIF authorization fault");
    }
    private static void Fault(Reply reply)
    {
        Assert(reply.Status is >= 400 and < 600, $"fault expected, got HTTP {reply.Status}");
        AssertNoCredentialReflection(reply);
        Assert(Parse(reply.Body).Descendants().Any(x => x.Name.LocalName == "Fault"), "missing SOAP fault");
    }
    private static void CheckProfiles(XElement response, int width, int height)
    {
        var profiles = response.Descendants().Where(x => x.Name.LocalName == "Profiles").ToArray();
        Assert(profiles.Length == 2, "expected two separate quality profiles");
        var main = profiles.Single(x => (string?)x.Attribute("token") == "main");
        var sub = profiles.Single(x => (string?)x.Attribute("token") == "sub");
        Assert(Node(main, "Encoding").Value == "H265", "main stream must honestly advertise H265");
        Assert(Node(sub, "Encoding").Value == "H264", "sub stream must advertise H264");
        Assert(Node(main, "Width").Value == width.ToString(CultureInfo.InvariantCulture) && Node(main, "Height").Value == height.ToString(CultureInfo.InvariantCulture), "incorrect main resolution");
        Assert(Node(sub, "Width").Value == "896" && Node(sub, "Height").Value == "512", "incorrect sub resolution");
        Assert(Node(main, "FrameRateLimit").Value == "20", "incorrect frame rate");
    }
    private static XElement Node(XElement root, string name) => root.Descendants().FirstOrDefault(x => x.Name.LocalName == name) ?? throw new Exception($"missing {name}");
    private static void AssertNoCredentialReflection(Reply reply)
    {
        string decoded = Parse(reply.Body).Root?.Value ?? "";
        Assert(!decoded.Contains(Password) && !decoded.Contains("incorrect-synthetic-secret"), "credential reflected in SOAP response");
        Assert(!reply.Body.Contains("UsernameToken", StringComparison.Ordinal), "security header reflected in SOAP response");
    }
    private static XDocument Parse(string xml)
    {
        using var reader = XmlReader.Create(new StringReader(xml), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null });
        return XDocument.Load(reader);
    }
    private static void Assert(bool condition, string message) { if (!condition) throw new Exception(message); }
    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
    private static VideoFrame SyntheticKeyframe() => new(VideoCodec.H264, true, 0, null,
        [0, 0, 0, 1, 0x67, 66, 0, 30, 0, 0, 1, 0x68, 0xCE, 6, 0xE2, 0, 0, 1, 0x65, 1, 2, 3]);
    private static string FindExample()
    {
        for (DirectoryInfo? directory = new DirectoryInfo(AppContext.BaseDirectory); directory != null; directory = directory.Parent)
        {
            string path = Path.Combine(directory.FullName, "examples", "config.pilot.json");
            if (File.Exists(path)) return path;
        }
        throw new FileNotFoundException("Repository pilot example not found above test executable");
    }
    private static async Task WaitForServer(HttpClient client, Task serving)
    {
        for (int attempt = 0; attempt < 40; attempt++)
        {
            if (serving.IsFaulted) await serving;
            try { using var response = await client.GetAsync("/health"); return; }
            catch (HttpRequestException) { await Task.Delay(50); }
        }
        throw new Exception("ONVIF HTTP listener did not start");
    }
    private sealed record Reply(int Status, string Body);
}

internal sealed class TestHub(string name) : IStreamHub
{
    public string Name { get; } = name;
    public int SubscriberCount => 0;
    public int ViewerCount => 0;
    public bool VideoReady { get; private set; }
    public bool LiveVideo { get; private set; }
    public VideoCodec? Codec { get; private set; }
    public byte[]? Sps => null;
    public byte[]? Pps => null;
    public byte[]? Vps => null;
    public uint Width { get; private set; }
    public uint Height { get; private set; }
    public AudioTrackInfo? Audio => null;
    public DateTime LastViewerAskUtc => DateTime.MinValue;
    public void Set(VideoCodec codec, uint width, uint height, bool live)
    {
        Codec = codec; Width = width; Height = height; VideoReady = true; LiveVideo = live;
    }
    public (Guid id, ChannelReader<HubPacket> reader) Subscribe(bool viewer = false) => throw new InvalidOperationException("SOAP must not subscribe to camera media");
    public void Unsubscribe(Guid id) => throw new InvalidOperationException("SOAP must not unsubscribe camera media");
    public Task<bool> WaitForDescribeInfoAsync(TimeSpan timeout, CancellationToken ct) => Task.FromResult(VideoReady);
    public VideoDiagnostics GetVideoDiagnostics() => new(LiveVideo ? 1 : 0, LiveVideo ? 8 : 0, LiveVideo ? 0 : null, 0, 0);
}

internal sealed class ManualTimeProvider : TimeProvider
{
    private long _timestamp;
    public override long TimestampFrequency => 1000000;
    public override long GetTimestamp() => Interlocked.Read(ref _timestamp);
    public DateTimeOffset WallClock { get; set; } = DateTimeOffset.UnixEpoch;
    public override DateTimeOffset GetUtcNow() => WallClock;
    public void Advance(TimeSpan duration) => Interlocked.Add(ref _timestamp, (long)(duration.TotalSeconds * TimestampFrequency));
    public void AdvanceTimestampTicks(long ticks) => Interlocked.Add(ref _timestamp, ticks);
}
