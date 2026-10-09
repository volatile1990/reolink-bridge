using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security;
using System.Text;
using System.Threading.Channels;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Streaming;

internal static class SnapshotContractTests
{
    private const string User = "snapshot-test";
    private const string Password = "synthetic-snapshot-only<&secret";
    private const string AlternateProfile = "sub & preview";
    private static readonly string Basic = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password));

    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    // Opaque provider output tests the binary HTTP contract. The native provider's
    // separate tests validate actual JPEG markers/SOF; no image is written to disk.
    private static byte[] JpegBytes(int length = 512)
    {
        var bytes = new byte[length];
        for (int i = 0; i < length; i++) bytes[i] = (byte)i;
        bytes[0] = 0xFF; bytes[1] = 0xD8; bytes[^2] = 0xFF; bytes[^1] = 0xD9;
        return bytes;
    }

    public static async Task SoapUriContracts()
    {
        var provider = new Provider();
        await using var fixture = await Fixture.StartAsync(provider);
        foreach (string ns in new[] { ProtectOnvifServer.NsMedia, ProtectOnvifServer.NsMedia2 })
        {
            var capabilities = await fixture.SoapAsync(ns, "GetServiceCapabilities");
            Check(capabilities.Status == 200 && capabilities.Xml.Descendants()
                .Single(element => element.Name.LocalName == "Capabilities").Attribute("SnapshotUri")?.Value == "true",
                "snapshot provider capability must be advertised in both media versions");
            foreach (string token in new[] { "main", AlternateProfile })
            {
                var reply = await fixture.SoapAsync(ns, "GetSnapshotUri", token);
                Check(reply.Status == 200, "valid snapshot profile did not produce a URI");
                var response = reply.Xml.Descendants(XName.Get("GetSnapshotUriResponse", ns)).Single();
                var uriElement = response.Descendants().Single(element => element.Name.LocalName == "Uri");
                string expected = fixture.Http.BaseAddress + "snapshot/" + Uri.EscapeDataString(token) + ".jpg";
                Check(uriElement.Value == expected && new Uri(uriElement.Value).UserInfo.Length == 0
                    && !reply.Body.Contains(Password), "snapshot URI changed identity or contains credentials");
                if (ns == ProtectOnvifServer.NsMedia)
                {
                    var media = response.Element(XName.Get("MediaUri", ns));
                    Check(media != null && uriElement.Name.NamespaceName == ProtectOnvifServer.NsSchema,
                        "Media1 snapshot URI needs the MediaUri/schema wrapper");
                    Check(media!.Elements().Single(element => element.Name.LocalName == "InvalidAfterConnect").Value == "false"
                        && media.Elements().Single(element => element.Name.LocalName == "InvalidAfterReboot").Value == "false"
                        && media.Elements().Single(element => element.Name.LocalName == "Timeout").Value == "PT0S",
                        "Media1 snapshot URI must remain valid indefinitely");
                }
                else
                    Check(uriElement.Parent == response && uriElement.Name.NamespaceName == ns,
                        "Media2 snapshot URI must be direct, without a MediaUri wrapper");
            }
            var unknown = await fixture.SoapAsync(ns, "GetSnapshotUri", "unknown-profile");
            Check(unknown.Status == 400 && unknown.Body.Contains("ter:NoProfile"), "unknown snapshot profile needs NoProfile");
            var anonymous = await fixture.SoapAsync(ns, "GetSnapshotUri", "main", authenticate: false);
            Check(anonymous.Status == 401 && anonymous.Body.Contains("ter:NotAuthorized"), "snapshot SOAP must require authentication");
        }
        fixture.Main.LiveVideo = false;
        var stopped = await fixture.SoapAsync(ProtectOnvifServer.NsMedia2, "GetSnapshotUri", "main");
        Check(stopped.Status == 200, "stable snapshot URI must remain available while source reconnects");
        Check(provider.Calls == 0, "SOAP URI/capability requests invoked the snapshot provider");
    }

    public static async Task CapabilityWithoutProvider()
    {
        await using var fixture = await Fixture.StartAsync(null);
        foreach (string ns in new[] { ProtectOnvifServer.NsMedia, ProtectOnvifServer.NsMedia2 })
        {
            var reply = await fixture.SoapAsync(ns, "GetServiceCapabilities");
            Check(reply.Status == 200 && reply.Xml.Descendants().Single(element => element.Name.LocalName == "Capabilities")
                .Attribute("SnapshotUri")?.Value == "false", "absent provider advertises phantom snapshots");
            var unsupported = await fixture.SoapAsync(ns, "GetSnapshotUri", "main");
            Check(unsupported.Status == 400 && unsupported.Body.Contains("ter:ActionNotSupported"),
                "absent provider must not publish a nonfunctional snapshot URI");
        }
        using var get = await fixture.GetAsync("/snapshot/main.jpg");
        Check(get.StatusCode == HttpStatusCode.ServiceUnavailable, "GET with no provider must be unavailable");
    }

    public static async Task AuthBeforeWork()
    {
        var provider = new Provider();
        await using var fixture = await Fixture.StartAsync(provider);
        foreach (string? authorization in new[] { null, "Basic invalid", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":incorrect")) })
        {
            using var response = await fixture.GetAsync("/snapshot/main.jpg", authorization);
            Check(response.StatusCode == HttpStatusCode.Unauthorized
                && response.Headers.WwwAuthenticate.Any(challenge => challenge.Scheme == "Digest"),
                "snapshot GET must challenge anonymous or incorrect credentials with Digest");
            Check(!((await response.Content.ReadAsStringAsync()).Contains(Password)), "snapshot auth response reflected credentials");
        }
        using (var response = await fixture.GetAsync("/snapshot/unknown.jpg"))
            Check(response.StatusCode == HttpStatusCode.NotFound, "unknown HTTP snapshot profile must be 404");
        using (var response = await fixture.GetAsync("/snapshot/main.jpg?profile=unknown"))
            Check(response.StatusCode == HttpStatusCode.NotFound, "query parameters must not bypass profile resolution");
        using (var response = await fixture.GetAsync("/snapshot/unknown.jpg", authorization: null))
            Check(response.StatusCode == HttpStatusCode.Unauthorized, "profile resolution preceded snapshot authentication");
        Check(provider.Calls == 0, "rejected HTTP requests invoked snapshot work");
    }

    public static async Task BinaryResponseAndBounds()
    {
        byte[] expected = JpegBytes();
        var provider = new Provider { Handler = _ => Task.FromResult<byte[]?>(expected) };
        await using var fixture = await Fixture.StartAsync(provider);
        using (var response = await fixture.GetAsync("/snapshot/main.jpg"))
        {
            Check(response.StatusCode == HttpStatusCode.OK && response.Content.Headers.ContentType?.MediaType == "image/jpeg",
                "successful snapshot needs image/jpeg");
            Check(response.Content.Headers.ContentLength == expected.Length && response.Headers.CacheControl?.NoStore == true,
                "snapshot length or no-store contract is incorrect");
            Check((await response.Content.ReadAsByteArrayAsync()).SequenceEqual(expected), "binary JPEG was modified by HTTP serialization");
        }
        using (var response = await fixture.GetAsync("/snapshot/" + Uri.EscapeDataString(AlternateProfile) + ".jpg"))
            Check(response.StatusCode == HttpStatusCode.OK, "escaped known profile must resolve exactly");
        foreach (byte[]? rejected in new byte[]?[] { null, [], [1, 2, 3, 4], [0xFF, 0xD8, 1, 2], JpegBytes(4 * 1024 * 1024 + 1) })
        {
            provider.Handler = _ => Task.FromResult(rejected);
            using var response = await fixture.GetAsync("/snapshot/main.jpg");
            Check(response.StatusCode == HttpStatusCode.ServiceUnavailable, "missing, invalid, truncated or oversized JPEG was served");
        }
        expected = JpegBytes(4 * 1024 * 1024);
        provider.Handler = _ => Task.FromResult<byte[]?>(expected);
        using var atLimit = await fixture.GetAsync("/snapshot/main.jpg");
        Check(atLimit.StatusCode == HttpStatusCode.OK && (await atLimit.Content.ReadAsByteArrayAsync()).SequenceEqual(expected),
            "exact bounded JPEG limit was rejected or modified");
    }

    public static async Task SourceFreshnessAndEpoch()
    {
        var provider = new Provider();
        await using var fixture = await Fixture.StartAsync(provider);
        Action[] makeUnavailable =
        [
            () => fixture.Main.LiveVideo = false,
            () => fixture.Main.VideoReady = false,
            () => fixture.Main.AuthenticationFailed = true,
            () => fixture.Main.Codec = VideoCodec.H264,
            () => fixture.Main.AgeMs = null,
            () => fixture.Main.AgeMs = -1,
            () => fixture.Main.AgeMs = 5000.001
        ];
        foreach (var change in makeUnavailable)
        {
            fixture.Main.Reset();
            change();
            using var response = await fixture.GetAsync("/snapshot/main.jpg");
            Check(response.StatusCode == HttpStatusCode.ServiceUnavailable, "unready, mismatched or stale source served a JPEG");
        }
        Check(provider.Calls == 0, "unready source invoked provider/cache work");
        fixture.Main.Reset();
        fixture.Main.AgeMs = 5000;
        using (var response = await fixture.GetAsync("/snapshot/main.jpg"))
            Check(response.StatusCode == HttpStatusCode.OK, "inclusive five-second source freshness boundary changed");
        Action[] changeDuringWork =
        [
            () => fixture.Main.SourceEpoch++,
            () => fixture.Main.LiveVideo = false,
            () => fixture.Main.AgeMs = 5000.001
        ];
        foreach (var change in changeDuringWork)
        {
            fixture.Main.Reset();
            provider.Handler = _ => { change(); return Task.FromResult<byte[]?>(JpegBytes()); };
            using var response = await fixture.GetAsync("/snapshot/main.jpg");
            Check(response.StatusCode == HttpStatusCode.ServiceUnavailable, "snapshot from old or stopped source session was served");
        }
    }

    public static async Task ProviderFailureAndBudget()
    {
        var provider = new Provider { Handler = _ => throw new InvalidOperationException(Password) };
        await using var fixture = await Fixture.StartAsync(provider);
        using (var response = await fixture.GetAsync("/snapshot/main.jpg"))
        {
            Check(response.StatusCode == HttpStatusCode.ServiceUnavailable && !(await response.Content.ReadAsStringAsync()).Contains(Password),
                "provider exception escaped or reflected credentials");
        }
        bool canceled = false;
        provider.Handler = async ct =>
        {
            try { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            catch (OperationCanceledException) { canceled = true; throw; }
            return null;
        };
        var elapsed = Stopwatch.StartNew();
        using var timedOut = await fixture.GetAsync("/snapshot/main.jpg");
        Check(timedOut.StatusCode == HttpStatusCode.ServiceUnavailable && elapsed.Elapsed < TimeSpan.FromSeconds(6),
            "snapshot provider work did not obey bounded server deadline");
        for (int attempt = 0; attempt < 20 && !canceled; attempt++) await Task.Delay(10);
        Check(canceled, "native provider was not canceled when its snapshot deadline expired");
    }

    private sealed class Provider : IProtectSnapshotProvider
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        public Func<CancellationToken, Task<byte[]?>> Handler { get; set; } = _ => Task.FromResult<byte[]?>(JpegBytes());
        public Task<byte[]?> GetJpegAsync(CancellationToken ct)
        {
            Interlocked.Increment(ref _calls);
            return Handler(ct);
        }
    }

    private sealed class SnapshotHub : IStreamHub
    {
        public string Name => "synthetic-snapshot";
        public int SubscriberCount => 0;
        public int ViewerCount => 0;
        public bool VideoReady { get; set; } = true;
        public bool LiveVideo { get; set; } = true;
        public bool AuthenticationFailed { get; set; }
        public long SourceEpoch { get; set; } = 1;
        public VideoCodec? Codec { get; set; } = VideoCodec.H265;
        public double? AgeMs { get; set; } = 0;
        public byte[]? Sps => null;
        public byte[]? Pps => null;
        public byte[]? Vps => null;
        public uint Width => 4512;
        public uint Height => 2512;
        public AudioTrackInfo? Audio => null;
        public DateTime LastViewerAskUtc => DateTime.MinValue;
        public (Guid id, ChannelReader<HubPacket> reader) Subscribe(bool viewer = false) => throw new Exception("snapshot HTTP subscribed to camera media");
        public void Unsubscribe(Guid id) => throw new Exception("snapshot HTTP altered camera media subscriptions");
        public Task<bool> WaitForDescribeInfoAsync(TimeSpan timeout, CancellationToken ct) => Task.FromResult(VideoReady);
        public VideoDiagnostics GetVideoDiagnostics() => new(1, 512, AgeMs, 0, 0);
        public void Reset() { VideoReady = LiveVideo = true; AuthenticationFailed = false; Codec = VideoCodec.H265; AgeMs = 0; }
    }

    private sealed record SoapReply(int Status, string Body)
    {
        public XDocument Xml => XDocument.Parse(Body);
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(20));
        private readonly Task _serving;
        public SnapshotHub Main { get; } = new();
        public HttpClient Http { get; }
        private Fixture(IProtectSnapshotProvider? provider)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            var config = new BridgeOnvifConfig
            {
                Port = port, Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Discovery = false,
                Mac = "02:54:45:53:54:53", Uuid = "97f01d98-bb31-41bd-a98d-796355031014",
                Name = "Synthetic snapshot camera", Model = "Synthetic snapshot bridge",
                Profiles =
                [
                    new() { Stream = "mainStream", Codec = "H265", Width = 4512, Height = 2512 },
                    new() { Stream = "subStream", Codec = "H265", Width = 4512, Height = 2512 }
                ]
            };
            ProtectOnvifStream[] streams = [new("main", "/test/mainStream", Main), new(AlternateProfile, "/test/subStream", Main)];
            var server = new ProtectOnvifServer(config, new Dictionary<string, string> { [User] = Password }, streams,
                port == 18554 ? 18555 : 18554, provider);
            _serving = server.RunAsync(_stop.Token);
            Http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}/"), Timeout = TimeSpan.FromSeconds(6) };
        }
        public static async Task<Fixture> StartAsync(IProtectSnapshotProvider? provider)
        {
            var fixture = new Fixture(provider);
            try
            {
                for (int attempt = 0; attempt < 40; attempt++)
                {
                    if (fixture._serving.IsFaulted) await fixture._serving;
                    try { using var ready = await fixture.Http.GetAsync("/health"); return fixture; }
                    catch (HttpRequestException) { await Task.Delay(25); }
                }
                throw new Exception("snapshot test listener did not start");
            }
            catch { await fixture.DisposeAsync(); throw; }
        }
        public Task<HttpResponseMessage> GetAsync(string path) => GetAsync(path, Basic);
        public async Task<HttpResponseMessage> GetAsync(string path, string? authorization)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, path);
            if (authorization != null) request.Headers.TryAddWithoutValidation("Authorization", authorization);
            return await Http.SendAsync(request);
        }
        public async Task<SoapReply> SoapAsync(string ns, string action, string? token = null, bool authenticate = true)
        {
            string body = $"<m:{action} xmlns:m=\"{ns}\">" + (token == null ? "" : $"<m:ProfileToken>{SecurityElement.Escape(token)}</m:ProfileToken>") + $"</m:{action}>";
            using var request = new HttpRequestMessage(HttpMethod.Post,
                ns == ProtectOnvifServer.NsMedia2 ? "/onvif/media2_service" : "/onvif/media_service");
            request.Content = new StringContent($"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\"><s:Body>{body}</s:Body></s:Envelope>", Encoding.UTF8, "application/soap+xml");
            if (authenticate) request.Headers.TryAddWithoutValidation("Authorization", Basic);
            using var response = await Http.SendAsync(request);
            return new((int)response.StatusCode, await response.Content.ReadAsStringAsync());
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            try { await _serving.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (OperationCanceledException) { }
            Http.Dispose();
            _stop.Dispose();
        }
    }
}
