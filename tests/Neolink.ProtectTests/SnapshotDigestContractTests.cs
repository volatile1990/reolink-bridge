using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Streaming;

internal static class SnapshotDigestContractTests
{
    private const string User = "digest-test";
    private const string Password = "synthetic-digest-only<&secret";
    private const string Target = "/snapshot/main.jpg";
    private static readonly IReadOnlyDictionary<string, string> Users = new Dictionary<string, string> { [User] = Password };
    private static string Basic => "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password));
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
    private static string Hash(string value) => Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static string Quoted(string value) => "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    private static string Digest(string nonce, string nc = "00000001", string cnonce = "synthetic-client",
        string uri = Target, string method = "GET", string realm = SnapshotDigestAuthentication.Realm,
        string password = Password, string user = User)
    {
        string response = Hash($"{Hash($"{user}:{realm}:{password}")}:{nonce}:{nc}:{cnonce}:auth:{Hash($"{method}:{uri}")}");
        return $"Digest username={Quoted(user)}, realm={Quoted(realm)}, nonce={Quoted(nonce)}, uri={Quoted(uri)}, " +
            $"algorithm=MD5, qop=auth, nc={nc}, cnonce={Quoted(cnonce)}, response={Quoted(response)}";
    }
    private static string Nonce(string challenge)
    {
        string marker = "nonce=\"";
        int start = challenge.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
        Check(start >= marker.Length, "Digest challenge lost its nonce");
        int end = challenge.IndexOf('"', start);
        Check(end > start, "Digest challenge nonce is not closed");
        return challenge[start..end];
    }

    public static async Task ChallengeAndAuthentication()
    {
        await using var fixture = await Fixture.Start();
        string first = await fixture.Challenge();
        string second = await fixture.Challenge();
        Check(first.StartsWith("Digest realm=\"reolink-bridge\"", StringComparison.Ordinal)
            && first.Contains("algorithm=MD5") && first.Contains("qop=\"auth\"") && first.Contains("charset=UTF-8"),
            "snapshot challenge omitted the supported Digest contract");
        string nonce = Nonce(first);
        Check(nonce.Length == 64 && nonce.All(Uri.IsHexDigit) && nonce != Nonce(second), "Digest nonces are not random bounded values");
        using (var accepted = await fixture.Get(Target, Digest(nonce)))
            Check(accepted.StatusCode == HttpStatusCode.OK && (await accepted.Content.ReadAsByteArrayAsync()).SequenceEqual(Fixture.Jpeg),
                "Digest snapshot GET did not preserve binary JPEG");
        using (var basic = await fixture.Get(Target, Basic))
            Check(basic.StatusCode == HttpStatusCode.OK, "Digest challenge broke preemptive Basic");

        string escaped = "/snapshot/" + Uri.EscapeDataString("sub & preview") + ".jpg";
        using (var accepted = await fixture.Get(escaped, Digest(Nonce(second), uri: escaped, cnonce: "quoted\\client\"value")))
            Check(accepted.StatusCode == HttpStatusCode.OK, "quoted escapes or exact escaped request-target failed");

        // Independent HTTP client implementation must be able to answer the challenge.
        using var handler = new HttpClientHandler { Credentials = new NetworkCredential(User, Password), AllowAutoRedirect = false };
        using var client = new HttpClient(handler) { BaseAddress = fixture.Http.BaseAddress, Timeout = TimeSpan.FromSeconds(4) };
        using (var automatic = await client.GetAsync(Target))
            Check(automatic.StatusCode == HttpStatusCode.OK, "standard HTTP Digest negotiation did not fetch a snapshot");
        Check(fixture.Provider.Calls == 4, "anonymous negotiation performed provider work");
    }

    public static async Task ReplayAndBindings()
    {
        await using var fixture = await Fixture.Start();
        string nonce = Nonce(await fixture.Challenge());
        string first = Digest(nonce);
        using (var accepted = await fixture.Get(Target, first)) Check(accepted.StatusCode == HttpStatusCode.OK, "initial Digest failed");
        using (var replay = await fixture.Get(Target, first)) Check(replay.StatusCode == HttpStatusCode.Unauthorized, "captured Digest was replayable");
        using (var wrong = await fixture.Get(Target, Digest(nonce, nc: "00000002", password: "incorrect")))
            Check(wrong.StatusCode == HttpStatusCode.Unauthorized, "wrong password was accepted");
        using (var second = await fixture.Get(Target, Digest(nonce, nc: "00000002")))
            Check(second.StatusCode == HttpStatusCode.OK, "invalid response consumed a valid future nonce-count");
        foreach (string invalid in new[]
        {
            Digest(nonce, nc: "00000003", uri: Target + "?cache=1"),
            Digest(nonce, nc: "00000003", method: "POST"),
            Digest(nonce, nc: "00000003", realm: "other-realm"),
            Digest(nonce, nc: "00000003", user: "unknown-user"),
            Digest(nonce, nc: "00000003").Replace("cnonce=\"synthetic-client\"", "cnonce=\"different-client\"", StringComparison.Ordinal)
        })
        {
            using var rejected = await fixture.Get(Target, invalid);
            Check(rejected.StatusCode == HttpStatusCode.Unauthorized, "Digest failed to bind URI method realm user or cnonce");
        }
        string third = Digest(nonce, nc: "00000003");
        var concurrent = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => fixture.Get(Target, third)));
        try
        {
            Check(concurrent.Count(response => response.StatusCode == HttpStatusCode.OK) == 1
                && concurrent.Count(response => response.StatusCode == HttpStatusCode.Unauthorized) == 3,
                "concurrent captured requests bypassed atomic replay protection");
        }
        finally { foreach (var response in concurrent) response.Dispose(); }
        using (var knownButMissing = await fixture.Get("/snapshot/unknown.jpg", Digest(nonce, nc: "00000004", uri: "/snapshot/unknown.jpg")))
            Check(knownButMissing.StatusCode == HttpStatusCode.NotFound, "authenticated unknown profile needs 404");
        Check(fixture.Provider.Calls == 3, "rejected Digest performed snapshot work");
    }

    public static async Task ParserAndScope()
    {
        await using var fixture = await Fixture.Start();
        string nonce = Nonce(await fixture.Challenge());
        string valid = Digest(nonce);
        foreach (string invalid in new[]
        {
            valid + ", USERNAME=\"digest-test\"", valid + ", nc=00000002", valid + ", unknown=value",
            valid + ",", valid + " trailing-junk", valid.Replace("algorithm=MD5", "algorithm=MD5-sess", StringComparison.Ordinal),
            valid.Replace("qop=auth", "qop=auth-int", StringComparison.Ordinal),
            valid.Replace("nc=00000001", "nc=00000000", StringComparison.Ordinal),
            valid.Replace("nc=00000001", "nc=1", StringComparison.Ordinal),
            valid.Replace("nc=00000001", "nc=0000000g", StringComparison.Ordinal),
            valid.Replace("cnonce=\"synthetic-client\"", "cnonce=\"\"", StringComparison.Ordinal),
            valid.Replace("cnonce=\"synthetic-client\"", "cnonce=\"" + new string('c', 129) + "\"", StringComparison.Ordinal),
            valid.Replace("response=\"", "response=\"g", StringComparison.Ordinal),
            valid[..^1], valid + ", algorithm=\"MD5\\\"", "Digest " + new string('a', 2048)
        })
        {
            using var rejected = await fixture.Get(Target, invalid);
            Check(rejected.StatusCode == HttpStatusCode.Unauthorized, "malformed or oversized Digest passed validation");
            string body = await rejected.Content.ReadAsStringAsync();
            Check(!body.Contains(User) && !body.Contains(Password) && !body.Contains(nonce), "Digest rejection reflected sensitive input");
        }
        using (var stillValid = await fixture.Get(Target, valid)) Check(stillValid.StatusCode == HttpStatusCode.OK, "bad parser input consumed nonce-count");
        using (var metrics = await fixture.Get("/metrics", Digest(nonce, nc: "00000002", uri: "/metrics")))
            Check(metrics.StatusCode == HttpStatusCode.Unauthorized && metrics.Headers.WwwAuthenticate.Single().Scheme == "Basic",
                "snapshot-only Digest changed metrics authentication");
        string soap = $"<s:Envelope xmlns:s=\"{ProtectOnvifServer.NsSoap}\"><s:Body><m:GetProfiles xmlns:m=\"{ProtectOnvifServer.NsMedia}\"/></s:Body></s:Envelope>";
        using (var request = new HttpRequestMessage(HttpMethod.Post, "/onvif/media_service"))
        {
            request.Content = new StringContent(soap, Encoding.UTF8, "application/soap+xml");
            request.Headers.TryAddWithoutValidation("Authorization", Digest(nonce, method: "POST", uri: "/onvif/media_service"));
            using var rejected = await fixture.Http.SendAsync(request);
            Check(rejected.StatusCode == HttpStatusCode.Unauthorized, "snapshot Digest changed SOAP authentication");
        }
        Check(fixture.Provider.Calls == 1, "parser or out-of-scope Digest invoked provider");
    }

    public static async Task MonotonicExpiryAndBounds()
    {
        var clock = new ManualTimeProvider();
        await using var fixture = await Fixture.Start(clock);
        string nonce = Nonce(await fixture.Challenge());
        clock.WallClock = DateTimeOffset.UtcNow.AddYears(20);
        clock.Advance(TimeSpan.FromSeconds(59));
        using (var accepted = await fixture.Get(Target, Digest(nonce)))
            Check(accepted.StatusCode == HttpStatusCode.OK, "wall-clock adjustment changed nonce lifetime");
        clock.Advance(TimeSpan.FromSeconds(1));
        using (var expired = await fixture.Get(Target, Digest(nonce, nc: "00000002")))
            Check(expired.StatusCode == HttpStatusCode.Unauthorized, "nonce remained valid at TTL boundary");

        var auth = new SnapshotDigestAuthentication(Users, clock);
        string oldest = Nonce(auth.Challenge());
        string latest = oldest;
        for (int i = 0; i < SnapshotDigestAuthentication.MaxNonces + 4; i++) latest = Nonce(auth.Challenge());
        string recent = latest;
        latest = Nonce(auth.Challenge());
        Check(!auth.Authenticate(Digest(oldest), "GET", Target) && auth.Authenticate(Digest(latest), "GET", Target),
            "challenge flood exceeded bounded nonce storage or removed every fresh nonce");
        Check(auth.Authenticate(Digest(recent), "GET", Target), "nonce slot reuse evicted the latest prior challenge");
        for (int i = 1; i < SnapshotDigestAuthentication.MaxReplayTuples; i++)
            Check(auth.Authenticate(Digest(latest, cnonce: "client-" + i), "GET", Target), "bounded replay tuple refused valid capacity");
        Check(!auth.Authenticate(Digest(latest, cnonce: "over-capacity"), "GET", Target), "cnonce fanout exceeded bounded replay storage");
        Check(auth.Authenticate(Digest(latest, nc: "00000002"), "GET", Target), "full replay table prevented advancing existing client");
        Check(!auth.Authenticate(Digest(latest, nc: "00000003"), "POST", Target), "non-GET method reached snapshot digest acceptance");
        clock.AdvanceTimestampTicks(-1);
        string future = Nonce(auth.Challenge());
        clock.AdvanceTimestampTicks(-1);
        Check(!auth.Authenticate(Digest(future), "GET", Target), "backwards monotonic clock retained future nonce");
        Check(fixture.Provider.Calls == 1, "expired nonce invoked snapshot provider");
    }

    private sealed class Provider : IProtectSnapshotProvider
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);
        public Task<byte[]?> GetJpegAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested(); Interlocked.Increment(ref _calls);
            return Task.FromResult<byte[]?>(Fixture.Jpeg);
        }
    }
    private sealed class Hub : IStreamHub
    {
        public string Name => "synthetic-digest-source";
        public int SubscriberCount => 0;
        public int ViewerCount => 0;
        public bool VideoReady => true;
        public bool LiveVideo => true;
        public long SourceEpoch => 1;
        public VideoCodec? Codec => VideoCodec.H265;
        public byte[]? Sps => null;
        public byte[]? Pps => null;
        public byte[]? Vps => null;
        public uint Width => 4512;
        public uint Height => 2512;
        public AudioTrackInfo? Audio => null;
        public DateTime LastViewerAskUtc => DateTime.MinValue;
        public (Guid id, ChannelReader<HubPacket> reader) Subscribe(bool viewer = false) => throw new Exception("Digest snapshot subscribed to media");
        public void Unsubscribe(Guid id) => throw new Exception("Digest snapshot altered media subscribers");
        public Task<bool> WaitForDescribeInfoAsync(TimeSpan timeout, CancellationToken ct) => Task.FromResult(true);
        public VideoDiagnostics GetVideoDiagnostics() => new(1, 512, 0, 0, 0);
    }
    private sealed class Fixture : IAsyncDisposable
    {
        internal static readonly byte[] Jpeg = [0xFF, 0xD8, 1, 2, 3, 4, 0xFF, 0xD9];
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(20));
        private readonly Task _serving;
        internal Provider Provider { get; } = new();
        internal HttpClient Http { get; }
        private Fixture(TimeProvider? clock)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start(); int port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
            var config = new BridgeOnvifConfig
            {
                Port = port, Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Discovery = false,
                Mac = "02:54:45:53:54:44", Uuid = "d70018c2-5988-4b8b-ad73-a775248c1205",
                Name = "Synthetic Digest camera", Model = "Synthetic Digest bridge",
                Profiles = [new() { Stream = "mainStream", Codec = "H265", Width = 4512, Height = 2512 },
                    new() { Stream = "subStream", Codec = "H265", Width = 4512, Height = 2512 }]
            };
            var hub = new Hub();
            ProtectOnvifStream[] streams = [new("main", "/test/mainStream", hub), new("sub & preview", "/test/subStream", hub)];
            var server = new ProtectOnvifServer(config, Users, streams, port == 18554 ? 18555 : 18554, Provider, clock);
            _serving = server.RunAsync(_stop.Token);
            Http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false })
                { BaseAddress = new Uri($"http://127.0.0.1:{port}/"), Timeout = TimeSpan.FromSeconds(4) };
        }
        internal static async Task<Fixture> Start(TimeProvider? clock = null)
        {
            var fixture = new Fixture(clock);
            try
            {
                for (int i = 0; i < 40; i++)
                {
                    if (fixture._serving.IsFaulted) await fixture._serving;
                    try { using var health = await fixture.Http.GetAsync("/health"); return fixture; }
                    catch (HttpRequestException) { await Task.Delay(25); }
                }
                throw new Exception("Digest test listener did not start");
            }
            catch { await fixture.DisposeAsync(); throw; }
        }
        internal async Task<HttpResponseMessage> Get(string target, string? authorization)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, target);
            if (authorization != null) request.Headers.TryAddWithoutValidation("Authorization", authorization);
            return await Http.SendAsync(request);
        }
        internal async Task<string> Challenge()
        {
            using var response = await Get(Target, null);
            Check(response.StatusCode == HttpStatusCode.Unauthorized && response.Headers.WwwAuthenticate.Single().Scheme == "Digest",
                "anonymous snapshot did not offer Digest before work");
            return response.Headers.WwwAuthenticate.Single().ToString();
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            try { await _serving.WaitAsync(TimeSpan.FromSeconds(5)); } catch (OperationCanceledException) { }
            Http.Dispose(); _stop.Dispose();
        }
    }
}
