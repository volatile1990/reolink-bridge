using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class RtspReliabilityTests
{
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(nal => new byte[] { 0, 0, 0, 1 }.Concat(nal)).ToArray();
    private static VideoFrame Frame(uint timestamp = 0) => new(VideoCodec.H265, true, timestamp, null,
        Annex([0x40, 0x01, 0x80], [0x42, 0x01, 0x80], [0x44, 0x01, 0x80], [0x26, 0x01, 0x80, 0x81]));

    public static async Task RequestBudgetsAndDeadlines()
    {
        await using var fixture = new ServerFixture(new RtspResourceLimits
        {
            MaxRequestBodyBytes = 32, MaxHeaderCount = 3, MaxHeaderBytes = 256,
            RequestDeadline = TimeSpan.FromMilliseconds(200), ConnectionIdleTimeout = TimeSpan.FromSeconds(3),
            WatchInterval = TimeSpan.FromMilliseconds(20)
        });
        await using (var normal = await fixture.Connect())
        {
            await normal.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 12\r\n\r\nhello world!");
            Check((await normal.Response()).StartsWith("RTSP/1.0 200", StringComparison.Ordinal), "bounded normal request stopped working");
        }
        await using (var body = await fixture.Connect())
        {
            await body.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 2\r\nContent-Length: 33\r\n\r\n");
            Check((await body.Response()).StartsWith("RTSP/1.0 413", StringComparison.Ordinal), "body budget was not enforced before reading it");
            await body.Closed();
        }
        await using (var count = await fixture.Connect())
        {
            await count.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 3\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n");
            await count.Closed();
        }
        await using (var aggregate = await fixture.Connect())
        {
            await aggregate.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 4\r\nNote: " + new string('a', 210) + "\r\n\r\n");
            await aggregate.Closed();
        }
        await using (var partial = await fixture.Connect())
        {
            await partial.Send("OPTIONS rtsp://127.0.0.1/synthetic");
            await partial.Closed();
        }
        await using var healthy = await fixture.Connect();
        await healthy.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 5\r\n\r\n");
        Check((await healthy.Response()).StartsWith("RTSP/1.0 200", StringComparison.Ordinal), "request cleanup damaged the listener");
    }

    public static async Task ConnectionAndSessionLimits()
    {
        await using (var fixture = new ServerFixture(new RtspResourceLimits
        { MaxConnections = 2, MaxConnectionsPerPeer = 1, WatchInterval = TimeSpan.FromMilliseconds(20) }))
        {
            await using var first = await fixture.Connect();
            await first.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 1\r\n\r\n");
            await first.Response();
            await using var second = await fixture.Connect();
            await second.Closed();
            Check(fixture.Server.ActiveConnectionCount == 1, "per-peer admission did not retain exactly one connection");
        }
        await using (var fixture = new ServerFixture(new RtspResourceLimits
        { MaxConnections = 1, MaxConnectionsPerPeer = 4, WatchInterval = TimeSpan.FromMilliseconds(20) }))
        {
            await using var first = await fixture.Connect();
            await first.Send("OPTIONS rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 1\r\n\r\n");
            await first.Response();
            await using var second = await fixture.Connect();
            await second.Closed();
            Check(fixture.Server.ActiveConnectionCount == 1, "global admission did not retain exactly one connection");
        }
        await using (var fixture = new ServerFixture(new RtspResourceLimits
        {
            MaxSessionsPerConnection = 1, SessionIdleTimeout = TimeSpan.FromMilliseconds(250),
            ConnectionIdleTimeout = TimeSpan.FromSeconds(3), WatchInterval = TimeSpan.FromMilliseconds(20)
        }))
        {
            await using var peer = await fixture.Connect();
            string session = await peer.Setup();
            await peer.Send("SETUP rtsp://127.0.0.1/synthetic/trackID=0 RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n\r\n");
            Check((await peer.Response()).StartsWith("RTSP/1.0 453", StringComparison.Ordinal), "session budget was not enforced");
            await Task.Delay(400);
            await peer.Send($"PLAY rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 3\r\nSession: {session}\r\n\r\n");
            Check((await peer.Response()).StartsWith("RTSP/1.0 454", StringComparison.Ordinal), "unused session did not expire");
            Check(!string.IsNullOrEmpty(await peer.Setup()), "expired session did not release its resource slot");
        }
        await using (var fixture = new ServerFixture(new RtspResourceLimits
        { ConnectionIdleTimeout = TimeSpan.FromMilliseconds(150), WatchInterval = TimeSpan.FromMilliseconds(20) }))
        {
            await using var idle = await fixture.Connect();
            await idle.Closed();
        }
    }

    public static async Task MediaActivityAndShutdown()
    {
        var clock = new ManualTimeProvider();
        await using var fixture = new ServerFixture(new RtspResourceLimits
        { WatchInterval = TimeSpan.FromMilliseconds(20) }, clock);
        await using var peer = await fixture.Connect();
        string session = await peer.Setup();
        await peer.Send($"PLAY rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 2\r\nSession: {session}\r\n\r\n");
        Check((await peer.Response()).StartsWith("RTSP/1.0 200", StringComparison.Ordinal), "PLAY failed");
        await peer.Media();
        // Exercise more than the advertised sixty seconds without any keepalive request.
        for (uint i = 1; i <= 70; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            fixture.Hub.PublishVideo(Frame(i * 50000));
            await peer.Media();
        }
        await Task.Delay(80);
        Check(fixture.Server.ActiveConnectionCount == 1 && fixture.Hub.SubscriberCount == 1,
            "ongoing media was treated as an idle sixty-second session");
        // A PAUSE/PLAY successor cannot overlap its cancelled predecessor.
        await peer.Send($"PAUSE rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 3\r\nSession: {session}\r\n\r\n");
        await peer.Response();
        Check(fixture.Hub.SubscriberCount == 0, "PAUSE responded before its old media subscription was released");
        await peer.Send($"PLAY rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 4\r\nSession: {session}\r\n\r\n");
        await peer.Response(); await peer.Media();
        await fixture.Stop();
        Check(fixture.Hub.SubscriberCount == 0 && fixture.Server.ActiveConnectionCount == 0,
            "server shutdown returned with a live pump or connection");
        await peer.Closed();
    }

    public static async Task ReplacedTransportsAndSessionHeader()
    {
        await using var fixture = new ServerFixture(new RtspResourceLimits());
        await using var peer = await fixture.Connect();
        string session = await peer.Setup();
        await peer.Send($"SETUP rtsp://127.0.0.1/synthetic/trackID=0 RTSP/1.0\r\nCSeq: 2\r\nSession: {session};timeout=60\r\nTransport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n\r\n");
        string second = await peer.Response();
        Check(second.StartsWith("RTSP/1.0 200", StringComparison.Ordinal) && second.Contains("Session: " + session + ";", StringComparison.Ordinal),
            "SETUP with a parameterized Session header created another session");
        // Direct resource check uses only ephemeral loopback UDP sockets.
        using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using var accepted = await listener.AcceptTcpClientAsync();
        var connection = new RtspConnection(accepted, fixture.Server);
        var owner = new RtspSession(connection, new RtspMount { Path = "/synthetic", Hub = fixture.Hub }, false);
        var old = TrackTransport.ForUdp(new IPEndPoint(IPAddress.Loopback, 5555), new RtpPacketizer(96)).transport;
        var fresh = TrackTransport.ForUdp(new IPEndPoint(IPAddress.Loopback, 5555), new RtpPacketizer(96)).transport;
        owner.SetTrack(0, old); owner.SetTrack(0, fresh);
        Check(old.UdpSocket!.SafeHandle.IsClosed && old.UdpRtcpSocket!.SafeHandle.IsClosed, "replaced RTP/RTCP sockets leaked");
        await owner.StopAsync();
        Check(fresh.UdpSocket!.SafeHandle.IsClosed && fresh.UdpRtcpSocket!.SafeHandle.IsClosed, "current RTP/RTCP sockets leaked on stop");
    }

    public static async Task ActorScopedTalk()
    {
        var control = System.Reflection.DispatchProxy.Create<ICameraControl, RecoveryControl>();
        var probe = (RecoveryControl)(object)control; probe.Failure = -1;
        var users = new Dictionary<string, string> { ["viewer"] = "synthetic-password", ["actor"] = "synthetic-password" };
        await using var fixture = new ServerFixture(new RtspResourceLimits(), users: users, talk: control, talkUsers: ["actor"]);
        await using var viewer = await fixture.Connect();
        viewer.Authorization = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes("viewer:synthetic-password"));
        await viewer.Send("DESCRIBE rtsp://127.0.0.1/synthetic?audio=original RTSP/1.0\r\nCSeq: 1\r\nRequire: www.onvif.org/ver20/backchannel\r\n\r\n");
        string readonlySdp = await viewer.Response();
        Check(readonlySdp.StartsWith("RTSP/1.0 200", StringComparison.Ordinal) && !readonlySdp.Contains("trackID=2", StringComparison.Ordinal)
            && probe.CapabilityReads == 0, "read-only user was offered speaker control or caused a camera capability read");
        Check(readonlySdp.Contains("Content-Base: rtsp://127.0.0.1/synthetic/\r\n", StringComparison.Ordinal),
            "audio query contaminated relative SDP track URLs");
        await viewer.Send("SETUP rtsp://127.0.0.1/synthetic/trackID=2 RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;interleaved=4-5\r\n\r\n");
        Check((await viewer.Response()).StartsWith("RTSP/1.0 403", StringComparison.Ordinal) && probe.Attempts == 0,
            "read-only user could set up a speaker track");
        await using var actor = await fixture.Connect();
        actor.Authorization = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes("actor:synthetic-password"));
        await actor.Send("DESCRIBE rtsp://127.0.0.1/synthetic RTSP/1.0\r\nCSeq: 1\r\nRequire: www.onvif.org/ver20/backchannel\r\n\r\n");
        Check((await actor.Response()).Contains("trackID=2", StringComparison.Ordinal), "authorized actor lost speaker discovery");
        await actor.Send("SETUP rtsp://127.0.0.1/synthetic/trackID=2 RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;interleaved=4-5\r\n\r\n");
        Check((await actor.Response()).StartsWith("RTSP/1.0 200", StringComparison.Ordinal) && probe.Attempts == 0,
            "authorized actor could not arm talk lazily");
    }

    private sealed class ServerFixture : IAsyncDisposable
    {
        public StreamHub Hub { get; } = new("synthetic-reliability");
        public RtspServer Server { get; }
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _run;
        public ServerFixture(RtspResourceLimits limits, TimeProvider? clock = null,
            IReadOnlyDictionary<string, string>? users = null, ICameraControl? talk = null, HashSet<string>? talkUsers = null)
        {
            Hub.PublishVideo(Frame());
            Server = new(users ?? new Dictionary<string, string>()) { ResourceLimits = limits, ResourceTimeProvider = clock ?? TimeProvider.System };
            Server.AddMount(new RtspMount { Path = "/synthetic", Hub = Hub, PermittedUsers = users?.Keys.ToHashSet(),
                Talk = talk, TalkPermittedUsers = talkUsers });
            _run = Server.RunAsync("127.0.0.1", 0, _stop.Token);
        }
        public async Task<Peer> Connect()
        {
            var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, Server.ListeningPort);
            return new Peer(client);
        }
        public async Task Stop() { _stop.Cancel(); await _run.WaitAsync(TimeSpan.FromSeconds(3)); }
        public async ValueTask DisposeAsync() { await Stop(); _stop.Dispose(); }
    }

    private sealed class Peer(TcpClient client) : IAsyncDisposable
    {
        private readonly NetworkStream _stream = client.GetStream();
        private int _mediaBuffered;
        public string? Authorization { get; set; }
        public async Task Send(string request)
        {
            if (Authorization != null) request = request.Replace("\r\n\r\n", "\r\nAuthorization: " + Authorization + "\r\n\r\n", StringComparison.Ordinal);
            await _stream.WriteAsync(Encoding.ASCII.GetBytes(request));
        }
        private async Task<byte[]> Read(int count)
        {
            byte[] data = new byte[count];
            // A first DESCRIBE may legitimately use the source's two-second audio grace.
            await _stream.ReadExactlyAsync(data).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            return data;
        }
        private async Task FramedMedia()
        {
            byte[] header = await Read(3);
            await Read((header[1] << 8) | header[2]);
        }
        public async Task<string> Response()
        {
            var text = new StringBuilder();
            while (true)
            {
                byte first = (await Read(1))[0];
                if (first == '$') { await FramedMedia(); _mediaBuffered++; continue; }
                text.Append((char)first);
                while (!text.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal)) text.Append((char)(await Read(1))[0]);
                string response = text.ToString();
                string? length = response.Split("\r\n").FirstOrDefault(line => line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase));
                if (length != null && int.Parse(length.Split(':')[1]) is var bytes && bytes > 0) response += Encoding.ASCII.GetString(await Read(bytes));
                return response;
            }
        }
        public async Task<string> Setup()
        {
            await Send("SETUP rtsp://127.0.0.1/synthetic/trackID=0 RTSP/1.0\r\nCSeq: 1\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n");
            string response = await Response();
            Check(response.StartsWith("RTSP/1.0 200", StringComparison.Ordinal), "normal SETUP failed: " + response.Split("\r\n")[0]);
            return response.Split("\r\n").First(line => line.StartsWith("Session:", StringComparison.OrdinalIgnoreCase)).Split(':')[1].Split(';')[0].Trim();
        }
        public async Task Media()
        {
            if (_mediaBuffered > 0) { _mediaBuffered--; return; }
            Check((await Read(1))[0] == '$', "expected an interleaved media packet");
            await FramedMedia();
        }
        public async Task Closed()
        {
            var elapsed = Stopwatch.StartNew();
            try
            {
                var buffer = new byte[1024];
                while (await _stream.ReadAsync(buffer).AsTask().WaitAsync(TimeSpan.FromSeconds(2)) != 0)
                    Check(elapsed.Elapsed < TimeSpan.FromSeconds(2), "connection did not close");
            }
            catch (IOException) { }
            catch (SocketException) { }
        }
        public ValueTask DisposeAsync() { client.Dispose(); return ValueTask.CompletedTask; }
    }
}
