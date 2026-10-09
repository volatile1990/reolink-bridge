using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using Neolink.Bc;
using Neolink.Bc.Xml;
using Neolink.Config;
using Neolink.Onvif;
using Neolink.Protocol;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class AuthParkingTests
{
    private const string User = "synthetic-auth-user";
    private const string Password = "synthetic-auth-password";
    private static string Basic => "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(User + ":" + Password));

    public static async Task Explicit401IsAuthenticationFailure()
    {
        await using var fake = new FakeBaichuanCamera(401, emptyReply: false);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var camera = await BcCamera.ConnectAsync("127.0.0.1", fake.Port, 0, timeout.Token, "synthetic-401");
        bool classified = false;
        try { await camera.LoginAsync(User, Password, timeout.Token); }
        catch (AuthFailedException) { classified = true; }
        Check(classified && fake.ModernLogins == 1, "nonempty phase-two 401 must be an authentication failure");
    }

    public static async Task ExistingEmptyReplyRemainsAuthenticationFailure()
    {
        await using var fake = new FakeBaichuanCamera(200, emptyReply: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var camera = await BcCamera.ConnectAsync("127.0.0.1", fake.Port, 0, timeout.Token, "synthetic-empty-refusal");
        bool classified = false;
        try { await camera.LoginAsync(User, Password, timeout.Token); }
        catch (AuthFailedException) { classified = true; }
        Check(classified && fake.ModernLogins == 1, "existing empty modern refusal must remain an authentication failure");
    }

    public static async Task OtherXmlFailureRemainsProtocolFailure()
    {
        await using var fake = new FakeBaichuanCamera(500, emptyReply: false);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var camera = await BcCamera.ConnectAsync("127.0.0.1", fake.Port, 0, timeout.Token, "synthetic-500");
        bool classified = false;
        try { await camera.LoginAsync(User, Password, timeout.Token); }
        catch (BcProtocolException) { classified = true; }
        Check(classified && fake.ModernLogins == 1, "phase-two XML 500 must not be reclassified as credential failure");
    }

    public static async Task TransportFailureStillReconnects()
    {
        await using var fake = new FakeBaichuanCamera(500, emptyReply: false, dropBeforeNonce: true);
        var hub = new StreamHub("synthetic-transport");
        var source = Source(fake.Port, hub, "synthetic-transport");
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        Task running = source.RunAsync(stop.Token);
        try
        {
            await Until(() => fake.Connections >= 2, TimeSpan.FromSeconds(4));
            Check(!hub.AuthenticationFailed && !running.IsCompleted, "transport failure incorrectly parked authentication or ended source");
        }
        finally
        {
            stop.Cancel();
            await running.WaitAsync(TimeSpan.FromSeconds(2));
        }
    }

    public static Task AuthenticationFailuresParkWithoutRetry() => Task.WhenAll(
        ParkOne(401, false, "synthetic-explicit-park"),
        ParkOne(200, true, "synthetic-empty-park"));

    private static CameraService Source(int port, StreamHub hub, string name) => new(
        new CameraConfig
        {
            Name = name, Host = "127.0.0.1", Port = port, Username = User, Password = Password,
            Stream = "mainStream", AlwaysOn = true, Udp = false, UdpProbe = false, Record = false
        }, StreamKind.Main, hub, TimeSpan.Zero) { RelayOnly = true };

    private static async Task ParkOne(ushort code, bool emptyReply, string name)
    {
        await using var fake = new FakeBaichuanCamera(code, emptyReply);
        var hub = new StreamHub(name);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var source = Source(fake.Port, hub, name);
        int rtspPort = FreePort();
        var users = new Dictionary<string, string> { [User] = Password };
        var rtsp = new RtspServer(users) { TcpOnly = true };
        rtsp.AddMount(new RtspMount { Path = "/park/mainStream", Hub = hub, PermittedUsers = users.Keys.ToHashSet() });
        var config = new BridgeOnvifConfig
        {
            Port = FreePort(), Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Mac = emptyReply ? "02:54:45:53:54:04" : "02:54:45:53:54:03",
            Uuid = Guid.NewGuid().ToString("D"), Name = name, Model = "Synthetic auth test", Discovery = false,
            Profiles = [new BridgeOnvifProfile { Stream = "mainStream", Codec = "H264", Width = 1920, Height = 1080, Fps = 20, Bitrate = 4096 }]
        };
        var onvif = new ProtectOnvifServer(config, users, [new ProtectOnvifStream("main", "/park/mainStream", hub)], rtspPort);
        Task sourceRunning = source.RunAsync(stop.Token);
        Task rtspRunning = rtsp.RunAsync("127.0.0.1", rtspPort, stop.Token);
        Task onvifRunning = onvif.RunAsync(stop.Token);
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{config.Port}"), Timeout = TimeSpan.FromSeconds(3) };
        try
        {
            await Until(() => hub.AuthenticationFailed, TimeSpan.FromSeconds(4));
            Check(!sourceRunning.IsCompleted && fake.Connections == 1 && fake.ModernLogins == 1, "authentication source must remain parked after exactly one login");
            using var health = await http.GetAsync("/health");
            Check((int)health.StatusCode == 503, "parked source must fail health");
            using var metricsRequest = new HttpRequestMessage(HttpMethod.Get, "/metrics");
            metricsRequest.Headers.TryAddWithoutValidation("Authorization", Basic);
            using var metrics = await http.SendAsync(metricsRequest);
            Check((int)metrics.StatusCode == 200, "metrics listener stopped while source parked");
            string body = await metrics.Content.ReadAsStringAsync();
            using var document = JsonDocument.Parse(body);
            Check(document.RootElement.GetProperty("status").GetString() == "authentication-failed", "metrics must identify the credential failure");
            Check(document.RootElement.GetProperty("streams")[0].GetProperty("authenticationFailed").GetBoolean(), "stream metric lost authentication failure");
            Check(!body.Contains(User) && !body.Contains(Password), "authentication-failure metrics contain credentials");
            using var soapRequest = new HttpRequestMessage(HttpMethod.Post, "/onvif/device_service");
            soapRequest.Headers.TryAddWithoutValidation("Authorization", Basic);
            soapRequest.Content = new StringContent("<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\"><s:Body><GetNetworkInterfaces xmlns=\"http://www.onvif.org/ver10/device/wsdl\"/></s:Body></s:Envelope>", Encoding.UTF8, "application/soap+xml");
            using var soap = await http.SendAsync(soapRequest);
            Check((int)soap.StatusCode == 200, "ONVIF device listener stopped while source parked");
            Check((await RtspOptions(rtspPort, stop.Token)).StartsWith("RTSP/1.0 200", StringComparison.Ordinal), "RTSP listener stopped while source parked");
            // The old AuthFailed path retried after 30 seconds. Both wire scenarios
            // run concurrently so this detects that regression in one bounded window.
            await Task.Delay(TimeSpan.FromSeconds(32), stop.Token);
            Check(fake.Connections == 1 && fake.ModernLogins == 1 && !sourceRunning.IsCompleted, "parked authentication source attempted another camera login");
            Check(hub.AuthenticationFailed && !hub.LiveVideo && !onvif.Healthy, "authentication failure ceased to be a terminal source state");
            var restartHub = new StreamHub("synthetic-restart");
            Check(!restartHub.AuthenticationFailed, "fresh process/source inherits previous authentication failure");
            var cancellation = Stopwatch.StartNew();
            stop.Cancel();
            await sourceRunning.WaitAsync(TimeSpan.FromSeconds(2));
            Check(cancellation.Elapsed < TimeSpan.FromSeconds(2), "parked source ignores cancellation");
        }
        finally
        {
            stop.Cancel();
            await Task.WhenAll(sourceRunning, rtspRunning, onvifRunning).WaitAsync(TimeSpan.FromSeconds(3));
        }
    }

    private static async Task<string> RtspOptions(int port, CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port, ct);
        var stream = client.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes($"OPTIONS rtsp://127.0.0.1:{port}/park/mainStream RTSP/1.0\r\nCSeq: 1\r\nAuthorization: {Basic}\r\n\r\n"), ct);
        var output = new StringBuilder();
        var bytes = new byte[2048];
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(3));
        while (!output.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
        {
            int read = await stream.ReadAsync(bytes, timeout.Token);
            Check(read != 0, "RTSP listener closed OPTIONS without a response");
            output.Append(Encoding.ASCII.GetString(bytes, 0, read));
        }
        return output.ToString();
    }

    private static async Task Until(Func<bool> ready, TimeSpan deadline)
    {
        var started = Stopwatch.StartNew();
        while (!ready())
        {
            Check(started.Elapsed < deadline, "timed out waiting for synthetic camera/source state");
            await Task.Delay(10);
        }
    }
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }
    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(); int port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop(); return port;
    }

    private sealed class FakeBaichuanCamera : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _serving;
        private readonly ushort _responseCode;
        private readonly bool _emptyReply, _dropBeforeNonce;
        private int _connections, _modernLogins;
        public int Port { get; }
        public int Connections => Volatile.Read(ref _connections);
        public int ModernLogins => Volatile.Read(ref _modernLogins);
        public FakeBaichuanCamera(ushort responseCode, bool emptyReply, bool dropBeforeNonce = false)
        {
            _responseCode = responseCode; _emptyReply = emptyReply; _dropBeforeNonce = dropBeforeNonce;
            _listener.Start(); Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _serving = Serve();
        }
        private async Task Serve()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    Interlocked.Increment(ref _connections);
                    var stream = client.GetStream();
                    var encryption = new EncryptionState();
                    var context = new BcContext(encryption);
                    using var requestLimit = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                    requestLimit.CancelAfter(TimeSpan.FromSeconds(4));
                    var upgrade = await BcCodec.ReadMessageAsync(stream, context, requestLimit.Token);
                    Check(upgrade.Meta.MsgId == BcConstants.MsgIdLogin && upgrade.Meta.Class == BcConstants.ClassLegacy, "expected legacy upgrade before synthetic nonce");
                    if (_dropBeforeNonce) continue;
                    var nonce = BcMessage.FromXml(new BcMeta
                    {
                        MsgId = BcConstants.MsgIdLogin, ChannelId = upgrade.Meta.ChannelId, MsgNum = upgrade.Meta.MsgNum,
                        Class = BcConstants.ClassModernNoOffset, ResponseCode = 0xdd00
                    }, BcXmlBody.FromRaw(new XElement("Encryption", new XAttribute("version", "1.1"),
                        new XElement("type", "none"), new XElement("nonce", "SYNTHETIC-NONCE"))));
                    await stream.WriteAsync(BcCodec.Serialize(nonce, encryption), requestLimit.Token);
                    var login = await BcCodec.ReadMessageAsync(stream, context, requestLimit.Token);
                    Check(login.Meta.MsgId == BcConstants.MsgIdLogin && login.Xml?.LoginUser != null, "expected modern login after synthetic nonce");
                    Interlocked.Increment(ref _modernLogins);
                    var meta = new BcMeta
                    {
                        MsgId = BcConstants.MsgIdLogin, ChannelId = login.Meta.ChannelId, MsgNum = login.Meta.MsgNum,
                        Class = BcConstants.ClassModernNoOffset, ResponseCode = _responseCode
                    };
                    var error = new BcXmlBody(); error.Raw.Add(new XElement("LoginError", "synthetic-refusal"));
                    var refusal = _emptyReply ? BcMessage.HeaderOnly(meta) : BcMessage.FromXml(meta, error);
                    await stream.WriteAsync(BcCodec.Serialize(refusal, encryption), requestLimit.Token);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (SocketException) when (_stop.IsCancellationRequested) { }
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel(); _listener.Stop();
            try { await _serving.WaitAsync(TimeSpan.FromSeconds(2)); }
            finally { _stop.Dispose(); }
        }
    }
}
