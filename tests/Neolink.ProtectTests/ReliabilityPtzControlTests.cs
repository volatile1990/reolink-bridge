using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Onvif;
using Neolink.Streaming;

/// <summary>Real loopback HTTP contracts using a synthetic camera. Never connects to hardware.</summary>
internal static class ReliabilityPtzControlTests
{
    public static async Task RunAsync()
    {
        await DiscoveryAsync();
        await RelativeOwnershipAsync(rest: false);
        await RelativeOwnershipAsync(rest: true);
        await ShutdownAsync(rest: false, zoom: false);
        await ShutdownAsync(rest: true, zoom: false);
        await ShutdownAsync(rest: false, zoom: true);
        await LazyControlsAsync();
        await EpochReadAsync();
        await SharedCapabilitiesAsync();
        await ShutdownDuringDiscoveryAsync();
        await PermissionsAsync();
        Console.WriteLine("Reliability PTZ/control loopback contracts passed: discovery, ownership, ordered stop/zoom shutdown, lazy reads, epoch cache and permissions.");
    }

    private static void Check(bool value, string error) { if (!value) throw new Exception(error); }
    private static async Task Until(Func<bool> predicate)
    {
        var elapsed = Stopwatch.StartNew();
        while (!predicate())
        { if (elapsed.Elapsed > TimeSpan.FromSeconds(3)) throw new Exception("Synthetic operation did not start"); await Task.Delay(5); }
    }

    private static async Task DiscoveryAsync()
    {
        await using var bridge = new Fixture();
        using var anonymous = await bridge.Soap("GetServices", device: true, user: null);
        Check(anonymous.IsSuccessStatusCode && bridge.Probe.CapabilityCalls == 0,
            "Anonymous discovery initiated camera I/O");
        using var services = await bridge.Soap("GetServices", device: true);
        string servicesXml = await services.Content.ReadAsStringAsync();
        Check(services.IsSuccessStatusCode && servicesXml.Contains(ProtectOnvifServer.NsPtz + "</tds:Namespace>"),
            "Authenticated cold service discovery omitted real PTZ");
        using var capabilities = await bridge.Soap("GetCapabilities", device: true);
        Check((await capabilities.Content.ReadAsStringAsync()).Contains("<tt:PTZ>"), "Cold capabilities omitted PTZ");
        using var profiles = await bridge.Soap("GetProfiles", media: true);
        var profileXml = XDocument.Parse(await profiles.Content.ReadAsStringAsync());
        Check(profiles.IsSuccessStatusCode && profileXml.Descendants().Any(e => e.Name.LocalName == "PTZConfiguration")
            && bridge.Probe.CapabilityCalls == 1, "ONVIF metadata did not share one capability discovery");
        bridge.Hub.SourceStopped(); bridge.Probe.SupportsPtz = false; bridge.Probe.SupportsZoom = false;
        using var reconnected = await bridge.Soap("GetProfiles", media: true);
        Check(!XDocument.Parse(await reconnected.Content.ReadAsStringAsync()).Descendants().Any(e => e.Name.LocalName == "PTZConfiguration")
            && bridge.Probe.CapabilityCalls == 2, "A new camera epoch retained old PTZ capabilities");
    }

    private static async Task RelativeOwnershipAsync(bool rest)
    {
        await using var bridge = new Fixture();
        var relative = bridge.Soap("RelativeMove", "<Translation><PanTilt x='-0.8' y='0.8'/></Translation>");
        await Until(() => bridge.Probe.Commands.Contains("left"));
        if (rest)
        {
            var newOwner = bridge.Control("POST", "ptz", "{\"values\":{\"command\":\"right\",\"durationMs\":1000}}");
            await Until(() => bridge.Probe.Commands.Contains("right"));
            using var oldReply = await relative;
            Check(oldReply.IsSuccessStatusCode && bridge.Probe.Commands.SequenceEqual(new[] { "left", "right" }),
                "Older relative request stopped or moved a newer REST owner");
            using var newReply = await newOwner;
            Check(newReply.IsSuccessStatusCode && bridge.Probe.Commands.SequenceEqual(new[] { "left", "right", "stop" }),
                "REST owner's own bounded stop was lost");
        }
        else
        {
            using var newer = await bridge.Soap("ContinuousMove", "<Velocity><PanTilt x='1' y='0'/></Velocity><Timeout>PT10S</Timeout>");
            using var older = await relative;
            Check(newer.IsSuccessStatusCode && older.IsSuccessStatusCode
                && bridge.Probe.Commands.SequenceEqual(new[] { "left", "right" }),
                "Older relative request stopped a newer continuous owner or executed its second axis");
        }
    }

    private static async Task ShutdownAsync(bool rest, bool zoom)
    {
        await using var bridge = new Fixture();
        Task<HttpResponseMessage>? pending = null;
        if (rest)
        {
            pending = bridge.Control("POST", "ptz", "{\"values\":{\"command\":\"left\",\"durationMs\":1000}}");
            await Until(() => bridge.Probe.Commands.Contains("left"));
        }
        else
        {
            string axes = "<PanTilt x='1' y='0'/>" + (zoom ? "<Zoom x='0.5'/>" : "");
            using var started = await bridge.Soap("ContinuousMove", "<Velocity>" + axes + "</Velocity><Timeout>PT10S</Timeout>");
            Check(started.IsSuccessStatusCode, "Synthetic continuous move was rejected");
            if (zoom) await Until(() => bridge.Probe.ZoomWrites > 0);
        }
        var watch = Stopwatch.StartNew();
        bridge.ListenerStop.Cancel();
        // The same helper used by the production host must keep the source alive
        // while the listener's shared owner sends its final stop.
        await ProtectBridgeHost.StopControlsBeforeSourceAsync(bridge.Server, bridge.SourceStop);
        await bridge.Listener;
        if (pending != null) { try { using var ignored = await pending; } catch (HttpRequestException) { } }
        Check(bridge.Probe.Commands.LastOrDefault() == "stop" && bridge.Probe.StopSawLiveSource
            && bridge.SourceStop.IsCancellationRequested && watch.Elapsed < TimeSpan.FromSeconds(5),
            "Shutdown did not stop the head before disposing its source");
        int writes = bridge.Probe.ZoomWrites;
        await Task.Delay(300);
        Check(bridge.Probe.ZoomWrites == writes, "A zoom loop continued writing after shutdown");
    }

    private static async Task LazyControlsAsync()
    {
        await using var bridge = new Fixture();
        using var caps = await bridge.Control("GET", "capabilities");
        Check(caps.IsSuccessStatusCode && bridge.Probe.Reads.All(r => r == "GetCapabilitiesAsync"),
            "Capability-only endpoint performed a settings sweep");
        bridge.Probe.Reads.Clear();
        using var volume = await bridge.Control("GET", "volume");
        Check(volume.IsSuccessStatusCode && bridge.Probe.Reads.SequenceEqual(new[] { "GetVolumeAsync" }),
            "Lazy resource read queried unrelated camera settings");
        bridge.Probe.Reads.Clear();
        using var state = await bridge.Control("GET", "state");
        Check(state.IsSuccessStatusCode && !bridge.Probe.Reads.Contains("GetHttpFeaturesAsync")
            && !bridge.Probe.Reads.Contains("GetSdCardsAsync") && !bridge.Probe.Reads.Contains("GetQuickRepliesAsync")
            && !bridge.Probe.Reads.Contains("GetWifiSignalAsync"), "Bridge state queried resources it does not expose");
        bridge.Probe.FailVolume = true;
        using var failed = await bridge.Control("GET", "volume");
        string failure = await failed.Content.ReadAsStringAsync();
        Check(failed.IsSuccessStatusCode && failure.Contains("read-timeout") && !failure.Contains("PRIVATE-SENTINEL"),
            "Resource read failure was silent or reflected private error text");
        using (var failedJson = JsonDocument.Parse(failure))
            Check(failedJson.RootElement.GetProperty("state").GetProperty("volume").GetInt32() == 30
                && failedJson.RootElement.GetProperty("descriptor").GetProperty("reason").GetString() == "cached-state-read-unavailable",
                "A transient read hid the last-known same-session resource state without a cache warning");
        bridge.Probe.FailVolume = false; bridge.Probe.FailReadback = true;
        using var write = await bridge.Control("POST", "volume", "{\"values\":{\"volume\":45}}");
        using var json = JsonDocument.Parse(await write.Content.ReadAsStringAsync());
        Check(write.IsSuccessStatusCode && !json.RootElement.GetProperty("readbackAvailable").GetBoolean()
            && json.RootElement.GetProperty("readError").GetString() == "read-timeout",
            "Write falsely presented a failed readback as available");
    }

    private static async Task SharedCapabilitiesAsync()
    {
        await using var bridge = new Fixture();
        bridge.Probe.CapabilityRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancelled = new CancellationTokenSource();
        var first = bridge.Controls.CapabilitiesAsync(cancelled.Token);
        await Until(() => bridge.Probe.CapabilityCalls == 1);
        var second = bridge.Controls.CapabilitiesAsync(CancellationToken.None);
        cancelled.Cancel();
        bool didCancel = false; try { await first; } catch (OperationCanceledException) { didCancel = true; }
        bridge.Probe.CapabilityRelease.TrySetResult();
        var result = await second;
        Check(didCancel && !result.Provisional && bridge.Probe.CapabilityCalls == 1,
            "A cancelled caller interrupted shared discovery or duplicated its camera probe");
    }

    private static async Task EpochReadAsync()
    {
        await using var bridge = new Fixture();
        using var first = await bridge.Control("GET", "volume");
        Check(first.IsSuccessStatusCode, "Initial state read failed");
        bridge.Probe.VolumeReadHook = bridge.Hub.SourceStopped;
        using var interrupted = await bridge.Control("GET", "volume");
        Check(interrupted.StatusCode == HttpStatusCode.ServiceUnavailable, "A cross-epoch state read was served as current");
        bridge.Probe.VolumeReadHook = null;
        using var caps = await bridge.Control("GET", "capabilities");
        using var json = JsonDocument.Parse(await caps.Content.ReadAsStringAsync());
        Check(!json.RootElement.GetProperty("values").GetProperty("volume").EnumerateObject().Any(),
            "A completed old-session read poisoned the new epoch's capability snapshot");
    }

    private static async Task ShutdownDuringDiscoveryAsync()
    {
        await using var bridge = new Fixture();
        using var move = await bridge.Soap("ContinuousMove", "<Velocity><PanTilt x='1' y='0'/></Velocity><Timeout>PT10S</Timeout>");
        Check(move.IsSuccessStatusCode, "Move before discovery shutdown was rejected");
        bridge.Hub.SourceStopped();
        bridge.Probe.CapabilityRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocked = bridge.Controls.CapabilitiesAsync(CancellationToken.None);
        await Until(() => bridge.Probe.CapabilityCalls == 2 && bridge.Probe.CapabilityGateHeld == 1);
        bridge.ListenerStop.Cancel();
        await ProtectBridgeHost.StopControlsBeforeSourceAsync(bridge.Server, bridge.SourceStop);
        await bridge.Listener;
        bool cancelled = false; try { await blocked; } catch (OperationCanceledException) { cancelled = true; }
        Check(cancelled && bridge.Probe.Commands.LastOrDefault() == "stop" && bridge.Probe.StopSawLiveSource,
            "Shared capability discovery held the command gate and starved shutdown's independent stop");
    }

    private static async Task PermissionsAsync()
    {
        await using var bridge = new Fixture(controlUsers: ["actor"]);
        using var view = await bridge.Soap("GetProfiles", media: true, user: "viewer");
        using var read = await bridge.Control("GET", "volume", user: "viewer");
        Check(view.IsSuccessStatusCode && read.IsSuccessStatusCode, "Read-only user lost metadata or settings access");
        int prior = bridge.Probe.Writes;
        using var noRest = await bridge.Control("POST", "volume", "{\"values\":{\"volume\":45}}", "viewer");
        using var noNative = await bridge.Soap("ContinuousMove", "<Velocity><PanTilt x='1' y='0'/></Velocity>", user: "viewer");
        using var noListen = await bridge.Control("GET", "listen", user: "viewer");
        Check(noRest.StatusCode == HttpStatusCode.Forbidden && noNative.StatusCode == HttpStatusCode.Forbidden
            && noListen.StatusCode == HttpStatusCode.Forbidden && bridge.Probe.Writes == prior,
            "Read-only user reached an actuator or protected microphone");
        using var allowed = await bridge.Control("POST", "volume", "{\"values\":{\"volume\":45}}", "actor");
        Check(allowed.IsSuccessStatusCode && bridge.Probe.Writes == prior + 1, "Explicit actuator user could not write");
        await using var empty = new Fixture(controlUsers: []);
        using var denied = await empty.Control("POST", "volume", "{\"values\":{\"volume\":45}}");
        Check(denied.StatusCode == HttpStatusCode.Forbidden && empty.Probe.Writes == 0, "Empty control-users list granted an actuator");
    }

    private sealed class Fixture : IAsyncDisposable
    {
        public readonly StreamHub Hub = new("synthetic-reliability");
        public readonly CancellationTokenSource ListenerStop = new(), SourceStop = new();
        public readonly ProtectOnvifServer Server;
        public readonly ProtectCameraControls Controls;
        public readonly CameraProbe Probe;
        public readonly Task Listener;
        private readonly HttpClient _http;
        public Fixture(List<string>? controlUsers = null)
        {
            using var reservation = new TcpListener(IPAddress.Loopback, 0); reservation.Start();
            int port = ((IPEndPoint)reservation.LocalEndpoint).Port; reservation.Stop();
            var proxy = DispatchProxy.Create<ICameraControl, CameraProbe>(); Probe = (CameraProbe)(object)proxy;
            SourceStop.Token.Register(() => Probe.SourceAlive = false);
            Controls = new ProtectCameraControls("synthetic-reliability", proxy, sourceEpoch: () => Hub.SourceEpoch);
            var config = new BridgeOnvifConfig
            {
                Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Port = port, Controls = true, Events = false, Discovery = false,
                Mac = "02:11:22:33:44:55", Uuid = "12345678-1234-4234-9234-123456789012", ControlUsers = controlUsers,
                Profiles = [new() { Stream = "mainStream", Codec = "H265", Width = 1920, Height = 1080, Fps = 20, Bitrate = 2048 }]
            };
            Server = new(config, new Dictionary<string, string> { ["actor"] = "synthetic-only", ["viewer"] = "synthetic-only" },
                [new("main", "/synthetic/mainStream", Hub)], 18554, controls: Controls);
            Listener = Server.RunAsync(ListenerStop.Token);
            _http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}"), Timeout = TimeSpan.FromSeconds(8) };
        }
        public Task<HttpResponseMessage> Soap(string action, string inner = "", bool device = false, bool media = false, string? user = "actor")
        {
            string ns = device ? ProtectOnvifServer.NsDevice : media ? ProtectOnvifServer.NsMedia : ProtectOnvifServer.NsPtz;
            string body = $"<s:Envelope xmlns:s='{ProtectOnvifServer.NsSoap}'><s:Body><p:{action} xmlns:p='{ns}'>" +
                (!device && !media ? "<ProfileToken>main</ProfileToken>" : "") + inner + $"</p:{action}></s:Body></s:Envelope>";
            var request = new HttpRequestMessage(HttpMethod.Post, device ? "/onvif/device_service" : media ? "/onvif/media_service" : "/onvif/ptz_service")
            { Content = new StringContent(body, Encoding.UTF8, "application/soap+xml") };
            Authorize(request, user); return Send(request);
        }
        public Task<HttpResponseMessage> Control(string method, string resource, string body = "", string? user = "actor")
        {
            var request = new HttpRequestMessage(new HttpMethod(method), "/control/" + resource);
            Authorize(request, user);
            if (method == "POST")
            { request.Content = new StringContent(body, Encoding.UTF8, "application/json"); request.Headers.Add("X-Reolink-Control-Intent", "user"); }
            return Send(request);
        }
        private static void Authorize(HttpRequestMessage request, string? user)
        { if (user != null) request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":synthetic-only"))); }
        private async Task<HttpResponseMessage> Send(HttpRequestMessage request)
        { using (request) return await _http.SendAsync(request); }
        public async ValueTask DisposeAsync()
        {
            ListenerStop.Cancel(); await ProtectBridgeHost.StopControlsBeforeSourceAsync(Server, SourceStop);
            try { await Listener; } catch (OperationCanceledException) { }
            _http.Dispose(); ListenerStop.Dispose(); SourceStop.Dispose();
        }
    }

    public class CameraProbe : DispatchProxy
    {
        public readonly ConcurrentQueue<string> Commands = new(), Reads = new();
        private Action<string>? _notices;
        public int CapabilityCalls, CapabilityGateHeld, Writes, ZoomWrites;
        public bool SupportsPtz = true, SupportsZoom = true, SourceAlive = true, StopSawLiveSource;
        public bool FailVolume, FailReadback;
        public TaskCompletionSource? CapabilityRelease;
        public Action? VolumeReadHook;
        private int _volume = 30;
        private readonly SemaphoreSlim _cameraGate = new(1, 1);
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            string name = method!.Name;
            if (name == "add_PtzCommandSent") { _notices += (Action<string>)args![0]!; return null; }
            if (name == "remove_PtzCommandSent") { _notices -= (Action<string>)args![0]!; return null; }
            if (name.StartsWith("Get", StringComparison.Ordinal)) Reads.Enqueue(name);
            if (name == "GetVolumeAsync") VolumeReadHook?.Invoke();
            if (name == "GetCapabilitiesAsync")
            { Interlocked.Increment(ref CapabilityCalls); return Capabilities((CancellationToken)args![0]!); }
            if (name == "PtzAsync")
                return SendPtz((string)args![0]!, (CancellationToken)args[2]!);
            if (name == "SetZoomFocusAsync")
            { Interlocked.Increment(ref ZoomWrites); Interlocked.Increment(ref Writes); return Task.CompletedTask; }
            if (name == "SetVolumeAsync")
            { _volume = (int)args![0]!; Interlocked.Increment(ref Writes); if (FailReadback) FailVolume = true; return Task.CompletedTask; }
            if (method.ReturnType == typeof(Task)) { Interlocked.Increment(ref Writes); return Task.CompletedTask; }
            if (name == "ZoneTypes") return new[] { "md" };
            if (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                Type result = method.ReturnType.GenericTypeArguments[0];
                if (name == "GetVolumeAsync" && FailVolume)
                    return typeof(Task).GetMethods().Single(m => m.Name == nameof(Task.FromException) && m.IsGenericMethod)
                        .MakeGenericMethod(result).Invoke(null, [new TimeoutException("PRIVATE-SENTINEL")]);
                object? value = name switch
                {
                    "GetVolumeAsync" => (int?)_volume,
                    "GetZoomFocusAsync" => new XElement("PtzZoomFocus", new XElement("zoom", new XElement("minPos", 0), new XElement("maxPos", 100), new XElement("curPos", 0))),
                    _ => result.IsValueType ? Activator.CreateInstance(result) : null
                };
                return typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(result).Invoke(null, [value]);
            }
            return method.ReturnType == typeof(bool) ? false : null;
        }
        private async Task<CameraCapabilities> Capabilities(CancellationToken ct)
        {
            await _cameraGate.WaitAsync(ct);
            Interlocked.Increment(ref CapabilityGateHeld);
            try
            {
                if (CapabilityRelease != null) await CapabilityRelease.Task.WaitAsync(ct);
                return new(null, null, new CameraFeatures(SupportsPtz, false, false, false, false, Zoom: SupportsZoom));
            }
            finally { Interlocked.Decrement(ref CapabilityGateHeld); _cameraGate.Release(); }
        }
        private async Task SendPtz(string command, CancellationToken ct)
        {
            await _cameraGate.WaitAsync(ct);
            try
            {
                if (!SourceAlive) throw new IOException("synthetic source already disposed");
                Commands.Enqueue(command); Interlocked.Increment(ref Writes);
                if (command == "stop") StopSawLiveSource = SourceAlive;
                _notices?.Invoke(command);
            }
            finally { _cameraGate.Release(); }
        }
    }
}
