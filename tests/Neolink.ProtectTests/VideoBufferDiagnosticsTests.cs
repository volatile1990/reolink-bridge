using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Streaming;

internal static class VideoBufferDiagnosticsTests
{
    // Synthetic Annex-B slice headers exercise picture boundaries, not decoder validity.
    // HEVC type1/layer0/temporal_id_plus1=1; first_slice flag is the next high bit.
    private static readonly byte[] FirstSlice = [0x02, 0x01, 0x80, 0x80];
    private static readonly byte[] OtherSlice = [0x02, 0x01, 0x40, 0x80];
    private static readonly byte[] Sei = [0x4E, 0x01, 0x05, 0x80];
    private static readonly byte[] Vps = [0x40, 0x01, 0x80];
    private static readonly byte[] Sps = [0x42, 0x01, 0x80];
    private static readonly byte[] Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x80];

    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(nal => new byte[] { 0, 0, 0, 1 }.Concat(nal)).ToArray();
    private static VideoFrame Frame(byte[] bytes, uint timestamp = 0, bool keyframe = false) => new(VideoCodec.H265, keyframe, timestamp, null, bytes);
    private static VideoFrame Keyframe(uint timestamp = 0) => Frame(Annex(Vps, Sps, Pps, Idr), timestamp, true);
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    public static Task PictureBoundaries()
    {
        var hub = new StreamHub("synthetic-picture-boundaries");
        var (id, reader) = hub.Subscribe();
        try
        {
            byte[] bundled = Annex(Sei, FirstSlice, OtherSlice, Sei, FirstSlice);
            hub.PublishVideo(Frame(bundled));
            var one = hub.GetVideoBufferDiagnostics();
            Check(one.TotalAccessUnits == 2 && one.MultiAccessUnitBuffers == 1 && one.MaxAccessUnitsPerBuffer == 2,
                "two first slices must count two pictures, including a continuation slice and SEI");
            Check(reader.TryRead(out var delivered) && delivered is HubVideo video && ReferenceEquals(video.AnnexB, bundled)
                && !video.Keyframe, "diagnosis changed the original buffer or keyframe flag");
            Check(!reader.TryRead(out _), "diagnosis split the delivered stream");
            hub.PublishVideo(Frame(Annex(FirstSlice, OtherSlice), 50000));
            Check(hub.GetVideoBufferDiagnostics().TotalAccessUnits == 3, "two slices of one picture were counted twice");
            hub.PublishVideo(Frame(Annex(Sei), 100000));
            hub.PublishVideo(Frame(Annex(Vps, Sps, Pps, [0x46, 0x01, 0x80]), 150000));
            var final = hub.GetVideoBufferDiagnostics();
            Check(final.TotalAccessUnits == 3 && final.MultiAccessUnitBuffers == 1 && final.MaxVideoBufferBytes == bundled.Length,
                "metadata-only buffers invent pictures or change maxima");
            Check(hub.GetVideoDiagnostics().IncomingFrames == 4, "incoming buffer count no longer has its original meaning");
        }
        finally { hub.Unsubscribe(id); }
        return Task.CompletedTask;
    }

    public static Task KeyframeAndCache()
    {
        var clock = new ManualTimeProvider();
        var hub = new StreamHub("synthetic-keyframes", clock);
        Check(hub.GetVideoBufferDiagnostics().KeyframeAgeMs == null, "cold source invents a keyframe");
        var first = Keyframe();
        hub.PublishVideo(first);
        var initial = hub.GetVideoBufferDiagnostics();
        Check(initial.KeyframeCount == 1 && initial.KeyframeAgeMs == 0 && initial.GopBytes == first.Data.Length
            && initial.GopPackets == 1 && initial.GopBuffered, "timestamp-zero keyframe/cache not recorded");
        clock.Advance(TimeSpan.FromSeconds(3));
        byte[] predicted = Annex(FirstSlice);
        hub.PublishVideo(Frame(predicted, 50000));
        var aged = hub.GetVideoBufferDiagnostics();
        Check(aged.KeyframeAgeMs == 3000 && aged.GopPackets == 2 && aged.GopBytes == first.Data.Length + predicted.Length,
            "keyframe age was overwritten by a P-buffer");
        clock.WallClock = DateTimeOffset.UnixEpoch.AddYears(50);
        Check(hub.GetVideoBufferDiagnostics() == aged, "keyframe age depends on calendar time");
        hub.SourceStopped();
        var stopped = hub.GetVideoBufferDiagnostics();
        Check(!stopped.GopBuffered && stopped.GopBytes == 0 && stopped.GopPackets == 0 && stopped.KeyframeCount == 1
            && stopped.TotalAccessUnits == 2 && stopped.KeyframeAgeMs == 3000, "source stop incorrectly resets lifetime diagnosis");
        hub.PublishVideo(Keyframe(10));
        Check(hub.GetVideoBufferDiagnostics().KeyframeCount == 2 && hub.GetVideoBufferDiagnostics().KeyframeAgeMs == 0,
            "new session keyframe not counted");
        return Task.CompletedTask;
    }

    public static Task CacheEvictions()
    {
        var bytesHub = new StreamHub("synthetic-byte-cap");
        bytesHub.PublishVideo(Keyframe());
        var (id, reader) = bytesHub.Subscribe();
        try
        {
            var large = new byte[6 * 1024 * 1024];
            Array.Fill(large, (byte)0x55);
            large[0] = large[1] = large[2] = 0; large[3] = 1;
            large[4] = 0x02; large[5] = 0x01; large[6] = 0x80;
            bytesHub.PublishVideo(Frame(large, 50000));
            var evicted = bytesHub.GetVideoBufferDiagnostics();
            Check(evicted.GopCacheEvictions == 1 && !evicted.GopBuffered && evicted.GopBytes == 0 && bytesHub.LiveVideo,
                "oversized GOP must evict its cache without marking publisher offline");
            Check(reader.TryRead(out _) && reader.TryRead(out var delivered) && delivered is HubVideo video
                && ReferenceEquals(video.AnnexB, large), "cache eviction interrupted the existing subscriber");
            bytesHub.PublishVideo(Frame(Annex(FirstSlice), 100000));
            Check(bytesHub.GetVideoBufferDiagnostics().GopCacheEvictions == 1, "closed cache evicts repeatedly");
            bytesHub.PublishVideo(Keyframe(150000));
            Check(bytesHub.GetVideoBufferDiagnostics().GopBuffered && bytesHub.GetVideoBufferDiagnostics().GopCacheEvictions == 1,
                "next keyframe must reopen the cache without resetting eviction total");
        }
        finally { bytesHub.Unsubscribe(id); }
        var packetsHub = new StreamHub("synthetic-packet-cap");
        packetsHub.PublishVideo(Keyframe());
        for (uint i = 1; i < 900; i++) packetsHub.PublishVideo(Frame(Annex(FirstSlice), i * 50000));
        Check(packetsHub.GetVideoBufferDiagnostics().GopPackets == 900, "cache packet limit reached prematurely");
        packetsHub.PublishVideo(Frame(Annex(FirstSlice), 45000000));
        Check(packetsHub.GetVideoBufferDiagnostics().GopCacheEvictions == 1 && !packetsHub.HasBufferedGop,
            "packet cap eviction was not recorded");
        return Task.CompletedTask;
    }

    public static Task TimestampDeltas()
    {
        var hub = new StreamHub("synthetic-camera-timestamps");
        byte[] picture = Annex(FirstSlice);
        hub.PublishVideo(Frame(picture, uint.MaxValue - 10));
        Check(hub.GetVideoBufferDiagnostics().LastCameraTimestampDeltaUs == null, "first buffer invents a delta");
        hub.PublishVideo(Frame(picture, 20));
        var wrap = hub.GetVideoBufferDiagnostics();
        Check(wrap.LastCameraTimestampDeltaUs == 31 && wrap.MaxCameraTimestampDeltaUs == 31
            && wrap.CameraTimestampBackwardCandidates == 0, "normal clock wrap treated as a reversal");
        hub.PublishVideo(Frame(picture, 20));
        hub.PublishVideo(Frame(picture, 10));
        var backwards = hub.GetVideoBufferDiagnostics();
        Check(backwards.CameraTimestampZeroDeltas == 1 && backwards.CameraTimestampBackwardCandidates == 1
            && backwards.MaxCameraTimestampDeltaUs == 31, "zero or backwards candidate inflated valid delta maximum");
        hub.SourceStopped();
        hub.PublishVideo(Frame(picture, 0));
        Check(hub.GetVideoBufferDiagnostics().LastCameraTimestampDeltaUs == null
            && hub.GetVideoBufferDiagnostics().CameraTimestampBackwardCandidates == 1, "reconnect counted as a backwards camera clock");
        hub.PublishVideo(Frame(picture, 50));
        Check(hub.GetVideoBufferDiagnostics().LastCameraTimestampDeltaUs == 50 && hub.GetVideoBufferDiagnostics().MaxCameraTimestampDeltaUs == 50,
            "new session lost its timestamp diagnostics");
        return Task.CompletedTask;
    }

    public static async Task HttpMetrics()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(); int port = ((IPEndPoint)listener.LocalEndpoint).Port; listener.Stop();
        var clock = new ManualTimeProvider();
        var hub = new StreamHub("PRIVATE-SYNTHETIC-SOURCE", clock);
        byte[] bundled = Annex(Vps, Sps, Pps, Idr, OtherSlice, FirstSlice);
        hub.PublishInfo(new MediaInfo(1920, 1080, 20));
        hub.PublishVideo(Frame(bundled, 0, true));
        clock.Advance(TimeSpan.FromMilliseconds(250));
        const string user = "synthetic-user", password = "PRIVATE-SYNTHETIC-PASSWORD";
        var config = new BridgeOnvifConfig
        {
            Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Port = port,
            Name = "PRIVATE-SYNTHETIC-NAME", Model = "Synthetic diagnostics",
            Mac = "02:54:45:53:54:22", Uuid = "e2284fca-70d0-48e0-9af0-ce4caa244d1e", Discovery = false,
            Profiles = [new BridgeOnvifProfile { Stream = "mainStream", Codec = "H265", Width = 1920, Height = 1080, Fps = 20, Bitrate = 8192 }]
        };
        var endpoint = new ProtectOnvifServer(config, new Dictionary<string, string> { [user] = password },
            [new ProtectOnvifStream("main", "/synthetic/mainStream", hub)], 18554);
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task serving = endpoint.RunAsync(cancel.Token);
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}"), Timeout = TimeSpan.FromSeconds(2) };
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "/metrics");
            request.Headers.TryAddWithoutValidation("Authorization", "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + password)));
            using var response = await http.SendAsync(request);
            Check((int)response.StatusCode == 200, "buffer metrics requires the correct Basic login");
            string body = await response.Content.ReadAsStringAsync();
            using var document = JsonDocument.Parse(body);
            var metrics = document.RootElement.GetProperty("streams")[0];
            Check(metrics.GetProperty("totalAccessUnits").GetInt64() == 2 && metrics.GetProperty("multiAccessUnitBuffers").GetInt64() == 1
                && metrics.GetProperty("maxAccessUnitsPerBuffer").GetInt64() == 2, "HTTP AU metrics disagree with real hub");
            Check(metrics.GetProperty("keyframeAgeMs").GetDouble() == 250 && metrics.GetProperty("keyframeCount").GetInt64() == 1
                && metrics.GetProperty("maxVideoBufferBytes").GetInt64() == bundled.Length
                && metrics.GetProperty("gopBytes").GetInt32() == bundled.Length && metrics.GetProperty("gopBuffered").GetBoolean(), "HTTP keyframe/cache metrics incorrect");
            Check(!body.Contains(password) && !body.Contains(user) && !body.Contains(hub.Name) && !body.Contains(config.Name)
                && !body.Contains(Convert.ToBase64String(bundled)), "metrics reflects secrets or encoded media");
            Check(response.Headers.CacheControl?.NoStore == true, "diagnostics must not be cached");
        }
        finally { cancel.Cancel(); await serving; }
    }
}
