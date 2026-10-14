using System.Net;
using System.Net.Sockets;
using System.Text;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Streaming;

internal static class ProtectAudioListenTests
{
    private static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }
    private static void Key(StreamHub hub) => hub.PublishVideo(new(VideoCodec.H264, true, 0, null,
        [0, 0, 0, 1, 0x67, 66, 0, 30, 0, 0, 1, 0x68, 0xCE, 6, 0xE2, 0, 0, 1, 0x65, 1, 2, 3]));
    private static AacFrame Aac(byte marker = 0x55)
    {
        var data = new byte[71];
        data[0] = 0xFF; data[1] = 0xF1; data[2] = 0x60; data[3] = 0x40;
        data[4] = (byte)(data.Length >> 3); data[5] = (byte)((data.Length << 5) | 0x1F); data[6] = 0xFC;
        Array.Fill(data, marker, 7, data.Length - 7);
        return new(data);
    }
    private static async Task Until(Func<bool> predicate)
    {
        for (int i = 0; i < 200; i++) { if (predicate()) return; await Task.Delay(10); }
        throw new Exception("Audio subscription lifecycle deadline exceeded");
    }

    public static async Task AacBytesAndDecode()
    {
        byte[] input = await NativeTalkUdpTests.MakeAac();
        var units = Adts.Split(input);
        var hub = new StreamHub("synthetic-listen-framing"); Key(hub); hub.PublishAac(new(input));
        byte[] framed = units.SelectMany(unit => ProtectAudioListener.FrameAdts(hub.Audio!, unit.Data)).ToArray();
        var output = Adts.Split(framed);
        Check(units.Count == output.Count && units.Zip(output).All(pair => pair.First.Data.SequenceEqual(pair.Second.Data)),
            "ADTS reconstruction changed actual AAC access unit bytes");
        string[] decode = ["-hide_banner", "-loglevel", "error", "-f", "aac", "-i", "pipe:0", "-ar", "8000", "-ac", "1", "-f", "s16le", "pipe:1"];
        var original = await Ffmpeg.RunAsync(Ffmpeg.ExePath!, decode, [input], TimeSpan.FromSeconds(10), CancellationToken.None);
        var rebuilt = await Ffmpeg.RunAsync(Ffmpeg.ExePath!, decode, [framed], TimeSpan.FromSeconds(10), CancellationToken.None);
        Check(original.Out.Length > 16000 && original.Out.SequenceEqual(rebuilt.Out), "reconstructed listen audio decodes to different samples");
        Check(!ProtectAudioListener.CanFrameAac(new(true, 16000, 1, [0, 0])), "unsupported ASC was advertised as listenable");
        Console.WriteLine($"Listen AAC framing: all {units.Count} access units and {rebuilt.Out.Length / 2} decoded samples unchanged.");
    }

    public static async Task HttpAuthPayloadAndDisconnect()
    {
        var hub = new StreamHub("synthetic-listen-http"); Key(hub); hub.PublishAac(Aac());
        var reservation = new TcpListener(IPAddress.Loopback, 0); reservation.Start();
        int port = ((IPEndPoint)reservation.LocalEndpoint).Port; reservation.Stop();
        var config = new BridgeOnvifConfig
        {
            Bind = "127.0.0.1", AdvertisedHost = "127.0.0.1", Port = port,
            Mac = "02:00:00:00:00:20", Uuid = "956e9cbf-0a49-45ad-8fcb-c0a624b25230", Controls = true,
            Events = false, Discovery = false, GopPlayout = true,
            Profiles = [new() { Codec = "H264", Width = 1920, Height = 1080 }],
        };
        var server = new ProtectOnvifServer(config,
            new Dictionary<string, string> { ["listen-contract"] = "synthetic-listen-only" }, [new("main", "/synthetic/mainStream", hub)], 28554);
        Check((ProtectListenMetadata)server.ListenMetadata() is { Available: true, Profile: "main", Transcode: false },
            "main-only GOP incorrectly suppressed independent real microphone audio");
        using var stop = new CancellationTokenSource();
        var serving = server.RunAsync(stop.Token);
        async Task<(TcpClient Client, NetworkStream Stream, string Header)> Connect(bool authorized)
        {
            var client = new TcpClient(AddressFamily.InterNetwork); await client.ConnectAsync(IPAddress.Loopback, port);
            var stream = client.GetStream();
            string auth = authorized ? "Authorization: Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(
                "listen-contract:synthetic-listen-only")) + "\r\n" : "";
            await stream.WriteAsync(Encoding.ASCII.GetBytes("GET /control/listen HTTP/1.1\r\nHost: localhost\r\n" + auth + "\r\n"));
            var bytes = new List<byte>(); var one = new byte[1];
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            while (bytes.Count < 8192)
            {
                int n = await stream.ReadAsync(one, deadline.Token); if (n == 0) break;
                bytes.Add(one[0]);
                if (bytes.Count >= 4 && bytes.TakeLast(4).SequenceEqual(new byte[] { 13, 10, 13, 10 })) break;
            }
            return (client, stream, Encoding.ASCII.GetString(bytes.ToArray()));
        }
        try
        {
            var refused = await Connect(false); using (refused.Client)
                Check(refused.Header.StartsWith("HTTP/1.1 401") && hub.AudioSubscriberCount == 0,
                    "anonymous listen obtained audio or attached to the source");
            var live = await Connect(true);
            using (live.Client)
            {
                Check(live.Header.StartsWith("HTTP/1.1 200") && live.Header.Contains("Content-Type: audio/aac"),
                    "protected listen did not return honest audio MIME");
                await Until(() => hub.AudioSubscriberCount == 1);
                var source = Aac(0x33); hub.PublishAac(source);
                byte[] expected = ProtectAudioListener.FrameAdts(hub.Audio!, Adts.Split(source.Data).Single().Data);
                byte[] received = new byte[expected.Length];
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                await live.Stream.ReadExactlyAsync(received, deadline.Token);
                Check(received.SequenceEqual(expected), "actual HTTP listen changed or replayed the AAC source frame");
                Check(hub.LiveVideo && hub.Codec == VideoCodec.H264 && hub.ViewerCount == 1,
                    "audio-only listen changed the existing video source");
            }
            await Until(() => hub.AudioSubscriberCount == 0 && hub.ViewerCount == 0);
        }
        finally { stop.Cancel(); await serving.WaitAsync(TimeSpan.FromSeconds(3)); }
    }

    public static async Task BoundBackpressureAndEpoch()
    {
        var hub = new StreamHub("synthetic-listen-bounds"); Key(hub); hub.PublishAac(Aac());
        var (id, queue) = hub.SubscribeAudio(false);
        try
        {
            Check(!queue.TryRead(out _), "audio subscription primed stale GOP data");
            for (int i = 0; i < 100; i++) { Key(hub); hub.PublishAac(Aac((byte)i)); }
            int count = 0; byte last = 0;
            while (queue.TryRead(out var packet))
            { Check(packet is HubAudioAac, "audio queue retained video bytes"); count++; last = ((HubAudioAac)packet).Au[0]; }
            Check(count == 32 && last == 99, "audio queue grew without bound or retained stale frames instead of latest speech");
        }
        finally { hub.UnsubscribeAudio(id); }
        var blocked = new BlockedAudioOutput();
        var pump = new ProtectAudioListener(hub, hub.Audio!).PumpAsync(blocked, CancellationToken.None);
        await Until(() => hub.AudioSubscriberCount == 1); hub.PublishAac(Aac());
        await blocked.Entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        for (int i = 0; i < 500; i++) { Key(hub); hub.PublishAac(Aac()); }
        try { await pump.WaitAsync(TimeSpan.FromSeconds(3)); throw new Exception("slow audio writer never timed out"); }
        catch (OperationCanceledException) { }
        Check(hub.AudioSubscriberCount == 0 && hub.ViewerCount == 0 && hub.GetVideoDiagnostics().IncomingFrames >= 600,
            "slow listen blocked the publisher or leaked its subscription");
        using var output = new MemoryStream();
        var epochPump = new ProtectAudioListener(hub, hub.Audio!).PumpAsync(output, CancellationToken.None);
        await Until(() => hub.AudioSubscriberCount == 1); hub.SourceStopped();
        try { await epochPump.WaitAsync(TimeSpan.FromSeconds(2)); } catch (OperationCanceledException) { }
        Check(hub.AudioSubscriberCount == 0, "source stop left old microphone audio attached");
    }

    public static async Task PcmOnDemandAac()
    {
        Check(ProtectAudioListener.CanEncodeAac, "bundled ffmpeg AAC encoder unavailable");
        var hub = new StreamHub("synthetic-listen-pcm"); Key(hub);
        var encoder = new AdpcmEncoder();
        short[] tone = Enumerable.Range(0, 320).Select(i => (short)(Math.Sin(2 * Math.PI * 440 * i / 8000) * 6000)).ToArray();
        hub.PublishAdpcm(new(encoder.EncodeBlock(tone)));
        using var stop = new CancellationTokenSource();
        var output = new CollectedAudioOutput();
        var pump = new ProtectAudioListener(hub, hub.Audio!).PumpAsync(output, stop.Token);
        try
        {
            await Until(() => hub.AudioSubscriberCount == 1);
            for (int i = 0; i < 70; i++) { hub.PublishAdpcm(new(encoder.EncodeBlock(tone))); await Task.Delay(10); }
            await output.Produced.Task.WaitAsync(TimeSpan.FromSeconds(3)); // before EOF: live listening, not a completed clip
        }
        finally { stop.Cancel(); try { await pump.WaitAsync(TimeSpan.FromSeconds(3)); } catch (OperationCanceledException) { } }
        byte[] data = output.Snapshot(); var units = Adts.Split(data);
        Check(units.Count >= 5 && units.All(unit => unit.SampleRate == 22050 && unit.Channels == 1),
            "ADPCM source did not become actual playable AAC while the listener was active");
        Check(hub.AudioSubscriberCount == 0 && hub.ViewerCount == 0, "PCM listener kept its subscription or encoder after stop");
        Console.WriteLine($"Listen ADPCM: on-demand encoder produced {units.Count} AAC frames at 22050 Hz before EOF.");
    }

    private sealed class BlockedAudioOutput : MemoryStream
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        { Entered.TrySetResult(); await Task.Delay(Timeout.Infinite, ct); }
    }
    private sealed class CollectedAudioOutput : MemoryStream
    {
        private readonly object _gate = new();
        public TaskCompletionSource Produced { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        { lock (_gate) base.Write(buffer.Span); Produced.TrySetResult(); return ValueTask.CompletedTask; }
        public byte[] Snapshot() { lock (_gate) return ToArray(); }
    }
}
