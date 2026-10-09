using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Neolink.Config;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class GopPlayoutWireTests
{
    private static readonly byte[] Vps = [0x40, 0x01, 0x80], Sps = [0x42, 0x01, 0x80], Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x81], P = [0x02, 0x01, 0x80, 0x82];
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
    private static VideoFrame Key(uint microseconds = 0, byte tag = 0x81) =>
        new(VideoCodec.H265, true, microseconds, null, Annex(Vps, Sps, Pps, [0x26, 0x01, 0x80, tag]));
    private static void Check(bool value, string message) { if (!value) throw new Exception(message); }

    private static string Example()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory != null; directory = directory.Parent)
        {
            string path = Path.Combine(directory.FullName, "examples", "config.pilot.json");
            if (File.Exists(path)) return path;
        }
        throw new FileNotFoundException("Pilot example missing");
    }

    public static async Task ConfigAndMetadata()
    {
        var input = JsonNode.Parse(await File.ReadAllTextAsync(Example()))!.AsObject();
        string temp = Path.Combine(Path.GetTempPath(), "neolink-gop-test-" + Guid.NewGuid().ToString("N") + ".json");
        async Task<NeolinkConfig> Load()
        {
            await File.WriteAllTextAsync(temp, input.ToJsonString());
            return NeolinkConfig.Load(temp, strict: true);
        }
        try
        {
            var defaults = await Load();
            Check(defaults.Onvif is { GopPlayout: false, PlayoutDelayMs: 0 }, "default config activates a new behavior");
            foreach (int reserve in new[] { 0, 1500, 5000 })
            {
                input["onvif"]!["gop_playout"] = true; input["onvif"]!["playout_delay_ms"] = reserve;
                var config = await Load();
                Check(config.Onvif!.GopPlayout && config.Onvif.PlayoutDelayMs == reserve, "config loses GOP options");
                var server = new RtspServer(new Dictionary<string, string>());
                var hub = new StreamHub("synthetic-mapping");
                ProtectBridgeHost.AddRtspMounts(server, "synthetic", hub, new(), config.Onvif.GopPlayout, config.Onvif.PlayoutDelayMs);
                foreach (string path in new[] { "/synthetic", "/synthetic/mainStream" })
                {
                    var mount = server.FindMount(path);
                    Check(mount is { GopPlayout: true } && mount.PlayoutDelayMs == reserve && ReferenceEquals(mount.Hub, hub), "alias creates another source or loses playout policy");
                }
            }
            foreach (int reserve in new[] { -1, 5001 })
            {
                input["onvif"]!["playout_delay_ms"] = reserve;
                bool rejected = false;
                try { await Load(); } catch (FormatException) { rejected = true; }
                Check(rejected, "out of range reserve was accepted");
            }
        }
        finally { File.Delete(temp); }

        var clock = new ManualTimeProvider();
        var actual = new StreamHub("synthetic-arrival-metadata", clock);
        actual.PublishInfo(new MediaInfo(1920, 1080, 20));
        actual.PublishVideo(Key()); clock.Advance(TimeSpan.FromMilliseconds(60));
        actual.PublishVideo(new VideoFrame(VideoCodec.H265, false, 50000, null, Annex(P)));
        clock.Advance(TimeSpan.FromSeconds(2));
        var (id, reader) = actual.Subscribe();
        try
        {
            bool keyRead = reader.TryRead(out var k), pRead = reader.TryRead(out var p);
            Check(keyRead && pRead, "cached GOP not primed");
            var key = (HubVideo)k!; var predicted = (HubVideo)p!;
            Check(key.ArrivalTimestamp == 0 && predicted.ArrivalTimestamp == 60000 && key.ArrivalTimestampFrequency == 1000000,
                "cache rewrites original publisher time or uses TimeSpan ticks as frequency");
            Check(key.CameraMicroseconds == 0 && predicted.CameraMicroseconds == 50000,
                "original camera counter metadata was rewritten");
            actual.SourceStopped(); actual.PublishVideo(Key(100000));
            Check(reader.TryRead(out var resumed) && resumed is HubVideo v && v.SourceEpoch != key.SourceEpoch
                && v.ArrivalTimestamp == 2060000, "source reset does not mark a new generation/arrival");
        }
        finally { actual.Unsubscribe(id); }
    }

    public static async Task WireAliasesAndPictures()
    {
        foreach (string alias in new[] { "/synthetic", "/synthetic/mainStream" })
        {
            var clock = new ManualTimeProvider(); var waits = new ControlledWait(clock);
            var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new MediaInfo(1920, 1080, 20));
            byte[] bundle = Annex(Vps, Sps, Pps, [0x46, 0x01, 0x80], Idr, [0x50, 0x01, 0x05, 0x80],
                [0x4E, 0x01, 0x06, 0x80], P);
            byte[] large = new byte[2400]; Array.Fill(large, (byte)0xA5);
            large[0] = 0x02; large[1] = 0x01; large[2] = 0x80;
            hub.PublishVideo(new VideoFrame(VideoCodec.H265, true, 0, null, bundle));
            await using var wire = await Wire.Start(hub, true, 1500, clock, waits.Wait);
            await wire.Setup(alias);
            await wire.Command("PLAY", alias);
            await Viewers(hub, 1);
            // Only now does the final key arrive: no incomplete GOP may have been sent.
            clock.Advance(TimeSpan.FromMilliseconds(100));
            hub.PublishVideo(new VideoFrame(VideoCodec.H265, false, 100000, null, Annex(large)));
            clock.Advance(TimeSpan.FromMilliseconds(68)); hub.PublishVideo(Key(150000));
            var initial = await waits.Next();
            Check(initial.Delay == TimeSpan.FromMilliseconds(1500) && wire.BufferedRtp == 0, "lookahead/startup reserve lost");
            initial.Release();
            var one = await wire.Picture();
            (await waits.Next()).Release(); var two = await wire.Picture();
            (await waits.Next()).Release(); var three = await wire.Picture();
            Check(unchecked(two.Timestamp - one.Timestamp) == 5040 && unchecked(three.Timestamp - one.Timestamp) == 10080,
                "wire timestamps do not normalize 150ms source onto 168ms wall duration");
            var expected = H26x.SplitNals(bundle).Concat(H26x.SplitNals(Annex(large))).Select(n => Convert.ToHexString(n.Span)).ToArray();
            var received = one.Nals.Concat(two.Nals).Concat(three.Nals).Select(n => Convert.ToHexString(n)).ToArray();
            Check(expected.SequenceEqual(received), "RTP normalization changed, dropped or duplicated encoded NAL contents");
            Check(three.PacketCount > 1, "large AU fixture did not exercise FU packetization");
            await wire.Command("TEARDOWN", alias);
        }
    }

    public static async Task DefaultAndAudioScope()
    {
        var clock = new ManualTimeProvider(); var hub = new StreamHub("synthetic", clock);
        hub.PublishInfo(new MediaInfo(1920, 1080, 20)); hub.PublishVideo(Key());
        var (id, read) = hub.Subscribe(); read.TryRead(out var expected); hub.Unsubscribe(id);
        int waits = 0;
        Task NoWait(TimeSpan delay, CancellationToken ct) { Interlocked.Increment(ref waits); throw new Exception("default path called playout waiter"); }
        await using (var legacy = await Wire.Start(hub, false, 1500, clock, NoWait))
        {
            await legacy.Setup("/synthetic"); await legacy.Command("PLAY", "/synthetic");
            var picture = await legacy.Picture();
            Check(picture.Timestamp == ((HubVideo)expected!).RtpTs && waits == 0, "gop_playout false changes timestamps or adds reserve");
            Check((await legacy.Command("SETUP", "/synthetic/trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n")).Status == 455,
                "legacy audio behavior changed while playing");
        }
        var withAudio = new AudioHub(hub);
        Check(Sdp.Build(withAudio, "synthetic").Contains("m=audio"), "audio fixture has no baseline audio track");
        await using (var videoOnly = await Wire.Start(withAudio, true, 0, clock, NoWait))
        {
            var described = await videoOnly.Command("DESCRIBE", "/synthetic?audio=opus");
            Check(described.Status == 200 && described.Body.Contains("m=video") && !described.Body.Contains("m=audio"),
                "opt-in ?audio=opus probes ffmpeg or advertises an audio track");
            await videoOnly.Setup("/synthetic");
            Check((await videoOnly.Command("SETUP", "/synthetic/trackID=1?audio=opus", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n")).Status == 404,
                "video-only mount accepts audio SETUP");
            Check((await videoOnly.Command("SETUP", "/synthetic/trackID=2", "Transport: RTP/AVP/TCP;unicast;interleaved=4-5\r\n")).Status == 404,
                "video-only mount accepts audio backchannel");
            Check(withAudio.OpusRequests == 0, "video-only query/SETUP requested an audio transcoder");
        }
    }

    public static async Task CancelAndEpochRecovery()
    {
        var clock = new ManualTimeProvider(); var waits = new ControlledWait(clock);
        var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new MediaInfo(1920, 1080, 20)); hub.PublishVideo(Key());
        await using var wire = await Wire.Start(hub, true, 1500, clock, waits.Wait);
        await wire.Setup("/synthetic"); await wire.Command("PLAY", "/synthetic");
        await Viewers(hub, 1);
        clock.Advance(TimeSpan.FromMilliseconds(100)); hub.PublishVideo(Key(100000));
        var cancelled = await waits.Next();
        await wire.Command("PAUSE", "/synthetic");
        await cancelled.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Viewers(hub, 0);
        Check(wire.BufferedRtp == 0, "PAUSE sends a delayed GOP");
        await wire.Command("PLAY", "/synthetic");
        await Viewers(hub, 1);
        clock.Advance(TimeSpan.FromMilliseconds(100)); hub.PublishVideo(Key(200000));
        var stale = await waits.Next();
        hub.SourceStopped();
        stale.Release();
        // Old queued closure is discarded; only a complete new epoch may start.
        clock.Advance(TimeSpan.FromMilliseconds(100)); hub.PublishVideo(Key(300000, 0x91));
        clock.Advance(TimeSpan.FromMilliseconds(100)); hub.PublishVideo(Key(400000, 0x92));
        var fresh = await waits.Next();
        Check(fresh.Delay == TimeSpan.FromMilliseconds(1500), "source reset retained old playout schedule");
        fresh.Release(); var first = await wire.Picture();
        Check(first.Nals.Any(nal => nal.SequenceEqual(new byte[] { 0x26, 0x01, 0x80, 0x91 })),
            "epoch recovery replayed a stopped publisher's queued keyframe");
        // Closing the connection cancels a future wait without a reserve-length pause.
        clock.Advance(TimeSpan.FromMilliseconds(50));
        hub.PublishVideo(new VideoFrame(VideoCodec.H265, false, 450000, null, Annex(P)));
        clock.Advance(TimeSpan.FromMilliseconds(50)); hub.PublishVideo(Key(500000));
        var stopped = await waits.Next();
        await wire.Command("TEARDOWN", "/synthetic");
        await stopped.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    public static async Task QueueGapRecovery()
    {
        var clock = new ManualTimeProvider(); var waits = new ControlledWait(clock);
        var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new MediaInfo(1920, 1080, 20)); hub.PublishVideo(Key());
        await using var wire = await Wire.Start(hub, true, 1500, clock, waits.Wait);
        await wire.Setup("/synthetic"); await wire.Command("PLAY", "/synthetic"); await Viewers(hub, 1);
        clock.Advance(TimeSpan.FromMilliseconds(50)); hub.PublishVideo(Key(50000));
        var firstWait = await waits.Next();
        for (int i = 1; i <= 2100; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(1));
            hub.PublishVideo(new VideoFrame(VideoCodec.H265, false, (uint)(50000 + i * 1000), null, Annex(P)));
        }
        clock.Advance(TimeSpan.FromMilliseconds(50)); hub.PublishVideo(Key(2200000, 0x91));
        clock.Advance(TimeSpan.FromMilliseconds(50)); hub.PublishVideo(Key(2250000, 0x92));
        firstWait.Release(); var first = await wire.Picture();
        var recovered = await waits.Next(); recovered.Release(); var next = await wire.Picture();
        Check(next.Nals.Any(n => n.SequenceEqual(new byte[] { 0x26, 0x01, 0x80, 0x91 }))
            && unchecked(next.Timestamp - first.Timestamp) >= 135000,
            "DropOldest gap sent a partial GOP or failed to rebase at a complete fresh keyframe");
        Check(hub.GetVideoDiagnostics().IncomingFrames == 2104, "local subscriber recovery modified source frame accounting");
    }

    public static async Task RawClockJumpPictures()
    {
        var clock = new ManualTimeProvider(); var waits = new ControlledWait(clock);
        var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new MediaInfo(1920, 1080, 20)); hub.PublishVideo(Key());
        await using var wire = await Wire.Start(hub, true, 2500, clock, waits.Wait);
        await wire.Setup("/synthetic"); await wire.Command("PLAY", "/synthetic"); await Viewers(hub, 1);
        clock.Advance(TimeSpan.FromMilliseconds(50));
        hub.PublishVideo(new VideoFrame(VideoCodec.H265, false, 50000, null, Annex(P)));
        clock.Advance(TimeSpan.FromMilliseconds(2145));
        byte[] afterJump = [0x02, 0x01, 0x80, 0x83];
        hub.PublishVideo(new VideoFrame(VideoCodec.H265, false, 12_177_668, null, Annex(afterJump)));
        clock.Advance(TimeSpan.FromMilliseconds(50)); hub.PublishVideo(Key(12_227_668));
        var reserve = await waits.Next();
        Check(reserve.Delay == TimeSpan.FromMilliseconds(2500), "configured fallback reserve was not applied");
        reserve.Release(); var first = await wire.Picture();
        (await waits.Next()).Release(); var second = await wire.Picture();
        (await waits.Next()).Release(); var third = await wire.Picture();
        Check(unchecked(second.Timestamp - first.Timestamp) == 67350
            && unchecked(third.Timestamp - second.Timestamp) == 67350,
            "raw-clock fallback does not distribute all three pictures over the measured 2.245s GOP");
        Check(second.Nals.Count == 1 && second.Nals[0].SequenceEqual(P)
            && third.Nals.Count == 1 && third.Nals[0].SequenceEqual(afterJump),
            "raw camera jump discarded or rewrote a valid encoded picture");
        var source = hub.GetVideoDiagnostics(); var raw = hub.GetVideoBufferDiagnostics();
        Check(source.IncomingFrames == 4 && raw.TotalAccessUnits == 4 && raw.MaxCameraTimestampDeltaUs == 12_127_668,
            "output smoothing changed original source diagnostics");
    }

    private static async Task Viewers(StreamHub hub, int count)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (hub.ViewerCount != count) await Task.Delay(5, limit.Token);
    }

    private sealed class AudioHub(StreamHub inner) : IStreamHub
    {
        public int OpusRequests;
        public string Name => inner.Name;
        public int SubscriberCount => inner.SubscriberCount;
        public int ViewerCount => inner.ViewerCount;
        public bool VideoReady => inner.VideoReady;
        public bool LiveVideo => inner.LiveVideo;
        public bool HasBufferedGop => inner.HasBufferedGop;
        public VideoCodec? Codec => inner.Codec;
        public byte[]? Sps => inner.Sps;
        public byte[]? Pps => inner.Pps;
        public byte[]? Vps => inner.Vps;
        public uint Width => inner.Width;
        public uint Height => inner.Height;
        public AudioTrackInfo? Audio => new(true, 16000, 1, [0x14, 0x08]);
        public DateTime LastViewerAskUtc => inner.LastViewerAskUtc;
        public long SourceEpoch => inner.SourceEpoch;
        public (Guid id, ChannelReader<HubPacket> reader) Subscribe(bool viewer = false) => inner.Subscribe(viewer);
        public void Unsubscribe(Guid id) => inner.Unsubscribe(id);
        public Task<bool> WaitForDescribeInfoAsync(TimeSpan timeout, CancellationToken ct) => Task.FromResult(VideoReady);
        public void AcquireOpus() => Interlocked.Increment(ref OpusRequests);
    }

    internal sealed class ControlledWait(ManualTimeProvider clock)
    {
        private readonly Channel<Pending> _pending = Channel.CreateUnbounded<Pending>();
        public async Task Wait(TimeSpan delay, CancellationToken ct)
        {
            var pending = new Pending(delay, clock); _pending.Writer.TryWrite(pending);
            try { await pending.Gate.Task.WaitAsync(ct); }
            catch (OperationCanceledException) { pending.Cancelled.TrySetResult(); throw; }
        }
        public async Task<Pending> Next() => await _pending.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(4));
        internal sealed class Pending(TimeSpan delay, ManualTimeProvider clock)
        {
            public TimeSpan Delay { get; } = delay;
            public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public void Release()
            {
                // A fake timer must wake at or after the due instant. Truncating
                // a fractional 90-kHz offset to this clock's 1-us granularity
                // can otherwise leave a sub-tick remainder that never elapses.
                clock.AdvanceTimestampTicks((long)Math.Ceiling(Delay.Ticks / 10d));
                Gate.TrySetResult();
            }
        }
    }

    private sealed record Response(int Status, string Headers, string Body);
    private sealed record VideoPicture(uint Timestamp, List<byte[]> Nals, int PacketCount);
    private sealed class Wire : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(30));
        private readonly TcpClient _client = new();
        private readonly Queue<byte[]> _rtp = new();
        private Task _serving = Task.CompletedTask;
        private NetworkStream _stream = null!;
        private int _sequence;
        private string? _session;
        private int _port;
        public int BufferedRtp => _rtp.Count;
        public static async Task<Wire> Start(IStreamHub hub, bool gop, int reserve, ManualTimeProvider clock,
            Func<TimeSpan, CancellationToken, Task> wait)
        {
            var wire = new Wire(); wire._listener.Start(); wire._port = ((IPEndPoint)wire._listener.LocalEndpoint).Port;
            var server = new RtspServer(new Dictionary<string, string> { ["synthetic"] = "synthetic-password" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = wait };
            ProtectBridgeHost.AddRtspMounts(server, "synthetic", hub, new() { "synthetic" }, gop, reserve);
            wire._serving = wire.Accept(server);
            await wire._client.ConnectAsync(IPAddress.Loopback, wire._port, wire._stop.Token);
            wire._stream = wire._client.GetStream();
            return wire;
        }
        private async Task Accept(RtspServer server)
        {
            using var accepted = await _listener.AcceptTcpClientAsync(_stop.Token);
            await new RtspConnection(accepted, server).RunAsync(_stop.Token);
        }
        public async Task Setup(string path)
        {
            var response = await Command("SETUP", path + "/trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n");
            Check(response.Status == 200, "video SETUP failed");
        }
        public async Task<Response> Command(string method, string path, string extra = "")
        {
            int sequence = ++_sequence;
            string request = $"{method} rtsp://127.0.0.1:{_port}{path} RTSP/1.0\r\nCSeq: {sequence}\r\n"
                + "Authorization: Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("synthetic:synthetic-password")) + "\r\n"
                + (_session == null ? "" : $"Session: {_session}\r\n") + extra + "\r\n";
            await _stream.WriteAsync(Encoding.ASCII.GetBytes(request), _stop.Token);
            while (true)
            {
                byte first = (await Read(1))[0];
                if (first == '$') { _rtp.Enqueue(await Rtp()); continue; }
                var header = new List<byte> { first };
                while (header.Count < 4 || !header.TakeLast(4).SequenceEqual(new byte[] { 13, 10, 13, 10 }))
                    header.Add((await Read(1))[0]);
                string text = Encoding.ASCII.GetString(header.ToArray());
                int length = 0;
                foreach (string line in text.Split("\r\n"))
                {
                    if (line.StartsWith("Session:", StringComparison.OrdinalIgnoreCase)) _session = line[8..].Trim().Split(';')[0];
                    if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)) length = int.Parse(line[15..].Trim());
                }
                return new Response(int.Parse(text.Split(' ')[1]), text, Encoding.ASCII.GetString(await Read(length)));
            }
        }
        private async Task<byte[]> Read(int length)
        {
            var bytes = new byte[length];
            await _stream.ReadExactlyAsync(bytes, _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(4));
            return bytes;
        }
        private async Task<byte[]> Rtp()
        {
            byte[] rest = await Read(3);
            Check(rest[0] == 0, "unexpected interleaved channel");
            return await Read((rest[1] << 8) | rest[2]);
        }
        public async Task<VideoPicture> Picture()
        {
            var nals = new List<byte[]>(); List<byte>? fragmented = null;
            uint? timestamp = null; int count = 0;
            while (true)
            {
                byte[] packet;
                if (_rtp.TryDequeue(out var queued)) packet = queued;
                else { Check((await Read(1))[0] == '$', "expected an interleaved RTP frame"); packet = await Rtp(); }
                count++;
                uint current = BinaryPrimitives.ReadUInt32BigEndian(packet.AsSpan(4, 4));
                timestamp ??= current;
                Check(current == timestamp, "one picture's FU/NAL packets have differing timestamps");
                var payload = packet.AsSpan(12).ToArray();
                if (((payload[0] >> 1) & 63) == 49)
                {
                    if ((payload[2] & 0x80) != 0) fragmented = [(byte)((payload[0] & 0x81) | ((payload[2] & 63) << 1)), payload[1]];
                    Check(fragmented != null, "HEVC FU lacks its start"); fragmented!.AddRange(payload.Skip(3));
                    if ((payload[2] & 0x40) != 0) { nals.Add(fragmented.ToArray()); fragmented = null; }
                }
                else nals.Add(payload);
                if ((packet[1] & 0x80) != 0)
                {
                    Check(fragmented == null, "marker terminates an incomplete FU");
                    return new VideoPicture(timestamp.Value, nals, count);
                }
            }
        }
        public async ValueTask DisposeAsync()
        {
            _stop.Cancel(); _client.Dispose(); _listener.Stop();
            try { await _serving.WaitAsync(TimeSpan.FromSeconds(3)); }
            catch (OperationCanceledException) { }
            finally { _stop.Dispose(); }
        }
    }
}
