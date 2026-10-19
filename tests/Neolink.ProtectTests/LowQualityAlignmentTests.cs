using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using Neolink.Config;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class LowQualityAlignmentTests
{
    private static readonly byte[] Vps = [0x40, 0x01, 0x80], Sps = [0x42, 0x01, 0x80], Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x91], Predicted = [0x02, 0x01, 0x80, 0x92];
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
    private static VideoFrame Key(uint us = 0) => new(VideoCodec.H265, true, us, null, Annex(Vps, Sps, Pps, Idr));
    private static void Check(bool yes, string message) { if (!yes) throw new Exception(message); }
    private static void Near(double actual, double expected, string message) => Check(Math.Abs(actual - expected) < 0.00001, message);
    private static HubVideo Video(long index, long sourceArrival, long outputArrival, bool key = true, long epoch = 0, byte[]? data = null) =>
        new(index, data ?? Annex(key ? Idr : Predicted), key, unchecked((uint)(1000 + index * 4500)))
        {
            SourceEpoch = epoch, ArrivalTimestamp = outputArrival, ArrivalTimestampFrequency = 1_000_000,
            SourceArrivalTimestamp = sourceArrival, SourceArrivalTimestampFrequency = 1_000_000
        };
    private static HubAudioAac Aac(long index, long arrival, long epoch = 0) => new(index, [0x12, 0x34, (byte)index], unchecked((uint)(5000 + index * 1024)))
        { SourceEpoch = epoch, ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000 };
    private static StreamHub Hub(string name, ManualTimeProvider clock)
    {
        var hub = new StreamHub(name, clock); hub.PublishInfo(new MediaInfo(672, 384, 20)); hub.PublishVideo(Key()); return hub;
    }
    private static bool Observe(HqPlayoutReference reference, string owner, long epoch, long arrival, long frequency, long presentation, long presentationFrequency)
    {
        Check(presentationFrequency == 1_000_000, "fixture presentation clock changed");
        var gop = new CompletedVideoGop([], TimeSpan.FromSeconds(2), epoch)
            { SourceArrivalTimestamp = arrival, SourceArrivalFrequency = frequency };
        return reference.Observe(owner, gop, presentation);
    }
    private static async Task RejectAsync(Func<Task> action, string message)
    {
        bool rejected = false;
        try { await action(); } catch (InvalidDataException) { rejected = true; }
        Check(rejected, message);
    }

    public static Task ConfigurationAndMountIsolation()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null && !File.Exists(Path.Combine(directory.FullName, "examples", "config.pilot.json"))) directory = directory.Parent;
        Check(directory != null, "pilot example missing");
        var original = JsonNode.Parse(File.ReadAllText(Path.Combine(directory!.FullName, "examples", "config.pilot.json")))!.AsObject();
        string temp = Path.Combine(Path.GetTempPath(), "neolink-lq-align-" + Guid.NewGuid().ToString("N") + ".json");
        NeolinkConfig Load(Action<JsonObject> configure)
        {
            var input = original.DeepClone().AsObject(); configure(input); File.WriteAllText(temp, input.ToJsonString()); return NeolinkConfig.Load(temp, strict: true);
        }
        void AddLow(JsonObject input)
        {
            input["onvif"]!["controls"] = true; input["onvif"]!["gop_playout"] = true; input["onvif"]!["gop_audio"] = true;
            input["onvif"]!["profiles"]!.AsArray().Add(new JsonObject
            { ["stream"] = "subStream", ["codec"] = "H265", ["width"] = 672, ["height"] = 384, ["fps"] = 20, ["bitrate"] = 512 });
            input["onvif"]!["low_quality_match_hq_delay"] = true;
        }
        try
        {
            Check(!Load(_ => { }).Onvif!.LowQualityMatchHqDelay, "default config introduces LQ delay");
            Check(Load(AddLow).Onvif!.LowQualityMatchHqDelay, "explicit LQ matching option was lost");
            foreach (Action<JsonObject> invalid in new Action<JsonObject>[]
            {
                input => input["onvif"]!["low_quality_match_hq_delay"] = true,
                input => { AddLow(input); input["onvif"]!["gop_playout"] = false; },
                input => { AddLow(input); input["onvif"]!["gop_audio"] = false; },
                input => { AddLow(input); input["onvif"]!["low_quality_transcode"] = "H265"; input["onvif"]!["low_quality_source_fps"] = 10; }
            })
            {
                bool rejected = false; try { Load(invalid); } catch (FormatException) { rejected = true; }
                Check(rejected, "LQ matching accepted missing HQ/audio/provenance or non-one-to-one encoder rates");
            }
        }
        finally { File.Delete(temp); }

        var clock = new ManualTimeProvider(); var main = Hub("main", clock); var low = Hub("low", clock);
        var permitted = new HashSet<string> { "u" };
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" }) { PlayoutTimeProvider = clock };
        ProtectBridgeHost.AddRtspMounts(server, "synthetic", main, permitted, true, 0, lowHub: low, gopAudio: true, lowQualityMatchHqDelay: true);
        var canonical = server.FindMount("/synthetic/mainStream")!; var alias = server.FindMount("/synthetic")!;
        var matched = server.FindMount("/synthetic/subStream")!; var direct = server.FindMount("/synthetic/subStream/direct")!;
        Check(ReferenceEquals(canonical.Hub, main) && ReferenceEquals(alias.Hub, main)
            && ReferenceEquals(canonical.PublishHqTiming, alias.PublishHqTiming) && canonical.PublishHqTiming != null,
            "HQ aliases do not share one timing reference");
        Check(ReferenceEquals(matched.MatchHqTiming, canonical.PublishHqTiming) && !matched.GopPlayout && matched.PlayoutDelayMs == 0,
            "LQ matching added a second GOP normalizer or static reserve");
        Check(ReferenceEquals(matched.Hub, low) && ReferenceEquals(direct.Hub, low)
            && direct.MatchHqTiming == null && direct.PublishHqTiming == null && !direct.GopPlayout && direct.PlayoutDelayMs == 0,
            "direct LQ alias changed source or acquired a playout delay");
        Check(new[] { canonical, alias, matched, direct }.All(m => ReferenceEquals(m.PermittedUsers, permitted)), "an alias lost original authentication policy");
        var unchanged = new RtspServer(new Dictionary<string, string>());
        ProtectBridgeHost.AddRtspMounts(unchanged, "old", main, permitted, true, 0, lowHub: low, gopAudio: true);
        Check(unchanged.FindMount("/old/subStream")!.MatchHqTiming == null && unchanged.FindMount("/old/mainStream")!.PublishHqTiming == null,
            "default mounts changed behavior without opt-in");
        return Task.CompletedTask;
    }

    public static Task SharedReferenceOwnershipAndExpiry()
    {
        var clock = new ManualTimeProvider(); var main = Hub("reference-main", clock);
        var reference = new HqPlayoutReference(main, clock);
        clock.Advance(TimeSpan.FromSeconds(2));
        Check(Observe(reference, "first", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "first HQ owner failed to publish");
        Check(reference.TryGetSnapshot(out var first), "new HQ reference unavailable"); Near(first.DelaySeconds, 2, "HQ observed duration changed");
        Check(Observe(reference, "second", main.SourceEpoch, 0, 1_000_000, 4_000_000, 1_000_000), "successor HQ observation was rejected");
        reference.Release("absent"); Check(reference.TryGetSnapshot(out var stable) && stable.Generation == first.Generation && stable.DelaySeconds == 2,
            "a later HQ viewer or non-owner release moved the active schedule");
        clock.WallClock = DateTimeOffset.UnixEpoch.AddYears(50);
        Check(reference.TryGetSnapshot(out var calendarStable) && calendarStable.Generation == first.Generation, "wall-clock adjustment invalidated monotonic timing");
        reference.Release("first"); Check(reference.TryGetSnapshot(out var successor) && successor.DelaySeconds == 4 && successor.Generation != first.Generation,
            "released HQ owner did not hand over to its fresh successor");
        reference.Release("second"); Check(!reference.TryGetSnapshot(out _), "last released HQ owner left a usable stale reference");
        Check(Observe(reference, "second", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "new owner could not establish a reference");
        Check(reference.TryGetSnapshot(out var next) && next.Generation != first.Generation, "owner replacement reused the old generation");
        main.SourceStopped(); Check(!reference.TryGetSnapshot(out _), "main reconnect retained old HQ timing");
        main.PublishVideo(Key());
        Check(!Observe(reference, "old-epoch", 0, 0, 1_000_000, 2_000_000, 1_000_000), "previous HQ source epoch was accepted");
        Check(Observe(reference, "fresh", main.SourceEpoch, 2_000_000, 1_000_000, 4_000_000, 1_000_000), "new HQ epoch could not recover");
        clock.Advance(TimeSpan.FromSeconds(13)); main.PublishVideo(Key());
        Check(!reference.TryGetSnapshot(out _), "fresh input kept an expired HQ presentation reference alive");
        var invalid = new HqPlayoutReference(main, clock);
        Check(!Observe(invalid, "bad", main.SourceEpoch, 0, 0, 2_000_000, 1_000_000)
            && !Observe(invalid, "bad", main.SourceEpoch, 0, 1_000_000, 11_000_000, 1_000_000), "invalid clock or excessive delay was accepted");
        return Task.CompletedTask;
    }

    public static async Task CommonClockAndEncoderProvenance()
    {
        var clock = new ManualTimeProvider(); var main = Hub("timing-main", clock); var low = Hub("timing-low", clock);
        low.PublishAac(new AacFrame([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
        var reference = new HqPlayoutReference(main, clock); clock.Advance(TimeSpan.FromSeconds(2));
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "HQ fixture unavailable");
        var waits = new List<TimeSpan>();
        Task Wait(TimeSpan delay, CancellationToken token) { token.ThrowIfCancellationRequested(); waits.Add(delay); clock.Advance(delay); return Task.CompletedTask; }
        var playout = new MatchedLowQualityPlayout(low, reference, clock, Wait);
        byte[] bytes = Annex(Vps, Sps, Pps, Idr); var video = Video(0, 1_000_000, 1_750_000, data: bytes);
        var audio = Aac(1, 1_064_000);
        long started = clock.GetTimestamp();
        await playout.WaitAsync(video, low.Audio, CancellationToken.None);
        Near(clock.GetElapsedTime(started).TotalSeconds, 1, "encoder time was added again instead of subtracted from the two-second HQ delay");
        await playout.WaitAsync(audio, low.Audio, CancellationToken.None);
        Near(clock.GetTimestamp() / 1_000_000d, 3.064, "original AAC did not use the same source-to-presentation offset as video");
        Check(ReferenceEquals(video.AnnexB, bytes) && video.RtpTs == 1000 && audio.RtpTs == 6024 && audio.Au.SequenceEqual(new byte[] { 0x12, 0x34, 1 }),
            "delay scheduling changed encoded payloads or media clocks");
        Check(waits.Count == 2 && waits.All(delay => delay > TimeSpan.Zero), "common scheduling used per-packet full delays or nonpositive waits");
        // Output arrival is a freshness measurement; source provenance remains a separate field through GOP replay.
        low.PublishVideo(Key(), 1_000_000, 1_000_000);
        var (id, reader) = low.Subscribe();
        try
        {
            Check(reader.TryRead(out var packet) && packet is HubVideo, "encoded LQ was not cached");
            var cached = (HubVideo)packet!;
            Check(cached.ArrivalTimestamp == clock.GetTimestamp() && cached.SourceArrivalTimestamp == 1_000_000
                && cached.SourceArrivalTimestampFrequency == 1_000_000, "encoder publication lost provenance or made source age the output freshness clock");
        }
        finally { low.Unsubscribe(id); }
        var provenance = new LowQualitySourceTimeline(20);
        var first = provenance.MapWithProvenance(Video(0, 0, 1_000_000) with { CameraMicroseconds = 8_670_000 }, 1)[0];
        var second = provenance.MapWithProvenance(Video(1, 0, 1_080_000) with { CameraMicroseconds = 17_340_000 }, 1)[0];
        Check(first.SourceArrivalTimestamp == 1_000_000 && first.SourceArrivalTimestampFrequency == 1_000_000
            && second.SourceArrivalTimestamp - first.SourceArrivalTimestamp == unchecked(second.Microseconds - first.Microseconds),
            "encoder provenance retained the unreliable camera origin or used a different elapsed clock from encoded RTP");
        var bundled = new LowQualitySourceTimeline(20).MapWithProvenance(Video(0, 0, 1_000_000), 4);
        Check(bundled.Select(value => value.SourceArrivalTimestamp).SequenceEqual(new long[] { 850_000, 900_000, 950_000, 1_000_000 })
            && bundled.Select(value => value.Microseconds).SequenceEqual(new uint[] { 11_111, 61_111, 111_111, 161_111 }),
            "initial multi-picture provenance extends into the future or changes the established encoded media timeline");
        var zeroReference = new HqPlayoutReference(main, clock);
        Check(Observe(zeroReference, "zero", main.SourceEpoch, clock.GetTimestamp(), 1_000_000, clock.GetTimestamp(), 1_000_000), "zero-delay HQ fixture unavailable");
        var zero = new MatchedLowQualityPlayout(low, zeroReference, clock, (_, _) => throw new Exception("zero-delay path introduced a wait"));
        await zero.WaitAsync(Video(0, clock.GetTimestamp(), clock.GetTimestamp()), low.Audio, CancellationToken.None);
    }

    public static async Task FailClosedAndCancellation()
    {
        var clock = new ManualTimeProvider(); var main = Hub("bounds-main", clock); var low = Hub("bounds-low", clock);
        var reference = new HqPlayoutReference(main, clock); clock.Advance(TimeSpan.FromSeconds(2));
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "HQ fixture unavailable");
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var cancelled = new MatchedLowQualityPlayout(low, reference, clock, waits.Wait);
        using var stop = new CancellationTokenSource(); Task pending = cancelled.WaitAsync(Video(0, 1_000_000, 1_500_000), low.Audio, stop.Token);
        var wait = await waits.Next(); stop.Cancel();
        bool didCancel = false; try { await pending.WaitAsync(TimeSpan.FromSeconds(1)); } catch (OperationCanceledException) { didCancel = true; }
        Check(didCancel && wait.Cancelled.Task.IsCompleted, "PAUSE/TEARDOWN failed to cancel delayed LQ promptly");

        var stale = new MatchedLowQualityPlayout(low, reference, clock, waits.Wait);
        Task stalePending = stale.WaitAsync(Video(0, 1_000_000, 1_500_000), low.Audio, CancellationToken.None);
        var staleWait = await waits.Next(); main.SourceStopped(); staleWait.Release();
        await RejectAsync(() => stalePending, "source reset emitted a previously queued LQ picture");

        long noReferenceStarted = clock.GetTimestamp();
        var noReference = new MatchedLowQualityPlayout(low, new HqPlayoutReference(main, clock), clock, (delay, token) =>
        {
            token.ThrowIfCancellationRequested(); clock.Advance(delay); low.PublishVideo(Key((uint)clock.GetTimestamp())); return Task.CompletedTask;
        });
        await RejectAsync(() => noReference.WaitAsync(Video(0, clock.GetTimestamp(), clock.GetTimestamp()), low.Audio, CancellationToken.None), "missing HQ reference silently served a differently delayed picture");
        Check(clock.GetElapsedTime(noReferenceStarted).TotalSeconds is >= 7.9 and <= 8.1, "missing initial HQ timing was not given a bounded startup grace period");
        main.PublishVideo(Key()); var fresh = new HqPlayoutReference(main, clock);
        Check(Observe(fresh, "fresh", main.SourceEpoch, clock.GetTimestamp(), 1_000_000, clock.GetTimestamp() + 2_000_000, 1_000_000), "fresh fixture unavailable");
        var huge = new MatchedLowQualityPlayout(low, fresh, clock);
        await RejectAsync(() => huge.WaitAsync(Video(0, clock.GetTimestamp(), clock.GetTimestamp(), data: new byte[6 * 1024 * 1024 + 1]), low.Audio, CancellationToken.None), "oversize pending payload bypassed the memory bound");
        var oldEpoch = new MatchedLowQualityPlayout(low, fresh, clock);
        await RejectAsync(() => oldEpoch.WaitAsync(Video(0, clock.GetTimestamp(), clock.GetTimestamp(), epoch: low.SourceEpoch + 1), low.Audio, CancellationToken.None), "wrong LQ epoch was accepted");
        var behind = new MatchedLowQualityPlayout(low, fresh, clock);
        await RejectAsync(() => behind.WaitAsync(Video(0, clock.GetTimestamp() - 3_000_000, clock.GetTimestamp()), low.Audio, CancellationToken.None), "a consumer more than its latency budget behind silently burst stale media");
        var changed = new MatchedLowQualityPlayout(low, fresh, clock, waits.Wait);
        Task ownerChanged = changed.WaitAsync(Video(0, clock.GetTimestamp(), clock.GetTimestamp()), low.Audio, CancellationToken.None);
        var ownerWait = await waits.Next(); fresh.Release("fresh");
        Check(Observe(fresh, "replacement", main.SourceEpoch, clock.GetTimestamp(), 1_000_000, clock.GetTimestamp() + 2_000_000, 1_000_000), "replacement fixture unavailable");
        ownerWait.Release();
        await RejectAsync(() => ownerChanged, "a queued LQ picture crossed an HQ timing generation change");
    }

    public static async Task DirectAndMatchedWirePayloads()
    {
        var clock = new ManualTimeProvider(); var main = Hub("wire-main", clock);
        clock.Advance(TimeSpan.FromSeconds(2)); var low = Hub("wire-low", clock);
        byte[] adts = [0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]; low.PublishAac(new AacFrame(adts));
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = waits.Wait };
        ProtectBridgeHost.AddRtspMounts(server, "synthetic", main, new() { "u" }, true, 0, lowHub: low, gopAudio: true, lowQualityMatchHqDelay: true);
        Check(Observe(server.FindMount("/synthetic")!.PublishHqTiming!, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "wire HQ fixture unavailable");
        await using var direct = await Wire.Start(server);
        Check((await direct.Command("DESCRIBE", "/synthetic/subStream/direct", authorized: false)).Status == 401, "direct LQ bypassed authentication");
        Check((await direct.Command("DESCRIBE", "/synthetic/subStream/direct")).Status == 200, "authenticated direct LQ unavailable");
        await direct.Setup("/synthetic/subStream/direct", 0); await direct.Setup("/synthetic/subStream/direct", 1);
        await direct.Command("PLAY", "/synthetic/subStream/direct"); var rawVideo = await direct.Video(); var rawAudio = await direct.Audio();
        Check(clock.GetTimestamp() == 2_000_000, "direct LQ alias waited for HQ timing");
        await using var matched = await Wire.Start(server);
        Check((await matched.Command("DESCRIBE", "/synthetic/subStream", authorized: false)).Status == 401, "matched LQ bypassed authentication");
        await matched.Setup("/synthetic/subStream", 0); await matched.Setup("/synthetic/subStream", 1);
        await matched.Command("PLAY", "/synthetic/subStream"); var gate = await waits.Next();
        Near(gate.Delay.TotalSeconds, 2, "matched wire did not apply measured HQ delay");
        gate.Release(); var delayedVideo = await matched.Video(); var delayedAudio = await matched.Audio();
        Check(rawVideo.Timestamp == delayedVideo.Timestamp && rawVideo.Nals.Select(Convert.ToHexString).SequenceEqual(delayedVideo.Nals.Select(Convert.ToHexString)),
            "LQ wire delay altered video RTP timestamps or encoded NALs");
        Check(rawAudio.Timestamp == delayedAudio.Timestamp && rawAudio.Payload.SequenceEqual(delayedAudio.Payload)
            && delayedAudio.Payload.SequenceEqual(new byte[] { 1, 2, 3, 4, 5 }), "LQ wire delay altered original AAC bytes/sample timestamps");
        await matched.Command("TEARDOWN", "/synthetic/subStream"); await direct.Command("TEARDOWN", "/synthetic/subStream/direct");
    }

    public static async Task SubscriberBackpressureFailsClosed()
    {
        var clock = new ManualTimeProvider(); var main = Hub("overflow-main", clock);
        clock.Advance(TimeSpan.FromSeconds(2)); var low = Hub("overflow-low", clock);
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = waits.Wait };
        ProtectBridgeHost.AddRtspMounts(server, "synthetic", main, new() { "u" }, true, 0, lowHub: low, gopAudio: true, lowQualityMatchHqDelay: true);
        Check(Observe(server.FindMount("/synthetic")!.PublishHqTiming!, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "overflow HQ fixture unavailable");
        await using var wire = await Wire.Start(server);
        await wire.Setup("/synthetic/subStream", 0); await wire.Command("PLAY", "/synthetic/subStream"); var gate = await waits.Next();
        // Hold the consumer's actual pacing await while the real bounded hub
        // channel drops packets. No synthetic index gap is injected into the pump.
        for (int i = 1; i <= 2100; i++) low.PublishVideo(new VideoFrame(VideoCodec.H265, false, (uint)(i * 50_000), null, Annex(Predicted)));
        gate.Release(); await wire.Video();
        Check(await wire.Closed(), "matched LQ emitted damaged media after subscriber backpressure instead of requiring a fresh keyframe/session");
        Check(low.VideoFrameCount == 2101, "local backpressure changed upstream frame accounting");
    }

    public static async Task SessionMediaPolicyIsolation()
    {
        var clock = new ManualTimeProvider(); var main = Hub("policy-main", clock);
        clock.Advance(TimeSpan.FromSeconds(2)); var low = Hub("policy-low", clock);
        byte[] adts = [0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5];
        main.PublishAac(new AacFrame(adts)); low.PublishAac(new AacFrame(adts));
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = waits.Wait };
        ProtectBridgeHost.AddRtspMounts(server, "synthetic", main, new() { "u" }, true, 0, lowHub: low, gopAudio: true, lowQualityMatchHqDelay: true);
        Check(Observe(server.FindMount("/synthetic")!.PublishHqTiming!, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "session policy HQ fixture unavailable");
        foreach (var (first, second) in new[]
        {
            ("/synthetic/subStream/direct", "/synthetic/subStream"),
            ("/synthetic/subStream", "/synthetic/subStream/direct")
        })
        {
            await using var wire = await Wire.Start(server);
            await wire.Setup(first, 0);
            Check((await wire.Command("SETUP", second + "/trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3\r\n")).Status == 455,
                "one RTSP session mixed direct and matched LQ tracks merely because both aliases share a hub");
            await wire.Setup(first, 1); // The refused cross-policy SETUP must not corrupt the original session.
            await wire.Command("TEARDOWN", first);
        }
        await using (var samePolicy = await Wire.Start(server))
        {
            await samePolicy.Setup("/synthetic", 0); await samePolicy.Setup("/synthetic/mainStream", 1);
            await samePolicy.Command("TEARDOWN", "/synthetic");
        }

        if (!Ffmpeg.SupportsOpus)
        { Console.WriteLine("SKIP direct Opus DESCRIBE policy transition: ffmpeg/libopus absent"); return; }
        await using var transition = await Wire.Start(server);
        var description = await transition.Command("DESCRIBE", "/synthetic/subStream/direct?audio=opus");
        Check(description.Status == 200 && description.Body.Contains("opus/48000/2"), "direct alias did not exercise a negotiated Opus description");
        Check((await transition.Command("SETUP", "/synthetic/subStream/trackID=0?audio=opus", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n")).Status == 404,
            "matched LQ accepted an explicit Opus request");
        await transition.Setup("/synthetic/subStream", 0); await transition.Setup("/synthetic/subStream", 1);
        await transition.Command("PLAY", "/synthetic/subStream"); var gate = await waits.Next(); gate.Release();
        await transition.Video(); var original = await transition.Audio();
        Check(original.Payload.SequenceEqual(new byte[] { 1, 2, 3, 4, 5 }), "direct Opus DESCRIBE leaked codec policy into matched LQ instead of preserving original AAC");
        await transition.Command("TEARDOWN", "/synthetic/subStream");
    }

    private sealed record Reply(int Status, string Body);
    private sealed record Frame(byte Channel, byte[] Data);
    private sealed record Picture(uint Timestamp, List<byte[]> Nals);
    private sealed record AudioUnit(uint Timestamp, byte[] Payload);
    private sealed class Wire : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly TcpClient _client = new();
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(20));
        private readonly Queue<Frame> _media = new();
        private NetworkStream _stream = null!; private Task _serving = Task.CompletedTask;
        private int _port, _sequence; private string? _session;
        public static async Task<Wire> Start(RtspServer server)
        {
            var wire = new Wire(); wire._listener.Start(); wire._port = ((IPEndPoint)wire._listener.LocalEndpoint).Port;
            wire._serving = Task.Run(async () => { using var peer = await wire._listener.AcceptTcpClientAsync(wire._stop.Token); await new RtspConnection(peer, server).RunAsync(wire._stop.Token); });
            await wire._client.ConnectAsync(IPAddress.Loopback, wire._port, wire._stop.Token); wire._stream = wire._client.GetStream(); return wire;
        }
        public async Task Setup(string path, int track) => Check((await Command("SETUP", path + "/trackID=" + track,
            $"Transport: RTP/AVP/TCP;unicast;interleaved={track * 2}-{track * 2 + 1}\r\n")).Status == 200, "LQ track SETUP failed");
        public async Task<Reply> Command(string method, string path, string extra = "", bool authorized = true)
        {
            string request = $"{method} rtsp://127.0.0.1:{_port}{path} RTSP/1.0\r\nCSeq: {++_sequence}\r\n"
                + (authorized ? "Authorization: Basic dTpw\r\n" : "") + (_session == null ? "" : $"Session: {_session}\r\n") + extra + "\r\n";
            await _stream.WriteAsync(Encoding.ASCII.GetBytes(request), _stop.Token);
            while (true)
            {
                byte first = (await Read(1))[0]; if (first == '$') { _media.Enqueue(await ReadFrame()); continue; }
                var head = new List<byte> { first };
                while (head.Count < 4 || !head.TakeLast(4).SequenceEqual(new byte[] { 13, 10, 13, 10 })) head.Add((await Read(1))[0]);
                string text = Encoding.ASCII.GetString(head.ToArray()); int length = 0;
                foreach (string line in text.Split("\r\n"))
                { if (line.StartsWith("Session:", StringComparison.OrdinalIgnoreCase)) _session = line[8..].Trim().Split(';')[0];
                  if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)) length = int.Parse(line[15..].Trim()); }
                string body = Encoding.ASCII.GetString(await Read(length)); return new Reply(int.Parse(text.Split(' ')[1]), body);
            }
        }
        private async Task<byte[]> Read(int length)
        { var data = new byte[length]; await _stream.ReadExactlyAsync(data, _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(4)); return data; }
        private async Task<Frame> ReadFrame()
        { byte[] head = await Read(3); return new(head[0], await Read((head[1] << 8) | head[2])); }
        private async Task<Frame> Next()
        { if (_media.TryDequeue(out var result)) return result; Check((await Read(1))[0] == '$', "expected RTSP interleaved media"); return await ReadFrame(); }
        public async Task<Picture> Video()
        {
            var nals = new List<byte[]>(); uint? timestamp = null;
            while (true)
            {
                var frame = await Next(); if (frame.Channel != 0) continue;
                uint ts = BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(4)); timestamp ??= ts; Check(ts == timestamp, "one LQ picture contains mixed RTP timestamps");
                nals.Add(frame.Data[12..]); if ((frame.Data[1] & 0x80) != 0) return new(timestamp.Value, nals);
            }
        }
        public async Task<AudioUnit> Audio()
        {
            while (true)
            { var frame = await Next(); if (frame.Channel != 2) continue; Check(frame.Data.Length >= 16, "AAC RTP packet lacks AU header");
              return new(BinaryPrimitives.ReadUInt32BigEndian(frame.Data.AsSpan(4)), frame.Data[16..]); }
        }
        public async Task<bool> Closed()
        {
            if (_media.Count != 0) return false;
            try { return await _stream.ReadAsync(new byte[1], _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(2)) == 0; }
            catch (IOException) { return true; }
        }
        public async ValueTask DisposeAsync()
        { _stop.Cancel(); _client.Dispose(); _listener.Stop(); try { await _serving.WaitAsync(TimeSpan.FromSeconds(3)); } catch (OperationCanceledException) { } finally { _stop.Dispose(); } }
    }
}
