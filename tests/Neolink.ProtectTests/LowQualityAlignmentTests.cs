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
        await TimerResolutionAndClockFailure();
        await PublicationOrderedAudioAndEncodedVideo();
        await EarlierVideoArrivingDuringAudioWait();
        await SmallReferenceMovementPreservesTrackOrder();
        await AudioOnlyReaderPreservesSampleClock();
    }

    private static async Task PublicationOrderedAudioAndEncodedVideo()
    {
        var clock = new ManualTimeProvider(); var main = Hub("flow-main", clock); var low = Hub("flow-low", clock);
        low.PublishAac(new AacFrame([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
        clock.AdvanceTimestampTicks(300_000);
        var reference = new HqPlayoutReference(main, clock);
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "flow HQ reference unavailable");
        var publications = new List<(long At, HubPacket Packet)>();
        // Native AAC arrives immediately. Each corresponding encoded picture is
        // published 300 ms later, so publication order differs from AV due order.
        for (int frame = 0; frame <= 100; frame++)
        {
            long source = frame * 50_000L;
            publications.Add((source + 300_000, Video(-1, source, source + 300_000, key: frame == 0)
                with { RtpTs = (uint)(1000 + frame * 4500) }));
        }
        for (int unit = 5; unit * 64_000L <= 5_000_000; unit++)
        {
            long source = unit * 64_000L;
            publications.Add((source, Aac(-1, source) with { RtpTs = (uint)(5000 + unit * 1024) }));
        }
        var input = Channel.CreateUnbounded<HubPacket>();
        var expected = new Dictionary<long, HubPacket>();
        long index = 0;
        foreach (var publication in publications.OrderBy(value => value.At))
        {
            HubPacket packet = publication.Packet switch
            {
                HubVideo video => video with { Index = index },
                HubAudioAac audio => audio with { Index = index },
                _ => throw new Exception("unexpected flow fixture")
            };
            expected.Add(index++, packet); input.Writer.TryWrite(packet);
        }
        input.Writer.Complete();
        Task Advance(TimeSpan delay, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested(); clock.Advance(delay);
            // Keep the independent upstream sources alive over the five-second
            // media interval; neither queue consumption nor old cached data does so.
            main.PublishVideo(Key((uint)clock.GetTimestamp())); low.PublishVideo(Key((uint)clock.GetTimestamp()));
            Check(Observe(reference, "hq", main.SourceEpoch, clock.GetTimestamp(), 1_000_000,
                clock.GetTimestamp() + 2_000_000, 1_000_000), "flow HQ refresh failed");
            return Task.CompletedTask;
        }
        var playout = new MatchedLowQualityPlayout(low, reference, clock, Advance);
        var seen = new HashSet<long>(); var videoRtp = new List<uint>(); var audioRtp = new List<uint>();
        long firstVideoAt = -1, lastVideoAt = -1;
        await foreach (HubPacket packet in playout.ReadAsync(input.Reader, low.Audio, CancellationToken.None))
        {
            Check(seen.Add(packet.Index) && expected.TryGetValue(packet.Index, out _), "AV scheduling duplicated or invented a packet");
            HubPacket original = expected[packet.Index]; long source;
            if (packet is HubVideo video && original is HubVideo originalVideo)
            {
                Check(ReferenceEquals(video.AnnexB, originalVideo.AnnexB) && video.RtpTs == originalVideo.RtpTs,
                    "publication-order scheduling rewrote encoded NALs or video RTP");
                videoRtp.Add(video.RtpTs); source = video.SourceArrivalTimestamp!.Value;
                if (firstVideoAt < 0) firstVideoAt = clock.GetTimestamp(); lastVideoAt = clock.GetTimestamp();
            }
            else if (packet is HubAudioAac audio && original is HubAudioAac originalAudio)
            {
                Check(ReferenceEquals(audio.Au, originalAudio.Au) && audio.RtpTs == originalAudio.RtpTs,
                    "publication-order scheduling rewrote original AAC or sample RTP");
                audioRtp.Add(audio.RtpTs); source = audio.ArrivalTimestamp!.Value;
            }
            else throw new Exception("AV scheduling changed a packet type");
            long late = clock.GetTimestamp() - source - 2_000_000;
            Check(late is >= 0 and <= 1000, "interleaved AAC blocked encoded video past its observed HQ deadline");
            playout.MarkSent(packet);
        }
        Check(seen.Count == expected.Count && videoRtp.Count == 101 && audioRtp.Count >= 70,
            "five-second publication-ordered flow lost audio or video");
        Check(lastVideoAt - firstVideoAt >= 5_000_000, "AV regression did not cover five seconds of delivered media");
        Check(videoRtp.Zip(videoRtp.Skip(1), (a, b) => unchecked(b - a)).All(step => step == 4500)
            && audioRtp.Zip(audioRtp.Skip(1), (a, b) => unchecked(b - a)).All(step => step == 1024),
            "AV scheduling reordered a track or changed AAC's 1024-sample clock");
    }

    private static async Task EarlierVideoArrivingDuringAudioWait()
    {
        var clock = new ManualTimeProvider(); var main = Hub("read-wait-main", clock); var low = Hub("read-wait-low", clock);
        low.PublishAac(new AacFrame([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
        var reference = new HqPlayoutReference(main, clock); clock.AdvanceTimestampTicks(300_000);
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "read-wait HQ reference unavailable");
        var input = Channel.CreateUnbounded<HubPacket>(); var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var playout = new MatchedLowQualityPlayout(low, reference, clock, waits.Wait);
        await using var output = playout.ReadAsync(input.Reader, low.Audio, CancellationToken.None).GetAsyncEnumerator();
        input.Writer.TryWrite(Video(0, 0, 300_000));
        Task<bool> initial = output.MoveNextAsync().AsTask(); var initialWait = await waits.Next(); initialWait.Release();
        Check(await initial.WaitAsync(TimeSpan.FromSeconds(2)) && output.Current is HubVideo { Index: 0 }, "initial scheduled keyframe missing");
        Check(System.Text.Json.JsonSerializer.SerializeToNode(playout.Diagnostics())!["videoPackets"]!.GetValue<long>() == 0,
            "scheduler counted a yielded packet as delivered before the sender acknowledged it");
        playout.MarkSent(output.Current);
        Check(System.Text.Json.JsonSerializer.SerializeToNode(playout.Diagnostics())!["videoPackets"]!.GetValue<long>() == 1,
            "sender acknowledgement did not advance the delivered-video counter");
        var audio = Aac(1, 400_000); input.Writer.TryWrite(audio);
        Task<bool> next = output.MoveNextAsync().AsTask(); var audioWait = await waits.Next();
        Near(audioWait.Delay.TotalSeconds, 0.4, "AAC fixture did not establish a later waiting deadline");
        var earlierVideo = Video(2, 100_000, 400_000, key: false); input.Writer.TryWrite(earlierVideo);
        var videoWait = await waits.Next();
        Check(audioWait.Cancelled.Task.IsCompleted, "earlier input failed to cancel the obsolete AAC timer");
        Near(videoWait.Delay.TotalSeconds, 0.1, "newly arrived earlier video did not replace the later AAC wait");
        videoWait.Release();
        Check(await next.WaitAsync(TimeSpan.FromSeconds(2)) && ReferenceEquals(output.Current, earlierVideo),
            "reader stopped ingesting while audio waited and emitted AAC ahead of earlier encoded video");
        playout.MarkSent(output.Current);
        next = output.MoveNextAsync().AsTask(); var remainingAudio = await waits.Next(); remainingAudio.Release();
        Check(await next.WaitAsync(TimeSpan.FromSeconds(2)) && ReferenceEquals(output.Current, audio), "deferred original AAC was lost after earlier video");
        playout.MarkSent(output.Current); input.Writer.Complete();
        Check(!await output.MoveNextAsync(), "completed scheduler retained an extra packet");
    }

    private static async Task SmallReferenceMovementPreservesTrackOrder()
    {
        var clock = new ManualTimeProvider(); var main = Hub("fifo-main", clock); var low = Hub("fifo-low", clock);
        clock.AdvanceTimestampTicks(2_000_000); var reference = new HqPlayoutReference(main, clock);
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "FIFO HQ reference unavailable");
        var input = Channel.CreateUnbounded<HubPacket>();
        var first = Video(0, 1_000_000, 1_300_000); var second = Video(1, 1_050_000, 1_350_000, key: false);
        input.Writer.TryWrite(first); input.Writer.TryWrite(second); input.Writer.Complete();
        var reader = new BeforeSecondPacketReader(input.Reader, () =>
            Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 1_900_000, 1_000_000), "small HQ delay update rejected"));
        Task Advance(TimeSpan delay, CancellationToken ct) { ct.ThrowIfCancellationRequested(); clock.Advance(delay); return Task.CompletedTask; }
        var playout = new MatchedLowQualityPlayout(low, reference, clock, Advance); var received = new List<HubPacket>();
        await foreach (HubPacket packet in playout.ReadAsync(reader, null, CancellationToken.None))
        { received.Add(packet); playout.MarkSent(packet); }
        Check(received.Count == 2 && ReferenceEquals(received[0], first) && ReferenceEquals(received[1], second),
            "a benign 100-ms HQ movement reordered 50-ms video frames by their individual deadlines");
        Check(clock.GetTimestamp() is >= 3_000_000 and <= 3_001_000, "track FIFO rewrote deadlines or extended a small reference movement");
    }

    private sealed class BeforeSecondPacketReader(ChannelReader<HubPacket> inner, Action beforeSecond) : ChannelReader<HubPacket>
    {
        private int _reads;
        public override Task Completion => inner.Completion;
        public override bool TryRead(out HubPacket item)
        {
            bool read = inner.TryRead(out item!);
            if (read && ++_reads == 2) beforeSecond();
            return read;
        }
        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => inner.WaitToReadAsync(cancellationToken);
    }

    private static async Task AudioOnlyReaderPreservesSampleClock()
    {
        var clock = new ManualTimeProvider(); var main = Hub("audio-only-main", clock); var low = Hub("audio-only-low", clock);
        low.PublishAac(new AacFrame([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
        clock.AdvanceTimestampTicks(2_000_000); var reference = new HqPlayoutReference(main, clock);
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "audio-only HQ reference unavailable");
        var input = Channel.CreateUnbounded<HubPacket>(); var first = Aac(0, 1_000_000);
        var second = Aac(2, 1_064_000) with { RtpTs = first.RtpTs + 1024 };
        input.Writer.TryWrite(first); input.Writer.TryWrite(Video(1, 1_000_000, 1_300_000));
        input.Writer.TryWrite(second); input.Writer.Complete();
        Task Advance(TimeSpan delay, CancellationToken ct) { ct.ThrowIfCancellationRequested(); clock.Advance(delay); return Task.CompletedTask; }
        var playout = new MatchedLowQualityPlayout(low, reference, clock, Advance); var seen = new List<HubPacket>();
        await foreach (var packet in playout.ReadAsync(input.Reader, low.Audio, CancellationToken.None, videoEnabled: false))
        { seen.Add(packet); playout.MarkSent(packet); }
        Check(seen.Count == 2 && ReferenceEquals(seen[0], first) && ReferenceEquals(seen[1], second),
            "audio-only SETUP waited for a video keyframe or lost AAC across an ignored video packet");
        Check(clock.GetTimestamp() is >= 3_064_000 and <= 3_065_000, "audio-only scheduling changed the original 1024-sample AAC phase");
    }

    private static async Task TimerResolutionAndClockFailure()
    {
        var clock = new ManualTimeProvider(); var main = Hub("timer-main", clock); var low = Hub("timer-low", clock);
        clock.AdvanceTimestampTicks(2_000_000);
        var reference = new HqPlayoutReference(main, clock);
        Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "timer HQ fixture unavailable");
        var waits = new List<TimeSpan>();
        Task FloorTimer(TimeSpan delay, CancellationToken token)
        {
            token.ThrowIfCancellationRequested(); waits.Add(delay);
            // Model the real Task.Delay timeout conversion: discard fractional
            // milliseconds, with a zero-ms timeout completing synchronously.
            clock.AdvanceTimestampTicks((long)Math.Floor(delay.TotalMilliseconds) * 1000);
            return Task.CompletedTask;
        }
        var timed = new MatchedLowQualityPlayout(low, reference, clock, FloorTimer);
        // The former loop waited 1000.375 ms, then repeatedly requested 0.375 ms.
        // This floor-timer could never advance that remainder and tripped at 64.
        const long source = 1_000_375, due = source + 2_000_000;
        await timed.WaitAsync(Video(0, source, 1_200_375), low.Audio, CancellationToken.None);
        Check(waits.Count == 1 && waits[0] == TimeSpan.FromMilliseconds(1001), "positive wait was not rounded up to the timer's millisecond resolution");
        Check(clock.GetTimestamp() >= due && clock.GetTimestamp() - due < 1000,
            "timer rounding returned early or added more than one millisecond beyond the presentation deadline");

        int stalledWaits = 0;
        var stalled = new MatchedLowQualityPlayout(low, reference, clock, (_, token) =>
        { token.ThrowIfCancellationRequested(); stalledWaits++; return Task.CompletedTask; });
        bool bounded = false;
        try { await stalled.WaitAsync(Video(0, clock.GetTimestamp() - 1_000_000, clock.GetTimestamp()), low.Audio, CancellationToken.None); }
        catch (InvalidDataException error) { bounded = error.Message.Contains("clock failed to advance", StringComparison.Ordinal); }
        Check(bounded && stalledWaits == 64, "a genuinely nonadvancing clock escaped the existing bounded-wait guard");
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
        await ScheduledReaderBoundsAndDiscontinuities();
    }

    private static async Task ScheduledReaderBoundsAndDiscontinuities()
    {
        static (ManualTimeProvider Clock, StreamHub Main, StreamHub Low, HqPlayoutReference Reference) Fixture(string name)
        {
            var clock = new ManualTimeProvider(); var main = Hub(name + "-main", clock); var low = Hub(name + "-low", clock);
            low.PublishAac(new AacFrame([0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5]));
            clock.AdvanceTimestampTicks(2_000_000); var reference = new HqPlayoutReference(main, clock);
            Check(Observe(reference, "hq", main.SourceEpoch, 0, 1_000_000, 2_000_000, 1_000_000), "bounded-reader HQ fixture unavailable");
            return (clock, main, low, reference);
        }
        static async Task Drain(MatchedLowQualityPlayout playout, ChannelReader<HubPacket> reader, AudioTrackInfo? audio, CancellationToken ct)
        { await foreach (var packet in playout.ReadAsync(reader, audio, ct)) playout.MarkSent(packet); }

        var count = Fixture("queue-count"); var countInput = Channel.CreateUnbounded<HubPacket>();
        for (int i = 0; i <= 512; i++) countInput.Writer.TryWrite(Video(i, 1_000_000 + i * 1000, 1_300_000 + i * 1000, key: i == 0));
        countInput.Writer.Complete();
        var countBound = new MatchedLowQualityPlayout(count.Low, count.Reference, count.Clock,
            (_, _) => throw new Exception("overfull scheduled queue started sending instead of failing closed"));
        await RejectAsync(() => Drain(countBound, countInput.Reader, count.Low.Audio, CancellationToken.None),
            "scheduled packet count exceeded its 512-reference memory bound");

        var bytes = Fixture("queue-bytes"); var bytesInput = Channel.CreateUnbounded<HubPacket>();
        for (int i = 0; i < 3; i++) bytesInput.Writer.TryWrite(Video(i, 1_000_000 + i * 50_000, 1_300_000 + i * 50_000,
            key: i == 0, data: new byte[2 * 1024 * 1024 + 1]));
        bytesInput.Writer.Complete();
        var byteBound = new MatchedLowQualityPlayout(bytes.Low, bytes.Reference, bytes.Clock,
            (_, _) => throw new Exception("overfull scheduled byte queue started sending instead of failing closed"));
        await RejectAsync(() => Drain(byteBound, bytesInput.Reader, bytes.Low.Audio, CancellationToken.None),
            "individually valid packets bypassed the six-MiB aggregate pending bound");

        var sequence = Fixture("queue-key"); var sequenceInput = Channel.CreateUnbounded<HubPacket>();
        sequenceInput.Writer.TryWrite(Aac(0, 1_000_000));
        sequenceInput.Writer.TryWrite(Video(1, 1_000_000, 1_300_000, key: false));
        sequenceInput.Writer.TryWrite(new HubAudioOpus(2, [1, 2, 3], 0));
        var key = Video(3, 1_050_000, 1_350_000); var predicted = Video(5, 1_100_000, 1_400_000, key: false);
        sequenceInput.Writer.TryWrite(key); sequenceInput.Writer.TryWrite(new HubAudioOpus(4, [4, 5, 6], 960));
        sequenceInput.Writer.TryWrite(predicted); sequenceInput.Writer.Complete();
        Task Advance(TimeSpan delay, CancellationToken ct) { ct.ThrowIfCancellationRequested(); sequence.Clock.Advance(delay); return Task.CompletedTask; }
        var gated = new MatchedLowQualityPlayout(sequence.Low, sequence.Reference, sequence.Clock, Advance); var delivered = new List<HubPacket>();
        await foreach (var packet in gated.ReadAsync(sequenceInput.Reader, sequence.Low.Audio, CancellationToken.None))
        { delivered.Add(packet); gated.MarkSent(packet); }
        Check(delivered.Count == 2 && ReferenceEquals(delivered[0], key) && ReferenceEquals(delivered[1], predicted),
            "first-key gating leaked earlier AAC/predicted video or ignored Opus corrupted global publication-index checks");

        var gap = Fixture("queue-gap"); var gapInput = Channel.CreateUnbounded<HubPacket>();
        gapInput.Writer.TryWrite(Video(0, 1_000_000, 1_300_000)); gapInput.Writer.TryWrite(new HubAudioOpus(2, [1], 0));
        gapInput.Writer.TryWrite(Video(3, 1_050_000, 1_350_000, key: false)); gapInput.Writer.Complete();
        var gapBound = new MatchedLowQualityPlayout(gap.Low, gap.Reference, gap.Clock,
            (_, _) => throw new Exception("discontinuous reader began scheduling damaged input"));
        await RejectAsync(() => Drain(gapBound, gapInput.Reader, gap.Low.Audio, CancellationToken.None),
            "an index gap on an ignored Opus packet bypassed fail-closed discontinuity handling");

        var cancel = Fixture("queue-cancel"); var cancelInput = Channel.CreateUnbounded<HubPacket>();
        cancelInput.Writer.TryWrite(Video(0, 1_000_000, 1_300_000));
        var cancelWaits = new GopPlayoutWireTests.ControlledWait(cancel.Clock);
        var cancellable = new MatchedLowQualityPlayout(cancel.Low, cancel.Reference, cancel.Clock, cancelWaits.Wait);
        using var stop = new CancellationTokenSource();
        await using (var output = cancellable.ReadAsync(cancelInput.Reader, cancel.Low.Audio, stop.Token).GetAsyncEnumerator())
        {
            Task<bool> pending = output.MoveNextAsync().AsTask(); var wait = await cancelWaits.Next(); stop.Cancel();
            bool cancelled = false;
            try { await pending.WaitAsync(TimeSpan.FromSeconds(2)); } catch (OperationCanceledException) { cancelled = true; }
            Check(cancelled && wait.Cancelled.Task.IsCompleted, "scheduled-reader cancellation leaked its timer or pending channel wait");
        }

        var epoch = Fixture("queue-epoch"); var epochInput = Channel.CreateUnbounded<HubPacket>();
        epochInput.Writer.TryWrite(Video(0, 1_000_000, 1_300_000));
        var epochWaits = new GopPlayoutWireTests.ControlledWait(epoch.Clock);
        var epochBound = new MatchedLowQualityPlayout(epoch.Low, epoch.Reference, epoch.Clock, epochWaits.Wait);
        await using (var output = epochBound.ReadAsync(epochInput.Reader, epoch.Low.Audio, CancellationToken.None).GetAsyncEnumerator())
        {
            Task<bool> pending = output.MoveNextAsync().AsTask(); var wait = await epochWaits.Next(); epoch.Low.SourceStopped(); wait.Release();
            await RejectAsync(async () => { await pending.WaitAsync(TimeSpan.FromSeconds(2)); },
                "scheduled reader emitted queued data after its low-source epoch ended");
        }
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
        await TeardownDuringScheduledWait();
    }

    private static async Task TeardownDuringScheduledWait()
    {
        var clock = new ManualTimeProvider(); var main = Hub("teardown-main", clock);
        clock.AdvanceTimestampTicks(2_000_000); var low = Hub("teardown-low", clock);
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = waits.Wait };
        ProtectBridgeHost.AddRtspMounts(server, "synthetic", main, new() { "u" }, true, 0,
            lowHub: low, gopAudio: true, lowQualityMatchHqDelay: true);
        Check(Observe(server.FindMount("/synthetic")!.PublishHqTiming!, "hq", main.SourceEpoch, 0,
            1_000_000, 2_000_000, 1_000_000), "TEARDOWN HQ reference unavailable");
        await using var wire = await Wire.Start(server); await wire.Setup("/synthetic/subStream", 0);
        await wire.Command("PLAY", "/synthetic/subStream"); var pending = await waits.Next();
        Check((await wire.Command("TEARDOWN", "/synthetic/subStream")).Status == 200,
            "real RTSP TEARDOWN stalled behind the scheduled packet");
        await pending.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Check(clock.GetTimestamp() == 2_000_000, "TEARDOWN released a delayed picture instead of cancelling its wait");
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
        gate.Release();
        Check(await wire.ClosedAfterAtMostOneKeyframe(), "matched LQ emitted damaged media after subscriber backpressure instead of requiring a fresh keyframe/session");
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
        public async Task<bool> ClosedAfterAtMostOneKeyframe()
        {
            int pictures = 0;
            // The reader-aware scheduler can discover an input gap before its
            // first deadline. A previously authorized cached key may also win
            // that race; neither outcome permits a subsequent predicted frame.
            for (int packets = 0; packets < 32; packets++)
            {
                Frame frame;
                try
                {
                    if (!_media.TryDequeue(out frame!))
                    {
                        var first = new byte[1];
                        if (await _stream.ReadAsync(first, _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(2)) == 0) return true;
                        Check(first[0] == '$', "overflow close returned unexpected RTSP data"); frame = await ReadFrame();
                    }
                }
                catch (IOException) { return true; }
                if (frame.Channel != 0) continue;
                Check(frame.Data.Length > 12, "overflow emitted an invalid video RTP packet");
                int nalType = (frame.Data[12] >> 1) & 63;
                if (nalType is not (19 or 32 or 33 or 34)) return false;
                if ((frame.Data[1] & 0x80) != 0 && ++pictures > 1) return false;
            }
            return false;
        }
        public async ValueTask DisposeAsync()
        { _stop.Cancel(); _client.Dispose(); _listener.Stop(); try { await _serving.WaitAsync(TimeSpan.FromSeconds(3)); } catch (OperationCanceledException) { } finally { _stop.Dispose(); } }
    }
}
