using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class LowQualityRtcpTests
{
    private static readonly byte[] Vps = [0x40, 0x01, 0x80], Sps = [0x42, 0x01, 0x80], Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x91], Predicted = [0x02, 0x01, 0x80, 0x92];
    private static readonly byte[] Adts = [0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5];
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
    private static VideoFrame Key(uint us = 0) => new(VideoCodec.H265, true, us, null, Annex(Vps, Sps, Pps, Idr));
    private static void Check(bool yes, string message) { if (!yes) throw new Exception(message); }
    private static void Close(double actual, double expected, double tolerance, string message) =>
        Check(Math.Abs(actual - expected) <= tolerance, $"{message}: actual={actual}, expected={expected}");
    private static StreamHub Hub(string name, ManualTimeProvider clock)
    {
        var hub = new StreamHub(name, clock); hub.PublishInfo(new MediaInfo(672, 384, 20));
        hub.PublishVideo(Key()); hub.PublishAac(new AacFrame(Adts)); return hub;
    }
    private static HubVideo Video(StreamHub hub, long index, uint rtp, long source, long output) =>
        new(index, Annex(Idr), true, rtp)
        {
            SourceEpoch = hub.SourceEpoch, ArrivalTimestamp = output, ArrivalTimestampFrequency = 1_000_000,
            SourceArrivalTimestamp = source, SourceArrivalTimestampFrequency = 1_000_000
        };
    private static HubAudioAac Aac(StreamHub hub, long index, uint rtp, long source) =>
        new(index, [1, 2, 3, (byte)index], rtp)
        { SourceEpoch = hub.SourceEpoch, ArrivalTimestamp = source, ArrivalTimestampFrequency = 1_000_000 };
    private static void At(ManualTimeProvider clock, long at)
    { Check(at >= clock.GetTimestamp(), "synthetic publication time moved backwards"); clock.AdvanceTimestampTicks(at - clock.GetTimestamp()); }
    private static double MappedInstant(LowQualityRtpReport report, uint reportRtp, uint packetRtp, int rate) =>
        (report.UtcTimestamp - DateTimeOffset.UnixEpoch).TotalSeconds - unchecked(reportRtp - packetRtp) / (double)rate;

    public static async Task RunAll()
    {
        LongRawDualClock();
        RawBundlesDiscontinuitiesAndEpochs();
        RawVideoClockGuards();
        NativeAndEncodedProvenanceModes();
        PacketizationDoesNotConfirmSend();
        await MatchedReportsUseConfirmedPlan();
        await LoopbackReportsAndSessionLifecycle();
        await MatchedPauseDoesNotConfirmUnsentMedia();
    }

    private static void LongRawDualClock()
    {
        // Forty-five minutes of deterministic timestamps, not real-time waiting.
        // The ADC runs 0.4% faster; video encoder output arrives 200ms later.
        // Compare each SR with the known source instant of its confirmed packet,
        // rather than comparing two RTP counters that have independent origins.
        var clock = new ManualTimeProvider(); var hub = Hub("lq-sr-long", clock);
        var sender = new LowQualityRtpSenderClock(hub, clock);
        const double duration = 45 * 60, audioPeriod = .064 / 1.004;
        const uint videoBase = uint.MaxValue - 9000, audioBase = uint.MaxValue - 2047;
        int v = 0, a = 0, reports = 0; long index = 0;
        HubVideo? lastVideo = null; HubAudioAac? lastAudio = null;
        double audioSource = 0, maximumVideoError = 0, maximumAudioError = 0;
        while (true)
        {
            long videoAt = (long)Math.Round((v * .05 + .2) * 1_000_000);
            long audioAt = (long)Math.Round(a * audioPeriod * 1_000_000);
            long now = Math.Min(videoAt, audioAt); if (now > duration * 1_000_000) break;
            At(clock, now);
            if (videoAt <= audioAt)
            {
                long source = (long)Math.Round(v * .05 * 1_000_000);
                hub.PublishVideo(Key(unchecked((uint)source)));
                lastVideo = Video(hub, index++, unchecked(videoBase + (uint)(v * 4500L)), source, now);
                Check(sender.ObserveRawVideoSent(lastVideo), "fresh delayed-encoder video anchor rejected"); v++;
            }
            else
            {
                hub.PublishAac(new AacFrame(Adts)); audioSource = now / 1_000_000d;
                lastAudio = Aac(hub, index++, unchecked(audioBase + (uint)(a * 1024L)), now);
                Check(sender.ObserveRawAacSent(lastAudio, hub.Audio), $"fresh drifting ADC anchor rejected at AU={a}, arrivalTicks={now}"); a++;
            }
            if (now > 1_350_000_000) clock.WallClock = DateTimeOffset.UnixEpoch.AddDays(1);
            if (!sender.ReportDue || !sender.TryGetReport(true, true, out var report)) continue;
            Check(lastVideo != null && lastAudio != null && report.VideoRtpTimestamp != null && report.AudioRtpTimestamp != null,
                "paired report appeared before both confirmed tracks");
            Close((report.UtcTimestamp - DateTimeOffset.UnixEpoch).TotalSeconds, now / 1_000_000d, .000001,
                "wall clock jump or independent track capture changed common monotonic report UTC");
            maximumVideoError = Math.Max(maximumVideoError, Math.Abs(MappedInstant(report, report.VideoRtpTimestamp!.Value,
                lastVideo!.RtpTs, 90000) - lastVideo.SourceArrivalTimestamp!.Value / 1_000_000d));
            maximumAudioError = Math.Max(maximumAudioError, Math.Abs(MappedInstant(report, report.AudioRtpTimestamp!.Value,
                lastAudio!.RtpTs, 16000) - audioSource));
            sender.MarkReportSent(); reports++;
            Check(!sender.ReportDue, "report cadence advanced before one second elapsed");
        }
        Check(v > 53_000 && a > 42_000 && reports > 2600, "long-run fixture did not traverse 45min, both RTP wraps and report cadence");
        Check(maximumVideoError < .00002 && maximumAudioError < .0001,
            $"RTP/NTP mapping accumulated drift or retained encoder delay: video={maximumVideoError}s, AAC={maximumAudioError}s");
    }

    private static void RawBundlesDiscontinuitiesAndEpochs()
    {
        var clock = new ManualTimeProvider(); var hub = Hub("lq-sr-reset", clock);
        var sender = new LowQualityRtpSenderClock(hub, clock);
        Check(!sender.TryGetReport(true, true, out _), "unsent cached metadata created sender-report anchors");
        At(clock, 1_000_000); hub.PublishVideo(Key(1_000_000));
        var video = Video(hub, 0, uint.MaxValue - 99, 800_000, 1_000_000);
        Check(sender.ObserveRawVideoSent(video), "video provenance anchor unavailable");
        HubAudioAac? last = null;
        for (int i = 0; i < 16; i++)
        {
            last = Aac(hub, i + 1, unchecked(uint.MaxValue - 511 + (uint)(i * 1024)), 1_000_000)
                with { BlockUnits = 16, BlockIndex = i };
            Check(sender.ObserveRawAacSent(last, hub.Audio), "valid original ADTS bundle phase rejected");
            Check(sender.TryGetReport(true, true, out var report), "fresh bundled report unavailable");
            Close(MappedInstant(report, report.AudioRtpTimestamp!.Value, last.RtpTs, 16000), .04 + i * .064, .00007,
                "ADTS arrival was used for every AU instead of preserving sample phase");
        }
        Check(!sender.TryGetReport(false, false, out _), "no requested transport produced a report");
        Check(sender.TryGetReport(true, false, out var soloV) && soloV.AudioRtpTimestamp == null, "video-only report invented an audio SSRC clock");
        Check(sender.TryGetReport(false, true, out var soloA) && soloA.VideoRtpTimestamp == null, "audio-only report invented a video SSRC clock");

        At(clock, 7_500_000);
        Check(!sender.TryGetReport(true, true, out _) && !sender.ObserveRawVideoSent(video), "six-second source silence retained a fresh report");
        hub.PublishVideo(Key(7_500_000));
        var resumedV = Video(hub, 20, 1000, 7_300_000, 7_500_000);
        var resumedA = Aac(hub, 21, unchecked(last!.RtpTs + 1024), 7_500_000);
        Check(!sender.ObserveRawVideoSent(resumedV) && sender.ObserveRawAacSent(resumedA, hub.Audio)
            && !sender.TryGetReport(true, true, out _), "first observation after a 6.5s hiccup skipped video revalidation");
        At(clock, 7_550_000); hub.PublishVideo(Key(7_550_000));
        LowQualityRtpReport resumed = default;
        Check(sender.ObserveRawVideoSent(Video(hub, 22, 5500, 7_350_000, 7_550_000))
            && sender.TryGetReport(true, true, out resumed), "following plausible video could not revalidate same-epoch clock");
        Close(MappedInstant(resumed, resumed.AudioRtpTimestamp!.Value, resumedA.RtpTs, 16000), 7.5, .00007,
            "six-second discontinuity retained obsolete accumulated AAC phase");
        long oldEpoch = hub.SourceEpoch; hub.SourceStopped(); hub.PublishVideo(Key()); hub.PublishAac(new AacFrame(Adts));
        Check(hub.SourceEpoch != oldEpoch && !sender.TryGetReport(true, true, out _), "source epoch reused prior sent-track anchors");
        Check(!sender.ObserveRawAacSent(resumedA, hub.Audio), "old-epoch audio rebound a current sender report");
        var freshV = Video(hub, 23, 2000, 7_550_000, 7_550_000); var freshA = Aac(hub, 24, 3000, 7_550_000);
        Check(sender.ObserveRawVideoSent(freshV) && sender.ObserveRawAacSent(freshA, hub.Audio)
            && sender.TryGetReport(true, true, out _), "new epoch could not establish independent clocks");
        Check(!sender.ObserveRawVideoSent(freshV with { SourceArrivalTimestampFrequency = 90000 }), "wrong monotonic frequency accepted");
        Check(!sender.ObserveRawAacSent(freshA with { BlockUnits = 0 }, hub.Audio), "invalid AAC bundle accepted");
        Check(!sender.ObserveMatchedSent(freshV, new(hub.SourceEpoch, double.NaN, 1_000_000)), "nonfinite presentation accepted");
        Check(!sender.ObserveMatchedSent(freshV, new(hub.SourceEpoch, 8_000_000, 1_000_000)), "future presentation was reported as sent");
    }

    private static void RawVideoClockGuards()
    {
        var clock = new ManualTimeProvider(); var hub = Hub("lq-sr-video-clock", clock);
        var sender = new LowQualityRtpSenderClock(hub, clock); At(clock, 1_000_000); hub.PublishVideo(Key());
        Check(sender.ObserveRawVideoSent(Video(hub, 0, uint.MaxValue - 1000, 700_000, 900_000)), "initial video anchor rejected");
        Check(sender.ObserveRawVideoSent(Video(hub, 1, 3499, 750_000, 950_000))
            && sender.TryGetReport(true, false, out _), "ordinary uint RTP wrap was classified as a reset");
        Check(!sender.ObserveRawVideoSent(Video(hub, 2, 7999, 740_000, 960_000))
            && !sender.TryGetReport(true, false, out _), "retrograde original arrival retained video report");
        Check(sender.ObserveRawVideoSent(Video(hub, 3, 7999, 800_000, 1_000_000)), "plausible video could not recover after rejected retrograde source instant");
        Check(!sender.ObserveRawVideoSent(Video(hub, 4, 7899, 850_000, 1_000_000))
            && !sender.TryGetReport(true, false, out _), "backward RTP reset was treated as positive elapsed video time");
        Check(sender.ObserveRawVideoSent(Video(hub, 5, 12499, 850_000, 1_000_000)), "plausible modular progression could not recover");
        Check(!sender.ObserveRawVideoSent(Video(hub, 6, 512500, 900_000, 1_000_000))
            && !sender.TryGetReport(true, false, out _), "RTP forward jump above five seconds retained a video mapping");
        Check(sender.ObserveRawVideoSent(Video(hub, 7, 16999, 900_000, 1_000_000)), "normal video anchor did not recover after oversized RTP step");
        Check(!sender.ObserveRawVideoSent(Video(hub, 8, 16999, 900_000, 1_000_000))
            && sender.TryGetReport(true, false, out _), "duplicate source/RTP refreshed or erased last confirmed mapping");
        At(clock, 6_000_001);
        Check(!sender.TryGetReport(true, false, out _), "duplicates kept an obsolete source epoch report alive");
    }

    private static void PacketizationDoesNotConfirmSend()
    {
        var packetizer = new RtpPacketizer(97);
        byte[] media = packetizer.PacketizeAac([1, 2, 3], 1024);
        byte[] unsent = RtcpSenderReport.Build(packetizer, 1024, DateTimeOffset.UnixEpoch, "not-sent");
        Check(BinaryPrimitives.ReadUInt32BigEndian(unsent.AsSpan(20)) == 0
            && BinaryPrimitives.ReadUInt32BigEndian(unsent.AsSpan(24)) == 0,
            "packetization counted media whose awaited transport send did not complete");
        packetizer.RecordSent(media);
        byte[] sent = RtcpSenderReport.Build(packetizer, 1024, DateTimeOffset.UnixEpoch, "sent");
        Check(BinaryPrimitives.ReadUInt32BigEndian(sent.AsSpan(20)) == 1
            && BinaryPrimitives.ReadUInt32BigEndian(sent.AsSpan(24)) == media.Length - 12,
            "confirmed send did not count exactly original RTP payload octets");
    }

    private static void NativeAndEncodedProvenanceModes()
    {
        var clock = new ManualTimeProvider(); var hub = Hub("lq-sr-provenance", clock);
        var native = new LowQualityRtpSenderClock(hub, clock);
        var encoded = new LowQualityRtpSenderClock(hub, clock, requireSourceArrival: true);
        At(clock, 1_000_000); hub.PublishVideo(Key());
        var nativePacket = Video(hub, 0, 1000, 700_000, 900_000) with { SourceArrivalTimestamp = null, SourceArrivalTimestampFrequency = 0 };
        LowQualityRtpReport nativeReport = default;
        Check(native.ObserveRawVideoSent(nativePacket) && native.TryGetReport(true, false, out nativeReport),
            "native non-encoded source lost its legitimate original-publication arrival fallback");
        Close(MappedInstant(nativeReport, nativeReport.VideoRtpTimestamp!.Value, nativePacket.RtpTs, 90000), .9, .00002,
            "native fallback used an invented earlier input picture time");
        Check(!encoded.ObserveRawVideoSent(nativePacket) && !encoded.TryGetReport(true, false, out _),
            "encoded LQ silently substituted encoder output arrival for missing source provenance");
        var encodedPacket = Video(hub, 1, 5500, 750_000, 950_000);
        LowQualityRtpReport encodedReport = default;
        Check(encoded.ObserveRawVideoSent(encodedPacket) && encoded.TryGetReport(true, false, out encodedReport),
            "encoded source with valid one-to-one provenance did not establish report");
        Close(MappedInstant(encodedReport, encodedReport.VideoRtpTimestamp!.Value, encodedPacket.RtpTs, 90000), .75, .00002,
            "encoded report retained its 200ms encoder latency");
        Check(!encoded.ObserveRawVideoSent(nativePacket with { RtpTs = 10000 }) && !encoded.TryGetReport(true, false, out _),
            "lost encoded provenance retained an earlier fresh report anchor");
        Check(!encoded.ObserveMatchedSent(nativePacket, new(hub.SourceEpoch, 900_000, 1_000_000))
            && !encoded.TryGetReport(true, false, out _), "encoded matched video without original source provenance created a report");
        Check(!encoded.ObserveMatchedSent(encodedPacket with { SourceArrivalTimestampFrequency = 90000 }, new(hub.SourceEpoch, 950_000, 1_000_000)),
            "encoded matched video with foreign monotonic provenance frequency created a report");
        Check(!encoded.ObserveMatchedSent(encodedPacket with { SourceEpoch = hub.SourceEpoch + 1 }, new(hub.SourceEpoch, 950_000, 1_000_000)),
            "matched packet epoch disagreed with its confirmed plan but created a report");
        var nativeMount = new RtspMount { Path = "/native", Hub = hub, LowQualitySenderReports = true };
        var encodedMount = new RtspMount { Path = "/encoded", Hub = hub, LowQualitySenderReports = true, LowQualityRequiresSourceArrival = true };
        Check(!nativeMount.SameMediaPolicy(encodedMount), "one session mixed tracks with different provenance requirements");
    }

    private static async Task MatchedReportsUseConfirmedPlan()
    {
        var clock = new ManualTimeProvider(); var main = Hub("lq-sr-main", clock);
        var reference = new HqPlayoutReference(main, clock); At(clock, 2_000_000); var low = Hub("lq-sr-matched", clock);
        Check(reference.Observe("hq", new CompletedVideoGop([], TimeSpan.FromSeconds(2), main.SourceEpoch)
            { SourceArrivalTimestamp = 0, SourceArrivalFrequency = 1_000_000 }, 2_000_000), "HQ fixture reference missing");
        var input = Channel.CreateUnbounded<HubPacket>();
        var video = Video(low, 0, uint.MaxValue - 1000, 1_000_000, 1_200_000); input.Writer.TryWrite(video);
        HubAudioAac? firstAudio = null;
        for (int i = 0; i < 15; i++)
        {
            var audio = Aac(low, i + 1, unchecked(uint.MaxValue - 511 + (uint)(i * 1024)), 1_064_000 + i * 64_000);
            firstAudio ??= audio; input.Writer.TryWrite(audio);
        }
        input.Writer.Complete();
        Task Advance(TimeSpan delay, CancellationToken ct) { ct.ThrowIfCancellationRequested(); clock.Advance(delay); return Task.CompletedTask; }
        var playout = new MatchedLowQualityPlayout(low, reference, clock, Advance);
        var sender = new LowQualityRtpSenderClock(low, clock);
        await using var output = playout.ReadAsync(input.Reader, low.Audio, CancellationToken.None).GetAsyncEnumerator();
        Check(await output.MoveNextAsync() && ReferenceEquals(output.Current, video), "matched first packet not original video");
        Check(!sender.TryGetReport(true, false, out _), "planned but unsent packet created a report anchor");
        var sentVideo = playout.MarkSent(output.Current);
        Close(sentVideo.PresentationTimestamp, 3_000_000, 1, "video report anchor did not freeze actual plan deadline");
        Check(sender.ObserveMatchedSent(video, sentVideo), "confirmed matched video rejected");
        Check(!sender.TryGetReport(true, true, out _), "ingested future AAC was mistaken for confirmed sent AAC");
        Check(await output.MoveNextAsync() && ReferenceEquals(output.Current, firstAudio), "matched first AAC not original packet");
        clock.AdvanceTimestampTicks(100_000); // Awaited send latency must not replace its scheduled presentation instant.
        var sentAudio = playout.MarkSent(output.Current);
        Close(sentAudio.PresentationTimestamp, 3_064_000, 1, "further-ingested AAC phase or send completion replaced first AAC plan");
        LowQualityRtpReport report = default;
        Check(sender.ObserveMatchedSent(output.Current, sentAudio) && sender.TryGetReport(true, true, out report), "confirmed matched dual report unavailable");
        // Sender was created at monotonic2s/UTC0; expected presentation UTC1s and1.064s.
        Close(MappedInstant(report, report.VideoRtpTimestamp!.Value, video.RtpTs, 90000), 1, .00002, "matched video RTCP did not describe its sent plan");
        Close(MappedInstant(report, report.AudioRtpTimestamp!.Value, firstAudio!.RtpTs, 16000), 1.064, .00007, "matched AAC RTCP used last ingested AU or send wall time");
        Check(video.AnnexB.SequenceEqual(Annex(Idr)) && firstAudio.Au.SequenceEqual(new byte[] { 1, 2, 3, 1 }), "clock binding rewrote encoded bytes");
    }

    private static RtspServer Server(StreamHub hub, ManualTimeProvider clock, bool reports = true)
    {
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" }) { TcpOnly = true, PlayoutTimeProvider = clock };
        server.AddMount(new RtspMount { Path = "/low", Hub = hub, PermittedUsers = new() { "u" }, LowQualitySenderReports = reports });
        return server;
    }

    private static async Task LoopbackReportsAndSessionLifecycle()
    {
        foreach (var (video, audio) in new[] { (true, true), (true, false), (false, true) })
        {
            var clock = new ManualTimeProvider(); var hub = Hub("lq-sr-wire", clock);
            var server = Server(hub, clock);
            await using var wire = await Wire.Start(server);
            if (video) await wire.Setup(0); if (audio) await wire.Setup(1);
            Check((await wire.Command("PLAY")).Status == 200, "LQ PLAY failed");
            await wire.UntilReports(video && audio ? 2 : 1);
            Check(wire.Reports.All(r => r.Channel == (video && !audio ? 1 : !video && audio ? 3 : r.Channel)), "report on an unconfigured RTCP channel");
            if (video && audio) Check(wire.Reports[0].Ntp == wire.Reports[1].Ntp
                && wire.Reports[0].Cname == wire.Reports[1].Cname, "wire A/V reports use different NTP instant or CNAME");
            Check(wire.Reports.All(r => r.Cname.StartsWith("reolink-lq-", StringComparison.Ordinal)), "LQ report lost per-session CNAME");
            int initial = wire.Reports.Count;
            At(clock, 1_100_000); hub.PublishVideo(Key(1_100_000)); hub.PublishAac(new AacFrame(Adts));
            await wire.UntilReports(initial + (video && audio ? 2 : 1));
            Check(wire.Reports.Skip(initial).All(r => r.Ntp > wire.Reports[0].Ntp), "second report did not advance common NTP");
            Check((await wire.Command("PAUSE")).Status == 200, "LQ PAUSE stalled behind RTCP pump");
            int pausedReports = wire.Reports.Count; At(clock, 2_200_000);
            hub.PublishVideo(Key(2_200_000)); hub.PublishAac(new AacFrame(Adts));
            Check((await wire.Command("GET_PARAMETER")).Status == 200 && wire.Reports.Count == pausedReports, "paused pump emitted new sender reports");
            Check((await wire.Command("PLAY")).Status == 200, "LQ resume failed");
            await wire.UntilReports(pausedReports + (video && audio ? 2 : 1));
            Check((await wire.Command("TEARDOWN")).Status == 200, "LQ TEARDOWN failed after reports");
        }
        // A no-opt-in mount still transmits the same AAC/video without creating
        // the new report clock. The command barrier drains already sent frames.
        var offClock = new ManualTimeProvider(); var offHub = Hub("lq-sr-off", offClock);
        await using var off = await Wire.Start(Server(offHub, offClock, reports: false));
        await off.Setup(0); await off.Setup(1); await off.Command("PLAY"); await off.UntilAudio();
        await off.Command("PAUSE"); Check(off.Reports.Count == 0, "default/native mount unexpectedly enabled LQ sender reports");
        await off.Command("TEARDOWN");
    }

    private static async Task MatchedPauseDoesNotConfirmUnsentMedia()
    {
        var clock = new ManualTimeProvider(); var main = Hub("lq-sr-cancel-main", clock);
        var reference = new HqPlayoutReference(main, clock); At(clock, 2_000_000); var low = Hub("lq-sr-cancel-low", clock);
        Check(reference.Observe("hq", new CompletedVideoGop([], TimeSpan.FromSeconds(2), main.SourceEpoch)
            { SourceArrivalTimestamp = 0, SourceArrivalFrequency = 1_000_000 }, 2_000_000), "cancel fixture HQ reference missing");
        var waits = new GopPlayoutWireTests.ControlledWait(clock);
        var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = waits.Wait };
        server.AddMount(new RtspMount { Path = "/low", Hub = low, PermittedUsers = new() { "u" },
            LowQualitySenderReports = true, MatchHqTiming = reference });
        await using var wire = await Wire.Start(server); await wire.Setup(0); await wire.Setup(1); await wire.Command("PLAY");
        var pending = await waits.Next();
        Check((await wire.Command("PAUSE")).Status == 200, "matched PAUSE stalled behind unsent-media deadline");
        await pending.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var diagnostics = System.Text.Json.JsonSerializer.SerializeToNode(server.LowQualityTimingDiagnostics())!;
        var finished = diagnostics["senderReports"]!["lastCompleted"]!;
        Check(wire.Reports.Count == 0 && finished["reportsSent"]!.GetValue<long>() == 0
            && !finished["videoAnchored"]!.GetValue<bool>() && !finished["audioAnchored"]!.GetValue<bool>(),
            "cancelled/unsent matched packet created confirmed RTP/NTP anchors or sender-report counters");
        Check(clock.GetTimestamp() == 2_000_000, "PAUSE emitted the delayed picture instead of cancelling its timer");
        await wire.Command("TEARDOWN");
    }

    private sealed record Reply(int Status);
    private sealed record Frame(byte Channel, byte[] Data);
    private sealed record Report(byte Channel, uint Ssrc, ulong Ntp, uint Rtp, uint Packets, uint Octets, string Cname);
    private sealed class Wire : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly TcpClient _client = new();
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(25));
        private readonly Queue<Frame> _frames = new();
        private readonly Dictionary<byte, (uint Ssrc, uint Packets, uint Octets)> _rtp = new();
        private NetworkStream _stream = null!; private Task _serving = Task.CompletedTask;
        private int _port, _sequence; private string? _session;
        public List<Report> Reports { get; } = new();
        public static async Task<Wire> Start(RtspServer server)
        {
            var wire = new Wire(); wire._listener.Start(); wire._port = ((IPEndPoint)wire._listener.LocalEndpoint).Port;
            wire._serving = Task.Run(async () => { using var peer = await wire._listener.AcceptTcpClientAsync(wire._stop.Token); await new RtspConnection(peer, server).RunAsync(wire._stop.Token); });
            await wire._client.ConnectAsync(IPAddress.Loopback, wire._port, wire._stop.Token); wire._stream = wire._client.GetStream(); return wire;
        }
        public async Task Setup(int track) => Check((await Command("SETUP", "/low/trackID=" + track,
            $"Transport: RTP/AVP/TCP;unicast;interleaved={track * 2}-{track * 2 + 1}\r\n")).Status == 200, "LQ sender-report track SETUP failed");
        public async Task<Reply> Command(string method, string path = "/low", string extra = "")
        {
            string request = $"{method} rtsp://127.0.0.1:{_port}{path} RTSP/1.0\r\nCSeq: {++_sequence}\r\nAuthorization: Basic dTpw\r\n"
                + (_session == null ? "" : $"Session: {_session}\r\n") + extra + "\r\n";
            await _stream.WriteAsync(Encoding.ASCII.GetBytes(request), _stop.Token);
            while (true)
            {
                byte first = (await Read(1))[0]; if (first == '$') { _frames.Enqueue(await ReadFrame()); continue; }
                var head = new List<byte> { first };
                while (head.Count < 4 || !head.TakeLast(4).SequenceEqual(new byte[] { 13, 10, 13, 10 })) head.Add((await Read(1))[0]);
                string text = Encoding.ASCII.GetString(head.ToArray()); int length = 0;
                foreach (string line in text.Split("\r\n"))
                {
                    if (line.StartsWith("Session:", StringComparison.OrdinalIgnoreCase)) _session = line[8..].Trim().Split(';')[0];
                    if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)) length = int.Parse(line[15..].Trim());
                }
                await Read(length); return new Reply(int.Parse(text.Split(' ')[1]));
            }
        }
        private async Task<byte[]> Read(int length)
        { var data = new byte[length]; await _stream.ReadExactlyAsync(data, _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(4)); return data; }
        private async Task<Frame> ReadFrame()
        {
            byte[] head = await Read(3); var frame = new Frame(head[0], await Read((head[1] << 8) | head[2])); Record(frame); return frame;
        }
        private async Task<Frame> Next()
        { if (_frames.TryDequeue(out var frame)) return frame; Check((await Read(1))[0] == '$', "expected interleaved sender-report media"); return await ReadFrame(); }
        public async Task UntilReports(int count) { while (Reports.Count < count) await Next(); }
        public async Task UntilAudio() { while (!_rtp.ContainsKey(2)) await Next(); }
        private void Record(Frame frame)
        {
            var data = frame.Data;
            if (frame.Channel is 0 or 2)
            {
                Check(data.Length >= 12 && data[0] == 0x80, "invalid RTP packet in LQ wire test");
                uint ssrc = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(8));
                var previous = _rtp.GetValueOrDefault(frame.Channel);
                Check(previous.Packets == 0 || previous.Ssrc == ssrc, "RTP SSRC changed within one transport");
                _rtp[frame.Channel] = (ssrc, previous.Packets + 1, previous.Octets + (uint)data.Length - 12);
                if (frame.Channel == 2) Check(data.AsSpan(16).SequenceEqual(Adts.AsSpan(7)), "sender-report path changed original AAC payload");
                return;
            }
            Check(frame.Channel is 1 or 3 && data.Length >= 40 && data.Length % 4 == 0, "invalid RTCP channel/compound size");
            Check(data[0] == 0x80 && data[1] == 200 && BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(2)) == 6,
                "RTCP SR header is not RFC3550 sender-report form");
            uint reportSsrc = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(4));
            Check(data[28] == 0x81 && data[29] == 202 && BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(32)) == reportSsrc
                && data[36] == 1 && 38 + data[37] < data.Length && data[38 + data[37]] == 0,
                "compound RTCP lacks the same-SSRC CNAME SDES item and terminator");
            Check((BinaryPrimitives.ReadUInt16BigEndian(data.AsSpan(30)) + 1) * 4 == data.Length - 28, "SDES length does not match compound padding");
            uint packets = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(20)), octets = BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(24));
            Check(_rtp.TryGetValue((byte)(frame.Channel - 1), out var sent) && sent.Ssrc == reportSsrc
                && sent.Packets == packets && sent.Octets == octets, "SR SSRC/counters include unsent data, RTCP or interleaving overhead");
            Reports.Add(new(frame.Channel, reportSsrc, BinaryPrimitives.ReadUInt64BigEndian(data.AsSpan(8)),
                BinaryPrimitives.ReadUInt32BigEndian(data.AsSpan(16)), packets, octets, Encoding.UTF8.GetString(data, 38, data[37])));
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
