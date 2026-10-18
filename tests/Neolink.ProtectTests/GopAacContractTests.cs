using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class GopAacContractTests
{
    private static readonly byte[] Vps = [0x40, 0x01, 0x80], Sps = [0x42, 0x01, 0x80], Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x91], P = [0x02, 0x01, 0x80, 0x92];
    private static byte[] Annex(params byte[][] nals) => nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
    private static void Check(bool yes, string what) { if (!yes) throw new Exception(what); }
    private static HubAudioAac Aac(long index, uint ts, long arrival, long epoch = 0, byte[]? data = null) =>
        new(index, data ?? [(byte)index, 0x21, 0x43], ts)
        { SourceEpoch = epoch, ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000 };
    private static HubVideo Video(long index, uint ts, long arrival, bool key, byte[]? data = null, uint? us = null, long epoch = 0) =>
        new(index, data ?? Annex(key ? Idr : P), key, ts)
        { SourceEpoch = epoch, ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000, CameraMicroseconds = us };

    public static Task TimelineAndRtcp()
    {
        var clock = new Clock(); var video = new GopVideoPlayout(VideoCodec.H265, 2500, clock);
        byte[] bundle = Annex(new[] { Vps, Sps, Pps, Idr }.Concat(Enumerable.Repeat(P, 15)).ToArray());
        video.Push(Video(0, uint.MaxValue - 9000, 0, true, bundle, 0));
        var audio = new GopAacTimeline(16000, 0);
        uint audioStart = uint.MaxValue - 511;
        var sourceAac = Enumerable.Range(0, 4).Select(i => Aac(i + 1, unchecked(audioStart + (uint)(i * 1024)), i * 64000)).ToArray();
        foreach (var packet in sourceAac) audio.Push(packet);
        video.Push(Video(5, 90000, 128000, false, us: 8_670_000));
        var complete = video.Push(Video(6, 99000, 256000, true, us: 8_720_000))!;
        Check(complete is { UniformCadence: true } && complete.Pictures.Count == 17, "Bundled images/raw8.67s clock jump changed the GOP fix");
        complete = video.BeginGop(complete);
        var plans = audio.Plan(complete);
        Check(plans.Count == 4 && plans.Select(p => p.Offset.TotalMilliseconds).SequenceEqual(new[] { 0d, 64d, 128d, 192d }), "AAC sample cadence was stretched to video frame rate");
        Check(plans.Select(p => p.Packet).SequenceEqual(sourceAac), "AAC access-unit bytes or timestamps were replaced");
        Check(complete.Pictures.SelectMany(v => v.Nals).SelectMany(n => n.ToArray())
            .SequenceEqual(H26x.SplitNals(bundle).Concat(H26x.SplitNals(Annex(P))).SelectMany(n => n.ToArray())), "GOP AAC changed video NAL contents");
        Check(video.PresentationTimestamp(TimeSpan.Zero) == 2_500_000, "AAC changed existing startup reserve");
        Check(audio.RtpAtSourceTime(0) == audioStart && audio.RtpAtSourceTime(0.128) == unchecked(audioStart + 2048), "Audio wrap/common time mapping failed");
        var vTrack = new RtpPacketizer(96); var aTrack = new RtpPacketizer(97);
        byte[] rtp = aTrack.PacketizeAac([0x12, 0x34], audioStart); aTrack.RecordSent(rtp);
        var now = new DateTimeOffset(2026, 10, 13, 0, 0, 0, TimeSpan.Zero).AddMilliseconds(128);
        byte[] vsr = RtcpSenderReport.Build(vTrack, unchecked(complete.Pictures[0].RtpTimestamp + 11520), now, "synthetic-common-clock");
        byte[] asr = RtcpSenderReport.Build(aTrack, unchecked(audioStart + 2048), now, "synthetic-common-clock");
        Check(vsr[1] == 200 && asr[1] == 200 && vsr.AsSpan(8, 8).SequenceEqual(asr.AsSpan(8, 8)) && vsr[29] == 202, "Sender reports do not share NTP or valid compound SDES");
        Check(BinaryPrimitives.ReadUInt32BigEndian(asr.AsSpan(20)) == 1 && BinaryPrimitives.ReadUInt32BigEndian(asr.AsSpan(24)) == 6, "RTCP packet/octet counts include framing or unsent packets");
        Check(Encoding.UTF8.GetString(vsr).Contains("synthetic-common-clock") && Encoding.UTF8.GetString(asr).Contains("synthetic-common-clock"), "Audio/video CNAME differs");
        Check(audio.Diagnostics is { CaptureSynchronizationVerified: false, InitialAudioOffsetMs: 0 }, "Arrival estimate was presented as camera capture synchronization");
        return Task.CompletedTask;
    }

    public static Task LossEpochAndBounds()
    {
        var audio = new GopAacTimeline(16000, 0); audio.Push(Aac(0, 0, 0)); audio.Push(Aac(1, 2048, 128000));
        Check(audio.Diagnostics is { Resets: 1, DroppedPackets: 1, BufferedPackets: 1 }, "Audio queue loss replayed obsolete sample anchor");
        audio.Reset(1); audio.Push(Aac(2, 3072, 192000, 0)); audio.Push(Aac(3, 4096, 256000, 1));
        var gop = new CompletedVideoGop([new([Idr], true, true, 1234, TimeSpan.Zero, 1)], TimeSpan.FromSeconds(1), 1)
        { SourceArrivalTimestamp = 256000, SourceArrivalFrequency = 1_000_000 };
        Check(audio.Plan(gop) is [var fresh] && fresh.Packet.SourceEpoch == 1 && fresh.Packet.RtpTs == 4096, "Source reconnect replayed old-epoch sound");
        var bounded = new GopAacTimeline(16000, 0);
        for (int i = 0; i <= GopAacTimeline.MaxPackets; i++) bounded.Push(Aac(i, (uint)(i * 1024), i * 64000));
        Check(bounded.Diagnostics.BufferedPackets <= GopAacTimeline.MaxPackets && bounded.Diagnostics.BufferedBytes <= GopAacTimeline.MaxBytes
            && bounded.Diagnostics.Resets > 0 && bounded.Diagnostics.DroppedPackets > 0, "AAC retention is not bounded after pressure");
        var byteBound = new GopAacTimeline(16000, 0);
        for (int i = 0; i < 100; i++) byteBound.Push(Aac(i, (uint)(i * 1024), i * 64000, data: new byte[8184]));
        Check(byteBound.Diagnostics.BufferedBytes <= GopAacTimeline.MaxBytes && byteBound.Diagnostics.Resets > 0, "AAC byte limit failed");
        byteBound.Push(Aac(101, 103424, 6500000, data: new byte[8192]));
        Check(byteBound.Diagnostics.DroppedPackets > 0, "Oversized RFC3640 AAC unit was accepted");
        Check(byteBound.Diagnostics.InvalidPackets == 1, "Invalid AAC drop reason was not counted");
        LongRunArrivalDiagnostics();
        RecoveryEpochAndReports();
        return Task.CompletedTask;
    }

    private static void LongRunArrivalDiagnostics()
    {
        // This is five hours of synthetic timestamps, not a five-hour wait or
        // a claim about the source camera's capture clock. The actual Plan/Push
        // implementation sees packets in source-arrival order and closes 2s GOPs.
        static CompletedVideoGop Gop(double start) => new([], TimeSpan.FromSeconds(2), 0)
            { SourceArrivalTimestamp = (long)Math.Round(start * 1_000_000), SourceArrivalFrequency = 1_000_000 };
        static GopAacDiagnostics Simulate(double duration, Func<int, double> delay)
        {
            var timeline = new GopAacTimeline(16000, 0);
            int total = (int)Math.Ceiling(duration / .064), index = 0;
            double Arrival(int i) => i * .064 + delay(i);
            double finish = Arrival(total - 1) + 2;
            for (double start = 0; start < finish; start += 2)
            {
                while (index < total && Arrival(index) < start + 2)
                {
                    timeline.Push(Aac(index, unchecked((uint)(index * 1024L)), (long)Math.Round(Arrival(index) * 1_000_000)));
                    index++;
                }
                foreach (var planned in timeline.Plan(Gop(start)))
                {
                    Check(planned.Packet.Au.SequenceEqual(new byte[] { (byte)planned.Packet.Index, 0x21, 0x43 }), "Phase correction changed original AAC access-unit bytes");
                    uint inverse = timeline.RtpAtSourceTime(start + planned.Offset.TotalSeconds);
                    Check(Math.Abs(unchecked((int)(inverse - planned.Packet.RtpTs))) <= 1, "AAC Plan and frozen inverse RTCP mapping disagree");
                    timeline.MarkSent();
                }
            }
            var result = timeline.Diagnostics;
            Check(index == total && result.BufferedPackets == 0 && result.Resets == 0
                && result.PlannedPackets + result.DroppedPackets == total, "Synthetic arrival accounting lost packets or introduced resets");
            return result;
        }
        var jitter = Simulate(5 * 60 * 60, i => i % 7 * .004);
        Check(jitter.DroppedPackets == 0 && jitter.LateSamplePackets == 0 && jitter.InvalidPackets == 0
            && jitter.MaxSignedArrivalEstimateDeviationMs <= 25, "Ordinary bounded jitter was classified as late audio");
        var phase = Simulate(1000, i => i < 100 ? 0 : .55);
        Check(phase.DroppedPackets == 0 && phase.FreshArrivalLateSamplePackets == 0 && phase.PhaseRecoveries == 1
            && phase.RecoveryRtpOffsetSamples is >= 8750 and <= 8850 && phase.MaxSignedArrivalEstimateDeviationMs is > 549 and < 551,
            "Sustained550-ms source pause still discards fresh AAC instead of coherent phase recovery");
        var larger = Simulate(1000, i => i < 100 ? 0 : .990);
        Check(larger.DroppedPackets == 0 && larger.FreshArrivalLateSamplePackets == 0 && larger.PhaseRecoveries == 1
            && larger.RecoveryRtpOffsetSamples is >= 15790 and <= 15890,
            "Observed990-ms source pause still permanently loses fresh AAC");
        var drift = Simulate(5 * 60 * 60, i => i * .064 * 60e-6);
        Check(drift.DroppedPackets == 0 && drift.FreshArrivalLateSamplePackets == 0 && drift.PhaseRecoveries == 0
            && drift.PhaseCorrectionMs is > 1070 and < 1090 && drift.MaxSignedArrivalEstimateDeviationMs is > 1070 and < 1090,
            "Five-hour60-ppm ADC/video arrival drift still drops current audio");
        var old = new GopAacTimeline(16000, 0);
        for (int i = 0; i < 4; i++) old.Push(Aac(i, (uint)(i * 1024), i * 64000));
        Check(old.Plan(Gop(10)).Count == 0 && old.Diagnostics is { DroppedPackets: 4, LateSamplePackets: 4, FreshArrivalLateSamplePackets: 0 },
            "Actually old observed arrivals were mislabeled as fresh sample-anchor lag");
        var block = new GopAacTimeline(16000, 0);
        for (int i = 0; i < 16; i++) block.Push(Aac(i, (uint)(i * 1024), 960000) with { BlockUnits = 16, BlockIndex = i });
        var plans = block.Plan(Gop(0));
        Check(plans.Count == 16 && plans.Select(p => p.Offset.TotalMilliseconds).SequenceEqual(Enumerable.Range(0, 16).Select(i => i * 64d))
            && block.Diagnostics.MaxArrivalEstimateDeviationMs < .001 && block.Diagnostics.DroppedPackets == 0,
            "ADTS burst block correction invented phase drift or changed sample cadence");
        Console.WriteLine($"GOP AAC corrected synthetic proof: jitter5h drops={jitter.DroppedPackets}; phase550ms drops={phase.DroppedPackets}, recoveries={phase.PhaseRecoveries}; phase990ms drops={larger.DroppedPackets}; drift60ppm5h drops={drift.DroppedPackets}, correctedPhaseMs={drift.PhaseCorrectionMs:F3}; cachedOld={old.Diagnostics.LateSamplePackets}; burst16={plans.Count}");
    }

    private static void RecoveryEpochAndReports()
    {
        static CompletedVideoGop Gop(double start) => new([], TimeSpan.FromSeconds(2), 0)
            { SourceArrivalTimestamp = (long)Math.Round(start * 1_000_000), SourceArrivalFrequency = 1_000_000 };
        var timeline = new GopAacTimeline(16000, 0);
        timeline.Push(Aac(0, uint.MaxValue - 511, 0));
        for (int i = 1; i < 16; i++) timeline.Push(Aac(i, unchecked(uint.MaxValue - 511 + (uint)(i * 1024)), 990000 + i * 64000));
        var plan = timeline.Plan(Gop(0));
        Check(timeline.Diagnostics.PhaseRecoveries == 1 && plan.Count == 16, "Forward pause did not retain all fresh wrap-boundary units");
        var before = timeline.Diagnostics;
        uint first = plan[0].Packet.RtpTs;
        Check(plan.Select((p, i) => unchecked(p.Packet.RtpTs - first) == (uint)(i * 1024)).All(p => p), "Recovery changed the1024-sample cadence inside a segment");
        for (int i = 0; i < 16; i++) Check(Math.Abs(unchecked((int)(timeline.RtpAtSourceTime(plan[i].Offset.TotalSeconds) - plan[i].Packet.RtpTs))) <= 1,
            "Recovery packet is not on the sender-report affine map");
        uint sr0 = timeline.RtpAtSourceTime(0), sr1 = timeline.RtpAtSourceTime(1);
        Check(unchecked(sr1 - sr0) == 16000, "Forward phase recovery caused an RTCP sample-clock jump");
        timeline.Reset(1); timeline.Push(Aac(16, unchecked(uint.MaxValue - 511 + 16 * 1024), 2500000, 1));
        var fresh = timeline.Plan(Gop(2.5) with { SourceEpoch = 1 });
        Check(fresh.Count == 1 && unchecked(fresh[0].Packet.RtpTs - plan[^1].Packet.RtpTs) == 1024,
            "Epoch/loss reset removed the recovered output offset and moved RTP backwards");
        Check(timeline.Diagnostics.PhaseCorrectionMs == 0 && timeline.Diagnostics.RecoveryRtpOffsetSamples == before.RecoveryRtpOffsetSamples,
            "Explicit reset retained the old phase or discarded required RTP continuity");
        timeline.Push(Aac(17, unchecked(uint.MaxValue - 511 + 17 * 1024), 2564000, 1));
        timeline.Plan(Gop(10) with { SourceEpoch = 1 });
        Check(timeline.Diagnostics.LateSamplePackets > 0 && timeline.Diagnostics.FreshArrivalLateSamplePackets == 0,
            "Phase recovery stopped rejecting actually old cached arrivals");
    }

    public static async Task ConfigAndObservedProfiles()
    {
        string example = FindExample(); var json = JsonNode.Parse(await File.ReadAllTextAsync(example))!.AsObject();
        Check(BridgeOnvifConfig.Parse(json["onvif"]!.DeserializeElement()).GopAudio == false, "Existing configuration enabled GOP audio");
        json["onvif"]!["gop_audio"] = true;
        bool rejected = false; try { BridgeOnvifConfig.Parse(json["onvif"]!.DeserializeElement()); } catch (FormatException) { rejected = true; }
        Check(rejected, "GOP audio accepted without controls and GOP opt-ins");
        json["onvif"]!["controls"] = true; json["onvif"]!["gop_playout"] = true;
        var cfg = BridgeOnvifConfig.Parse(json["onvif"]!.DeserializeElement());
        var clock = new Clock(); var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new(1920, 1080, 20));
        hub.PublishVideo(new(VideoCodec.H265, true, 0, null, Annex(Vps, Sps, Pps, Idr)));
        cfg.Profiles = [new() { Width = 1920, Height = 1080, Codec = "H265" }];
        var server = new ProtectOnvifServer(cfg, new Dictionary<string, string> { ["u"] = "p" }, [new("main", "/synthetic/mainStream", hub)], 18554);
        string request = $"<s:Envelope xmlns:s='{ProtectOnvifServer.NsSoap}'><s:Body><m:GetProfiles xmlns:m='{ProtectOnvifServer.NsMedia}'/></s:Body></s:Envelope>";
        string auth = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("u:p"));
        Check(!(await server.HandleAsync(request, auth, CancellationToken.None)).Body.Contains("AudioEncoderConfiguration"), "Cold metadata falsely advertised microphone");
        hub.PublishAac(new(Adts([0x12, 0x34, 0x56])));
        Check((await server.HandleAsync(request, auth, CancellationToken.None)).Body.Contains("AudioEncoderConfiguration"), "Actual AAC was absent from opted-in GOP profile");
        clock.Advance(TimeSpan.FromSeconds(6));
        Check(!(await server.HandleAsync(request, auth, CancellationToken.None)).Body.Contains("AudioEncoderConfiguration"), "Stale AAC remained advertised");
        hub.SourceStopped();
        Check(!(await server.HandleAsync(request, auth, CancellationToken.None)).Body.Contains("AudioEncoderConfiguration"), "Old source epoch remained advertised");
    }

    public static async Task WirePayloadsAndSynchronization()
    {
        var clock = new Clock(); var hub = new StreamHub("synthetic", clock); hub.PublishInfo(new(1920, 1080, 20));
        byte[][] sourceNals = new[] { Vps, Sps, Pps, Idr }.Concat(Enumerable.Repeat(P, 15)).ToArray();
        hub.PublishVideo(new(VideoCodec.H265, true, 0, null, Annex(sourceNals)));
        byte[][] aus = [[0x01, 0x22, 0x33], [0x02, 0x23, 0x34], [0x03, 0x24, 0x35], [0x04, 0x25, 0x36]];
        hub.PublishAac(new(Adts(aus[0])));
        await using var wire = await Wire.Start(hub, clock);
        var describe = await wire.Command("DESCRIBE", "/synthetic");
        Check(describe.Status == 200 && describe.Body.Contains("mpeg4-generic/16000/1"), "GOP actual AAC missing from SDP");
        Check((await wire.Command("DESCRIBE", "/synthetic?audio=opus")).Status == 404, "Unsupported GOP Opus request pretended AAC");
        await wire.Setup(0); await wire.Setup(1); await wire.Command("PLAY", "/synthetic");
        for (int i = 0; i < 100 && hub.ViewerCount == 0; i++) await Task.Delay(2);
        Check(hub.ViewerCount == 1, "GOP AAC viewer did not subscribe");
        for (int i = 1; i < 4; i++) { clock.Advance(TimeSpan.FromMilliseconds(64)); hub.PublishAac(new(Adts(aus[i]))); }
        clock.Advance(TimeSpan.FromMilliseconds(64)); hub.PublishVideo(new(VideoCodec.H265, true, 8_670_000, null, Annex(Idr)));
        var frames = new List<Frame>(); int videoPictures = 0, audioPackets = 0;
        while (videoPictures < 16 || audioPackets < 4)
        {
            var frame = await wire.Frame(); frames.Add(frame);
            if (frame.Channel == 0 && (frame.Payload[1] & 0x80) != 0) videoPictures++;
            if (frame.Channel == 2) audioPackets++;
        }
        var vr = frames.Where(f => f.Channel == 0).ToArray(); var ar = frames.Where(f => f.Channel == 2).ToArray();
        Check(vr.SelectMany(f => f.Payload.Skip(12)).SequenceEqual(sourceNals.SelectMany(n => n)), "Opt-in audio changed/omitted original video NAL bytes");
        Check(ar.Select(f => f.Payload.Skip(16).ToArray()).Zip(aus).All(pair => pair.First.SequenceEqual(pair.Second)), "RFC3640 AAC access units changed on wire");
        uint a0 = BinaryPrimitives.ReadUInt32BigEndian(ar[0].Payload.AsSpan(4));
        Check(ar.Select((f, i) => unchecked(BinaryPrimitives.ReadUInt32BigEndian(f.Payload.AsSpan(4)) - a0) == (uint)(i * 1024)).All(x => x), "AAC RTP clock was rewritten per video frame");
        var reports = frames.Where(f => f.Channel is 1 or 3).ToArray();
        Check(reports.Length >= 2 && reports[0].Payload.AsSpan(8, 8).SequenceEqual(reports[1].Payload.AsSpan(8, 8)), "Real wire lacks common NTP sender reports");
        Check(Encoding.UTF8.GetString(reports[0].Payload.AsSpan(28)).Contains("reolink-gop-")
            && reports[0].Payload.AsSpan(38).SequenceEqual(reports[1].Payload.AsSpan(38)), "Real wire CNAME does not associate tracks");
        uint vFirst = BinaryPrimitives.ReadUInt32BigEndian(vr[0].Payload.AsSpan(4));
        Check(BinaryPrimitives.ReadUInt32BigEndian(reports.First(f => f.Channel == 1).Payload.AsSpan(16)) == vFirst
            && BinaryPrimitives.ReadUInt32BigEndian(reports.First(f => f.Channel == 3).Payload.AsSpan(16)) == a0, "RTP/NTP pair did not describe the same first presentation instant");
        await wire.Command("PAUSE", "/synthetic");
        hub.SourceStopped(); hub.PublishVideo(new(VideoCodec.H265, true, 0, null, Annex(Vps, Sps, Pps, Idr)));
        hub.PublishAac(new(Adts([0x91, 0x92])));
        await wire.Command("PLAY", "/synthetic");
        for (int i = 0; i < 100 && hub.ViewerCount == 0; i++) await Task.Delay(2);
        clock.Advance(TimeSpan.FromMilliseconds(64)); hub.PublishVideo(new(VideoCodec.H265, true, 64000, null, Annex(Idr)));
        bool freshAudio = false;
        for (int i = 0; i < 20 && !freshAudio; i++)
        { var f = await wire.Frame(); if (f.Channel == 2) { Check(f.Payload.Skip(16).SequenceEqual(new byte[] { 0x91, 0x92 }), "Reconnect replayed previous epoch AAC"); freshAudio = true; } }
        Check(freshAudio, "Reconnect produced no current epoch AAC");
        await wire.Command("TEARDOWN", "/synthetic");
        await WireForwardPhaseRecovery();
    }

    private static async Task WireForwardPhaseRecovery()
    {
        var clock = new Clock(); var hub = new StreamHub("synthetic-phase", clock); hub.PublishInfo(new(1920, 1080, 20));
        var tap = hub.Subscribe();
        hub.PublishVideo(new(VideoCodec.H265, true, 0, null, Annex(Vps, Sps, Pps, Idr)));
        byte[][] aus = Enumerable.Range(0, 20).Select(i => new byte[] { (byte)i, 0x45, 0x67 }).ToArray();
        hub.PublishAac(new(Adts(aus[0])));
        uint sourceFirst = 0;
        while (tap.reader.TryRead(out var packet)) if (packet is HubAudioAac aac) sourceFirst = aac.RtpTs;
        hub.Unsubscribe(tap.id);
        await using var wire = await Wire.Start(hub, clock);
        await wire.Command("DESCRIBE", "/synthetic"); await wire.Setup(0); await wire.Setup(1);
        await wire.Command("PLAY", "/synthetic");
        for (int i = 0; i < 100 && hub.ViewerCount == 0; i++) await Task.Delay(2);
        clock.Advance(TimeSpan.FromMilliseconds(990));
        for (int i = 1; i < 20; i++) { clock.Advance(TimeSpan.FromMilliseconds(64)); hub.PublishAac(new(Adts(aus[i]))); }
        clock.Advance(TimeSpan.FromMilliseconds(64)); hub.PublishVideo(new(VideoCodec.H265, true, 2270000, null, Annex(Idr)));
        var frames = new List<Frame>();
        while (frames.Count(f => f.Channel == 2) < 20) frames.Add(await wire.Frame());
        var audio = frames.Where(f => f.Channel == 2).ToArray();
        Check(audio.Select((f, i) => f.Payload.Skip(16).SequenceEqual(aus[i])).All(x => x), "Actual SendAsync recovery changed AAC units or discarded fresh packets");
        uint expectedOffset = 15840;
        Check(audio.Select((f, i) => BinaryPrimitives.ReadUInt32BigEndian(f.Payload.AsSpan(4)) == unchecked(sourceFirst + expectedOffset + (uint)(i * 1024))).All(x => x),
            "Actual SendAsync ignored the forward-pause RTP offset or changed segment cadence");
        var reports = frames.Where(f => f.Channel == 3).ToArray();
        Check(reports.Length >= 2 && BinaryPrimitives.ReadUInt32BigEndian(reports[0].Payload.AsSpan(16)) == sourceFirst,
            "Actual phase-compensated RTCP map jumped at recovery");
        for (int i = 1; i < reports.Length; i++)
        {
            uint delta = unchecked(BinaryPrimitives.ReadUInt32BigEndian(reports[i].Payload.AsSpan(16)) - BinaryPrimitives.ReadUInt32BigEndian(reports[i - 1].Payload.AsSpan(16)));
            Check(delta is > 0 and < 5 * 16000, "Actual recovery RTCP went backward or jumped over5s");
        }
        await wire.Command("TEARDOWN", "/synthetic");
        Console.WriteLine("GOP AAC actual RTSP recovery proof: sourcePause990ms, all20 AU bytes retained, outputRtpOffset15840, segmentStep1024, commonSR clock continuous");
    }

    private static byte[] Adts(byte[] payload)
    {
        int length = payload.Length + 7;
        return new byte[] { 0xff, 0xf1, 0x60, (byte)(0x40 | (length >> 11)), (byte)(length >> 3), (byte)(((length & 7) << 5) | 0x1f), 0xfc }.Concat(payload).ToArray();
    }
    private static string FindExample()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        { string file = Path.Combine(dir.FullName, "examples", "config.pilot.json"); if (File.Exists(file)) return file; }
        throw new FileNotFoundException("Example missing");
    }
    private static System.Text.Json.JsonElement DeserializeElement(this JsonNode node) =>
        System.Text.Json.JsonSerializer.SerializeToElement(node);
    internal sealed class Clock : TimeProvider
    {
        private long _us;
        public override long TimestampFrequency => 1_000_000;
        public override long GetTimestamp() => Interlocked.Read(ref _us);
        public override DateTimeOffset GetUtcNow() => new DateTimeOffset(2026, 10, 13, 0, 0, 0, TimeSpan.Zero).AddTicks(GetTimestamp() * 10);
        public void Advance(TimeSpan delay) => Interlocked.Add(ref _us, (long)Math.Ceiling(delay.TotalSeconds * 1_000_000));
    }
    private sealed record Frame(byte Channel, byte[] Payload);
    private sealed record Reply(int Status, string Body);
    private sealed class Wire : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly TcpClient _client = new();
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(20));
        private readonly Queue<Frame> _frames = new();
        private NetworkStream _stream = null!; private Task _server = Task.CompletedTask;
        private int _port, _sequence; private string? _session;
        public static async Task<Wire> Start(StreamHub hub, Clock clock)
        {
            var wire = new Wire(); wire._listener.Start(); wire._port = ((IPEndPoint)wire._listener.LocalEndpoint).Port;
            var server = new RtspServer(new Dictionary<string, string> { ["u"] = "p" })
            { TcpOnly = true, PlayoutTimeProvider = clock, PlayoutWaiter = (delay, ct) => { ct.ThrowIfCancellationRequested(); clock.Advance(delay); return Task.CompletedTask; } };
            ProtectBridgeHost.AddRtspMounts(server, "synthetic", hub, new() { "u" }, true, 2500, gopAudio: true);
            wire._server = Task.Run(async () =>
            { using var client = await wire._listener.AcceptTcpClientAsync(wire._stop.Token); await new RtspConnection(client, server).RunAsync(wire._stop.Token); });
            await wire._client.ConnectAsync(IPAddress.Loopback, wire._port, wire._stop.Token); wire._stream = wire._client.GetStream(); return wire;
        }
        public async Task Setup(int track) => Check((await Command("SETUP", "/synthetic/trackID=" + track,
            $"Transport: RTP/AVP/TCP;unicast;interleaved={track * 2}-{track * 2 + 1}\r\n")).Status == 200, "Track SETUP failed");
        public async Task<Reply> Command(string method, string path, string extra = "")
        {
            string req = $"{method} rtsp://127.0.0.1:{_port}{path} RTSP/1.0\r\nCSeq: {++_sequence}\r\nAuthorization: Basic dTpw\r\n"
                + (_session == null ? "" : $"Session: {_session}\r\n") + extra + "\r\n";
            await _stream.WriteAsync(Encoding.ASCII.GetBytes(req), _stop.Token);
            while (true)
            {
                byte first = (await Read(1))[0];
                if (first == '$') { _frames.Enqueue(await ReadFrame()); continue; }
                var header = new List<byte> { first };
                while (header.Count < 4 || !header.TakeLast(4).SequenceEqual(new byte[] { 13, 10, 13, 10 })) header.Add((await Read(1))[0]);
                string text = Encoding.ASCII.GetString(header.ToArray()); int length = 0;
                foreach (string line in text.Split("\r\n"))
                { if (line.StartsWith("Session:", StringComparison.OrdinalIgnoreCase)) _session = line[8..].Trim().Split(';')[0];
                    if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)) length = int.Parse(line[15..].Trim()); }
                return new(int.Parse(text.Split(' ')[1]), Encoding.ASCII.GetString(await Read(length)));
            }
        }
        private async Task<byte[]> Read(int count)
        { var data = new byte[count]; await _stream.ReadExactlyAsync(data, _stop.Token).AsTask().WaitAsync(TimeSpan.FromSeconds(4)); return data; }
        private async Task<Frame> ReadFrame()
        { byte[] head = await Read(3); return new(head[0], await Read((head[1] << 8) | head[2])); }
        public async Task<Frame> Frame()
        { if (_frames.TryDequeue(out var frame)) return frame; Check((await Read(1))[0] == '$', "Expected media frame"); return await ReadFrame(); }
        public async ValueTask DisposeAsync()
        { _stop.Cancel(); _client.Dispose(); _listener.Stop(); try { await _server.WaitAsync(TimeSpan.FromSeconds(3)); } catch (OperationCanceledException) { } _stop.Dispose(); }
    }
}
