using System.Diagnostics;
using System.Reflection;
using System.Threading.Channels;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class VideoRtpClockTests
{
    private static readonly byte[] Picture = [0, 0, 0, 1, 0x65, 0x80];
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }
    private static readonly BindingFlags Private = BindingFlags.Instance | BindingFlags.NonPublic;
    private static FieldInfo Field(string name) => typeof(StreamHub).GetField(name, Private)
        ?? throw new Exception("Missing StreamHub field: " + name);

    // Exercise the actual publisher and emitted packets, not a copied conversion.
    private sealed class Feed : IDisposable
    {
        public readonly ManualTimeProvider Clock = new();
        public readonly StreamHub Hub;
        private readonly Guid _id;
        private readonly ChannelReader<HubPacket> _reader;
        private readonly bool _sourceProvenance;
        private long _elapsedUs;
        public Feed(string name, bool sourceProvenance = false)
        {
            _sourceProvenance = sourceProvenance;
            Hub = new StreamHub(name, Clock);
            (_id, _reader) = Hub.Subscribe();
        }
        public Feed(string name, uint initialRtp, bool sourceProvenance = false) : this(name, sourceProvenance)
            => Field("_videoRtpTs").SetValue(Hub, initialRtp);
        public HubVideo Publish(uint cameraUs, long elapsedUs)
        {
            Check(elapsedUs >= _elapsedUs, "Synthetic host clock moved backwards");
            Clock.AdvanceTimestampTicks(elapsedUs - _elapsedUs); _elapsedUs = elapsedUs;
            var frame = new VideoFrame(VideoCodec.H264, true, cameraUs, null, Picture);
            if (_sourceProvenance) Hub.PublishVideo(frame, Clock.GetTimestamp(), Clock.TimestampFrequency);
            else Hub.PublishVideo(frame);
            Check(_reader.TryRead(out var packet) && packet is HubVideo, "Actual StreamHub lost a synthetic picture");
            var video = (HubVideo)packet!;
            Check(ReferenceEquals(video.AnnexB, Picture) && video.CameraMicroseconds == cameraUs && video.Keyframe,
                "RTP clock changed encoded bytes, keyframe status or the original camera counter");
            Check(video.SourceEpoch == Hub.SourceEpoch && video.ArrivalTimestamp == Clock.GetTimestamp()
                && video.ArrivalTimestampFrequency == Clock.TimestampFrequency
                && video.SourceArrivalTimestamp == (_sourceProvenance ? Clock.GetTimestamp() : null),
                "RTP clock changed source epoch or timestamp provenance");
            return video;
        }
        public void Dispose() => Hub.Unsubscribe(_id);

    }

    public static Task SubMillisecondCadenceAndSenderReports()
    {
        using var feed = NewFeed("video-rtp-sr", sourceProvenance: true);
        var sender = new LowQualityRtpSenderClock(feed.Hub, feed.Clock, requireSourceArrival: true);
        uint originUs = uint.MaxValue - 900_000;
        uint originRtp = feed.Publish(originUs, 0).RtpTs;
        long elapsedUs = 0; int reports = 0, biasedReports = 0;
        string? firstMismatch = null; uint finalTicks = 0;
        // Paired 50,001/49,999-us source durations are exactly 20fps on average.
        // The old implementation loses one tick per pair: -111.111 ppm.
        for (int i = 1; i <= 6000; i++)
        {
            elapsedUs += i % 2 == 1 ? 50_001 : 49_999;
            var packet = feed.Publish(unchecked(originUs + (uint)elapsedUs), elapsedUs);
            uint expected = (uint)((ulong)elapsedUs * 90 / 1000);
            uint actual = unchecked(packet.RtpTs - originRtp);
            if (actual != expected) firstMismatch ??=
                $"Actual StreamHub cumulative conversion drifted: frame={i}, elapsedUs={elapsedUs}, actualTicks={actual}, expectedTicks={expected}";
            Check(packet.Index == i + 1, "Video clock changed packet ordering/index");
            finalTicks = actual;
            Check(sender.ObserveRawVideoSent(packet), "Confirmed original video packet rejected by SR adapter");
            if (i % 20 != 0) continue;
            // Same instant paired RTP/NTP mapping, including fractional 90kHz ticks.
            Check(sender.TryGetReport(true, false, out var report) && report.VideoRtpTimestamp != null,
                "Fresh confirmed video SR unavailable");
            if (unchecked(report.VideoRtpTimestamp!.Value - originRtp) != expected) biasedReports++;
            Check(Math.Abs((report.UtcTimestamp - DateTimeOffset.UnixEpoch).TotalMicroseconds - elapsedUs) < .1,
                "Actual video sender report changed the paired monotonic/UTC instant");
            sender.MarkReportSent(); reports++;
        }
        Check(reports == 300, "Five-minute video/SR fixture was truncated");
        long expectedTicks = elapsedUs * 90 / 1000;
        Console.WriteLine($"Actual StreamHub 6000 pictures/300 SRs: ticks={finalTicks}, expected={expectedTicks}, errorTicks={finalTicks - expectedTicks}, errorPpm={(finalTicks - expectedTicks) * 1_000_000d / expectedTicks:F6}, biasedReports={biasedReports}.");
        Check(firstMismatch == null && biasedReports == 0,
            firstMismatch ?? "Actual video sender reports retained cumulative quantization drift");
        return Task.CompletedTask;
    }

    public static Task LongRunAndWraps()
    {
        Run(8 * 3600L, coarse: false);
        Run(4 * 24 * 3600L, coarse: true);
        return Task.CompletedTask;

        static void Run(long seconds, bool coarse)
        {
            // Force an RTP wrap as well as many natural 32-bit camera-us wraps.
            using var feed = NewFeed("video-rtp-long", uint.MaxValue - 500);
            const uint cameraOrigin = uint.MaxValue - 1000;
            uint origin = feed.Publish(cameraOrigin, 0).RtpTs;
            long elapsed = 0, frames = 0, wraps = 0;
            uint previous = origin;
            while (elapsed < seconds * 1_000_000)
            {
                // Eight hours at 20fps with every fractional phase; four days
                // also exercise the existing valid five-second delta boundary.
                long delta = coarse ? (frames % 2 == 0 ? 4_999_999 : 5_000_000)
                    : (frames % 2 == 0 ? 50_000 + frames / 2 % 100 : 50_000 - frames / 2 % 100);
                delta = Math.Min(delta, seconds * 1_000_000 - elapsed);
                elapsed += delta; frames++;
                var packet = feed.Publish(unchecked(cameraOrigin + (uint)elapsed), elapsed);
                ulong expected = (ulong)elapsed * 90 / 1000;
                Check(unchecked(packet.RtpTs - origin) == unchecked((uint)expected),
                    $"Actual StreamHub long conversion failed: seconds={seconds}, frame={frames}, expectedTicks={expected}, actualRelativeTicks={unchecked(packet.RtpTs - origin)}");
                Check(expected * 1000 <= (ulong)elapsed * 90 && (ulong)elapsed * 90 - expected * 1000 < 1000,
                    "Cumulative conversion exceeded one tick of quantization");
                if (packet.RtpTs < previous) wraps++;
                previous = packet.RtpTs;
            }
            Check(frames > (coarse ? 69_000 : 575_000) && wraps >= (coarse ? 7 : 1),
                "Long fixture did not cover its full duration and RTP wraps");
            Console.WriteLine($"PASS actual StreamHub: {seconds}s/{frames} frames/{wraps} RTP wraps, exact cumulative conversion with <1 tick error.");
        }
    }

    public static Task EpochIsolationAndFallbackRules()
    {
        using var first = NewFeed("video-rtp-epoch", 100u);
        using var second = NewFeed("video-rtp-independent", 100u);
        Check(first.Publish(0, 0).RtpTs == 100 && second.Publish(0, 0).RtpTs == 100,
            "First camera timestamp advanced RTP");
        Check(first.Publish(50_011, 50_011).RtpTs == 4600, "Fixture did not retain a nonzero fractional remainder");
        Check(second.Publish(50_001, 50_001).RtpTs == 4600, "One hub's fraction leaked into another hub");
        Check(second.Publish(100_001, 100_001).RtpTs == 9100,
            "Integral 50,000-us duration changed despite its exact 4,500-tick advance");
        long epoch = first.Hub.SourceEpoch; first.Hub.SourceStopped();
        Check(first.Publish(4_000_000_000, 50_011).RtpTs == 4600 && first.Hub.SourceEpoch == epoch + 1,
            "New source epoch changed the existing RTP counter or retained its previous camera origin");
        Check(first.Publish(4_000_050_001, 100_012).RtpTs == 9100,
            "Previous source's sub-tick remainder leaked through SourceStopped");

        // Read the existing private wall samples to verify precisely which
        // fallback delta the unchanged publisher selected. No timing tolerance
        // substitutes for the cumulative integer conversion oracle.
        uint previousCamera = 4_000_050_001, previousRtp = 9100;
        ulong chosenUs = 50_001;
        var wall = (Stopwatch)Field("_wallClock").GetValue(first.Hub)!;
        foreach (var (camera, wallGap) in new[] {
            (previousCamera, 17), // duplicate -> wall fallback
            (previousCamera - 1, 19), // backward -> wall fallback
            (unchecked(previousCamera - 1 + 6_000_000), 21), // >5 seconds -> fallback
            (previousCamera, 6000) }) // wall fallback stays capped at five seconds
        {
            var before = wall.Elapsed - TimeSpan.FromMilliseconds(wallGap);
            Field("_lastWall").SetValue(first.Hub, before);
            var packet = first.Publish(camera, 100_012);
            var after = (TimeSpan)Field("_lastWall").GetValue(first.Hub)!;
            uint deltaUs = (uint)Math.Clamp((after - before).TotalMicroseconds, 0, 5_000_000);
            chosenUs += deltaUs;
            uint expected = unchecked(4600u + (uint)(chosenUs * 90 / 1000));
            Check(packet.RtpTs == expected && deltaUs <= 5_000_000,
                "Duplicate/backward/gap fallback no longer converts the existing bounded elapsed delta exactly");
            previousCamera = camera; previousRtp = packet.RtpTs;
        }
        var boundary = first.Publish(unchecked(previousCamera + 5_000_000), 100_012);
        Check(unchecked(boundary.RtpTs - previousRtp) == 450_000,
            "Valid five-second camera delta fell back to wall time or overflowed");
        return Task.CompletedTask;
    }

    private static Feed NewFeed(string name, uint? initialRtp = null, bool sourceProvenance = false)
        => initialRtp == null ? new Feed(name, sourceProvenance) : new Feed(name, initialRtp.Value, sourceProvenance);
}
