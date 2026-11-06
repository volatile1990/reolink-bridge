using System.Buffers.Binary;
using System.Text.Json;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

/// <summary>Long continuous sessions expose drift hidden by short arrival windows.
/// All clocks/media are local synthetic inputs; no camera or network is used.</summary>
internal static class AacLongDriftTests
{
    private const double DurationSeconds = 8 * 60 * 60;
    private const uint FirstRtp = uint.MaxValue - 2047;
    private static void Check(bool condition, string message)
    { if (!condition) throw new Exception(message); }
    private static void At(ManualTimeProvider clock, long timestamp)
    {
        Check(timestamp >= clock.GetTimestamp(), "long fixture moved the actual send clock backwards");
        clock.AdvanceTimestampTicks(timestamp - clock.GetTimestamp());
    }

    public static Task RunAll()
    {
        var failures = new List<string>();
        foreach (double ppm in new[] { 25d, -25d, 50d, -50d })
        {
            try { LowDriftAndJitter(ppm); }
            catch (Exception error)
            {
                string message = $"{ppm:+0;-0}ppm: {error.Message}";
                Console.Error.WriteLine("FAIL long AAC drift " + message); failures.Add(message);
            }
        }
        foreach (double ppm in new[] { 1d, -1d })
        {
            try { LowDriftAndJitter(ppm, 4 * 24 * 60 * 60, 60); }
            catch (Exception error)
            {
                string message = $"{ppm:+0;-0}ppm/four days: {error.Message}";
                Console.Error.WriteLine("FAIL long AAC drift " + message); failures.Add(message);
            }
        }
        try { LatePureDeliveryOffsets(); }
        catch (Exception error) { failures.Add("nominal late delivery offsets: " + error.Message); }
        RawAdapterPreservesOriginalMedia();
        Check(failures.Count == 0, "long AAC contracts failed: " + string.Join(" | ", failures));
        Console.WriteLine("AAC long drift: four8h and two4day low-drift/jitter sessions, late pure delivery offsets and original AAC/RTP preservation passed.");
        return Task.CompletedTask;
    }

    private static void LowDriftAndJitter(double ppm, double duration = DurationSeconds, double cadence = 1)
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        double actualRate = 16000 * (1 + ppm / 1_000_000), period = 1024 / actualRate;
        int next = 0, reports = 0; double lastTruth = .05, maximumPhase = 0;
        uint? previousWire = null; double previousAt = 0;
        // One independent real arrival per AU. ±8ms is bounded delivery noise,
        // while the source's actual AAC sample period remains 1024/actualRate.
        for (double reportAt = .25; ; reportAt = Math.Min(reportAt + cadence, duration))
        {
            while (true)
            {
                double truth = .05 + next * period;
                double arrival = truth + (next % 2 == 0 ? .008 : -.008);
                if (arrival > reportAt) break;
                At(clock, (long)Math.Round(arrival * 1_000_000));
                uint rtp = unchecked(FirstRtp + (uint)(next * 1024L));
                // This is the production raw adapter's rounded presentation and
                // its separate actual block-end observation timestamp.
                sender.ObserveSent(rtp, Math.Round(arrival * 1_000_000), true,
                    observationTimestamp: arrival * 1_000_000);
                lastTruth = truth; next++;
            }
            At(clock, (long)Math.Round(reportAt * 1_000_000));
            uint wire;
            try { wire = sender.RtpAt(clock.GetTimestamp()); }
            catch (IOException error)
            {
                throw new Exception($"guard at{reportAt:F2}s/{reportAt / 3600:F4}h; " +
                    $"diagnostics={JsonSerializer.Serialize(sender.Diagnostics())}; {error.Message}", error);
            }
            uint lastRtp = unchecked(FirstRtp + (uint)((next - 1) * 1024L));
            // Use the known actual fixture clock, never nominal16000 to invert
            // a genuinely nonnominal source. The250ms bound is unchanged.
            double mapped = reportAt - unchecked((int)(wire - lastRtp)) / actualRate;
            double phase = Math.Abs(mapped - lastTruth); maximumPhase = Math.Max(maximumPhase, phase);
            Check(phase < .250, $"phase exceeded250ms at{reportAt}s: {phase * 1000:F6}ms");
            if (previousWire is { } previous)
            {
                int advance = unchecked((int)(wire - previous)); double elapsed = reportAt - previousAt;
                Check(advance > 0 && Math.Abs(advance / 16000d - elapsed)
                    <= elapsed * AacReportClock.MaximumFrequencyDeviation + 1d / 16000,
                    $"SR timestamp moved outside its continuous supported frequency envelope at{reportAt}s");
            }
            sender.MarkReportSent(clock.GetTimestamp());
            Check(sender.RtpAt(clock.GetTimestamp()) == wire,
                $"MarkReportSent changed already-emitted phase at{reportAt}s");
            if (cadence >= 60 || reports % 60 == 0)
            {
                using var diagnostic = JsonDocument.Parse(JsonSerializer.Serialize(sender.Diagnostics()));
                Check(diagnostic.RootElement.GetProperty("fitRepresentativeCount").GetInt32() <= 120,
                    $"AAC fit retained more than120 representative observations at{reportAt}s");
            }
            previousWire = wire; previousAt = reportAt; reports++;
            if (reportAt == duration) break;
        }
        Check(next > (duration - 1) * actualRate / 1024 && reports >= (duration - 1) / cadence,
            "long fixture did not traverse its genuine AU/report duration");
        Console.WriteLine($"PASS AAC long drift {ppm:+0;-0}ppm: duration={duration}s, cadence={cadence}s, AUs={next}, reports={reports}, maximumPhaseMs={maximumPhase * 1000:F6}");
    }

    private static double DeliveryOffset(double source)
    {
        // Plateaus and ramps begin only after a mature30min/1h/2h/3h/4h/etc
        // session. Each is a pure network delivery change at exactly16000Hz.
        if (source < 1800) return 0;
        if (source < 3600) return .008;
        if (source < 5400) return .008 * (1 - Math.Min((source - 3600) / 30, 1));
        if (source < 7200) return .024;
        if (source < 10800) return .024 * (1 - Math.Min((source - 7200) / 30, 1));
        if (source < 12600) return -.008;
        if (source < 14400) return -.008 * (1 - Math.Min((source - 12600) / 30, 1));
        if (source < 16200) return -.024;
        if (source < 18000) return -.024 * (1 - Math.Min((source - 16200) / 30, 1));
        if (source < 21600) return .024 * Math.Min((source - 18000) / .2, 1);
        if (source < 25200) return .024 * (1 - Math.Min((source - 21600) / .2, 1));
        return -.008 * Math.Min((source - 25200) / .2, 1);
    }

    private static void LatePureDeliveryOffsets()
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        int next = 0, reports = 0;
        for (double reportAt = .25; reportAt <= DurationSeconds; reportAt += 1)
        {
            while (true)
            {
                double source = next * .064;
                double arrival = .05 + source + DeliveryOffset(source);
                if (arrival > reportAt) break;
                At(clock, (long)Math.Round(arrival * 1_000_000));
                sender.ObserveSent(unchecked(FirstRtp + (uint)(next * 1024L)),
                    Math.Round(arrival * 1_000_000), true); next++;
            }
            At(clock, (long)Math.Round(reportAt * 1_000_000));
            uint expected = unchecked(FirstRtp + (uint)(long)Math.Round((reportAt - .05) * 16000));
            Check(sender.RtpAt(clock.GetTimestamp()) == expected,
                $"pure late delivery offset moved the original nominal line at{reportAt}s");
            sender.MarkReportSent(clock.GetTimestamp());
            Check(sender.RtpAt(clock.GetTimestamp()) == expected,
                $"pure late delivery offset reanchored the nominal line during commit at{reportAt}s");
            using var diagnostic = JsonDocument.Parse(JsonSerializer.Serialize(sender.Diagnostics()));
            Check(!diagnostic.RootElement.GetProperty("nonNominalRateKnown").GetBoolean()
                && diagnostic.RootElement.GetProperty("rateHz").GetDouble() == 16000,
                $"pure late delivery offset was misclassified as rate drift at{reportAt}s"); reports++;
        }
        Check(reports >= 28_799, "late-offset fixture did not complete its8h session");
        Console.WriteLine("PASS AAC late pure delivery offsets:8h nominal session, ±8/24ms steps/ramps preserved original line.");
    }

    private static void RawAdapterPreservesOriginalMedia()
    {
        var clock = new ManualTimeProvider(); var hub = new StreamHub("long-aac-payload", clock);
        byte[] adts = [0xff, 0xf1, 0x60, 0x40, 0x01, 0x9f, 0xfc, 1, 2, 3, 4, 5];
        hub.PublishAac(new AacFrame(adts)); var sender = new LowQualityRtpSenderClock(hub, clock);
        var packetizer = new RtpPacketizer(97);
        for (int index = 0; index < 512; index++)
        {
            long at = (long)Math.Round((.05 + index * 1024 / 16000.4 + (index % 2 == 0 ? .008 : -.008)) * 1_000_000);
            At(clock, at); byte[] bytes = [1, 2, 3, (byte)index];
            uint rtp = unchecked(FirstRtp + (uint)(index * 1024L));
            var packet = new HubAudioAac(index, bytes, rtp)
                { SourceEpoch = hub.SourceEpoch, ArrivalTimestamp = at, ArrivalTimestampFrequency = 1_000_000 };
            byte[] before = packetizer.PacketizeAac(packet.Au, packet.RtpTs);
            Check(sender.ObserveRawAacSent(packet, hub.Audio), "raw adapter rejected fresh original AAC during long-test media check");
            byte[] after = packetizer.PacketizeAac(packet.Au, packet.RtpTs);
            Check(packet.RtpTs == rtp && packet.Au.AsSpan().SequenceEqual(bytes)
                && BinaryPrimitives.ReadUInt32BigEndian(before.AsSpan(4)) == rtp
                && BinaryPrimitives.ReadUInt32BigEndian(after.AsSpan(4)) == rtp
                && before.AsSpan(12).SequenceEqual(after.AsSpan(12)),
                "sender report adaptation changed original AAC bytes or its1024-sample RTP counter");
        }
    }
}
