using Neolink.Rtsp;
using Neolink.Streaming;

internal static class AacReportClockTests
{
    private static void Check(bool yes, string message) { if (!yes) throw new Exception(message); }
    private static void At(ManualTimeProvider clock, long at)
    { Check(at >= clock.GetTimestamp(), "clock fixture moved backwards"); clock.AdvanceTimestampTicks(at - clock.GetTimestamp()); }
    private readonly record struct Result(double MaximumPhase, double MaximumSteadyPhase, double MaximumSrResidual);

    public static Task RunAll()
    {
        AacLongDriftTests.RunAll().GetAwaiter().GetResult();
        FractionalPhaseAndExplicitRecovery();
        MatureRecoveryAndPeekCommit();
        NominalDeliveryOffsetsKeepTheOriginalLine();
        UnsupportedRateCannotFreezeAsHealthy();
        foreach (double ppm in new[] { 0d, 25d, 60d, 4000d })
        {
            var clean = Simulate(ppm, 1, false, false);
            Check(clean.MaximumPhase < .250 && clean.MaximumSteadyPhase < .0001,
                $"clean {ppm}ppm did not acquire within250ms and converge to100us: {clean}");
        }
        foreach (int units in new[] { 1, 16 })
        {
            var jitter = Simulate(0, units, true, false);
            Check(jitter.MaximumPhase < .020 && jitter.MaximumSrResidual < .001,
                $"nominal jitter became a moving SR phase: bundle={units}, {jitter}");
            var cold = Simulate(4000, units, true, true);
            Check(cold.MaximumPhase < .250 && cold.MaximumSrResidual < .005,
                $"one-AU cold drift acquisition became unbounded or jumped: bundle={units}, {cold}");
        }
        Console.WriteLine("AAC report clock: nominal start, robust uncertainty, continuous phase,45min drift/wrap and bounded cold acquisition passed.");
        return Task.CompletedTask;
    }

    private static Result Simulate(double ppm, int units, bool jitter, bool cold)
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        double rate = 16000 * (1 + ppm / 1_000_000), period = 1024 / rate;
        const uint start = uint.MaxValue - 2047;
        int next = 0, last = 0; double lastTruth = .05, maximumPhase = 0, maximumSteady = 0, maximumSr = 0;
        uint? previousReport = null; double previousAt = 0;
        double firstReport = cold ? .05 + (jitter ? .008 : 0) : .25;
        // First emit after exactly one confirmed AU, without a rate oracle.
        if (cold)
        {
            At(clock, (long)Math.Round(firstReport * 1_000_000));
            sender.ObserveSent(start, clock.GetTimestamp(), true);
            uint first = sender.RtpAt(clock.GetTimestamp());
            Check(first == start, "one-AU startup invented a learned rate or phase");
            sender.MarkReportSent(clock.GetTimestamp());
            previousReport = first; previousAt = firstReport; next = 1;
        }
        for (double reportAt = firstReport + (cold ? 1 : 0); reportAt <= 2700; reportAt += 1)
        {
            while (true)
            {
                int block = (next - (cold ? 1 : 0)) / units;
                int end = (block + 1) * units - (cold ? 0 : 1);
                double noise = jitter ? (block % 2 == 0 ? .008 : -.008) : 0;
                double arrival = end * period + .05 + noise;
                if (arrival > reportAt) break;
                At(clock, (long)Math.Round(arrival * 1_000_000));
                for (; next <= end; next++)
                {
                    uint rtp = unchecked(start + (uint)(next * 1024L));
                    // Original bundles use nominal de-burst spacing; only the
                    // real last AU's arrival becomes a fit observation.
                    double presentation = arrival - (end - next) * .064;
                    sender.ObserveSent(rtp, presentation * 1_000_000, next == end,
                        observationTimestamp: arrival * 1_000_000);
                    last = next; lastTruth = next * period + .05;
                }
            }
            At(clock, (long)Math.Round(reportAt * 1_000_000));
            if (!sender.HaveAnchor) continue; // No report before the first actual bundle arrived.
            uint wire = sender.RtpAt(clock.GetTimestamp());
            double mapped = reportAt - unchecked((int)(wire - unchecked(start + (uint)(last * 1024L)))) / rate;
            double error = Math.Abs(mapped - lastTruth); maximumPhase = Math.Max(maximumPhase, error);
            if (reportAt >= 1500) maximumSteady = Math.Max(maximumSteady, error);
            if (previousReport is { } previous)
                maximumSr = Math.Max(maximumSr, Math.Abs(unchecked((int)(wire - previous)) / rate - (reportAt - previousAt)));
            // A successful emission can change only the FUTURE slope. Neither
            // observation nor Mark may jump the already computed report phase.
            sender.MarkReportSent(clock.GetTimestamp());
            Check(sender.RtpAt(clock.GetTimestamp()) == wire, "MarkReportSent reanchored phase instead of changing future rate");
            previousReport = wire; previousAt = reportAt;
        }
        return new(maximumPhase, maximumSteady, maximumSr);
    }

    private static void FractionalPhaseAndExplicitRecovery()
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        const uint first = uint.MaxValue - 2047;
        sender.ObserveSent(first, 0, true); sender.MarkReportSent(0);
        for (int i = 1; i <= 10000; i++)
        {
            long at = i * 70003L; At(clock, at);
            uint wire = sender.RtpAt(at);
            Check(wire == unchecked(first + (uint)(long)Math.Round(at / 1_000_000d * 16000)),
                "fractional wire rounding accumulated into the next report phase");
            sender.MarkReportSent(at);
        }
        sender.Reset(); Check(!sender.HaveAnchor && !sender.Published, "explicit epoch/format reset retained a published clock");
        sender.ObserveSent(first, clock.GetTimestamp(), true); sender.MarkReportSent(clock.GetTimestamp());
        long following = clock.GetTimestamp() + 1_064_000; At(clock, following);
        uint before = sender.RtpAt(following);
        sender.ObserveSent(unchecked(first + 1024 + 16000), following, true, 16000);
        Check(sender.Published && sender.RtpAt(following) == before, "paired forward-pause RTP offset changed published phase");
        At(clock, following + 128000);
        sender.ObserveSent(unchecked(first + 1024 + 16000 + 2048), following + 128000, true, 16000);
        Check(!sender.Published, "actual lost AU did not start an explicit clock generation");
    }

    private static void NominalDeliveryOffsetsKeepTheOriginalLine()
    {
        foreach (bool ramp in new[] { false, true })
        foreach (double stepAt in new[] { .2, 4d, 20d, 120d })
        foreach (double step in new[] { -.024, -.008, .008, .024 })
        {
            var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
            const uint start = uint.MaxValue - 2047;
            for (int i = 0; i < 6000; i++)
            {
                double source = i * .064;
                double delivery = ramp ? Math.Min(source / .2, 1) * .0008 : source >= stepAt ? step : 0;
                long at = (long)Math.Round((source + .05 + delivery) * 1_000_000); At(clock, at);
                sender.ObserveSent(unchecked(start + (uint)(i * 1024L)), at, true);
                if (i % 16 != 0) continue;
                uint expected = unchecked(start + (uint)(long)Math.Round((at / 1_000_000d - .05) * 16000));
                Check(sender.RtpAt(at) == expected, $"pure {(ramp ? "startup ramp" : "delivery step")} was learned as oscillator drift at AU={i}: {sender.Diagnostics()}");
                sender.MarkReportSent(at);
                Check(sender.RtpAt(at) == expected, "nominal delivery offset changed phase during commit");
            }
        }
    }

    private static void UnsupportedRateCannotFreezeAsHealthy()
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        bool rejected = false;
        for (int i = 0; i < 1000 && !rejected; i++)
        {
            long at = (long)Math.Round(i * 1024d / 16320 * 1_000_000); At(clock, at);
            sender.ObserveSent((uint)(i * 1024), at, true);
            if (i % 16 != 0) continue;
            try { sender.RtpAt(at); sender.MarkReportSent(at); }
            catch (IOException) { rejected = true; }
        }
        Check(rejected, "unsupported rate retained an old apparently healthy nominal fit indefinitely");
    }

    private static void MatureRecoveryAndPeekCommit()
    {
        var clock = new ManualTimeProvider(); var sender = new AacReportClock(16000, clock);
        const uint start = uint.MaxValue - 2047;
        for (int i = 0; i < 2000; i++)
        {
            long at = i * 64000L; At(clock, at);
            sender.ObserveSent(unchecked(start + (uint)(i * 1024L)), at, true);
            if (i % 16 == 0) { sender.RtpAt(at); sender.MarkReportSent(at); }
        }
        long peekAt = clock.GetTimestamp(); uint peek = sender.RtpAt(peekAt);
        clock.AdvanceTimestampTicks(100000); // Simulated awaited transport latency.
        sender.MarkReportSent(peekAt);
        Check(sender.RtpAt(peekAt) == peek, "send latency replaced the paired report's original timestamp");
        long recoveryAt = 2000 * 64000L + 990000; At(clock, recoveryAt);
        uint before = sender.RtpAt(recoveryAt);
        sender.ObserveSent(unchecked(start + (uint)(2000 * 1024L) + 15840), recoveryAt, true, 15840);
        Check(sender.Published && sender.RtpAt(recoveryAt) == before, "mature forward recovery reset or moved the report model");
        At(clock, recoveryAt + 6500000);
        sender.ObserveSent(unchecked(start + (uint)(2001 * 1024L) + 15840), clock.GetTimestamp(), true, 15840);
        Check(!sender.Published, "actual6.5-second send silence retained the old report generation");
    }
}
