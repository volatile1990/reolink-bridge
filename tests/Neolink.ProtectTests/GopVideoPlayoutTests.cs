using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class GopVideoPlayoutTests
{
    // These NALs test elementary-stream boundaries, not decoder validity.
    private static readonly byte[] Vps = [0x40, 0x01, 0x80];
    private static readonly byte[] Sps = [0x42, 0x01, 0x80];
    private static readonly byte[] Pps = [0x44, 0x01, 0x80];
    private static readonly byte[] Aud = [0x46, 0x01, 0x80];
    private static readonly byte[] PrefixSei = [0x4E, 0x01, 0x05, 0x80];
    private static readonly byte[] SuffixSei = [0x50, 0x01, 0x05, 0x80];
    private static readonly byte[] Idr = [0x26, 0x01, 0x80, 0x80];
    private static readonly byte[] Predicted = [0x02, 0x01, 0x80, 0x80];
    private static readonly byte[] Continuation = [0x02, 0x01, 0x40, 0x80];

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private static byte[] Annex(params byte[][] nals) =>
        nals.SelectMany(nal => new byte[] { 0, 0, 0, 1 }.Concat(nal)).ToArray();

    private static HubVideo Packet(long index, uint timestamp, long arrivalUs, bool keyframe = false,
        byte[]? bytes = null, long epoch = 0, long frequency = 1_000_000) =>
        new(index, bytes ?? Annex(keyframe ? Idr : Predicted), keyframe, timestamp)
        {
            SourceEpoch = epoch,
            ArrivalTimestamp = arrivalUs,
            ArrivalTimestampFrequency = frequency
        };

    private static CompletedVideoGop Close(GopVideoPlayout playout, HubVideo nextKeyframe) =>
        playout.Push(nextKeyframe) ?? throw new Exception("complete bounded GOP was not emitted at its closing keyframe");

    private static void Near(double actual, double expected, double tolerance, string message) =>
        Check(Math.Abs(actual - expected) <= tolerance, message);

    public static async Task DriftAndPacing()
    {
        var clock = new ManualTimeProvider(); // deliberately 1 MHz, not TimeSpan's 10 MHz
        var waits = new List<TimeSpan>();
        var playout = new GopVideoPlayout(VideoCodec.H265, 1500, clock, (delay, token) =>
        {
            token.ThrowIfCancellationRequested();
            waits.Add(delay);
            clock.Advance(delay);
            return Task.CompletedTask;
        });
        var originals = new List<byte[]>();
        for (int i = 0; i < 40; i++)
        {
            byte[] bytes = Annex(i == 0 ? Idr : Predicted);
            originals.Add(bytes);
            Check(playout.Push(Packet(i, (uint)(10_000 + i * 4500), i * 56_000L, i == 0, bytes)) == null,
                "an unclosed GOP leaked pictures before its wall duration was known");
            clock.Advance(TimeSpan.FromMilliseconds(56));
        }
        var completed = Close(playout, Packet(40, 190_000, 2_240_000, true));
        Check(completed.Pictures.Count == 40, "normalization changed the picture count");
        Near(completed.Duration.TotalSeconds, 2.24, 0.000001, "source timestamp frequency was interpreted incorrectly");
        uint first = completed.Pictures[0].RtpTimestamp;
        for (int i = 0; i < completed.Pictures.Count; i++)
        {
            var picture = completed.Pictures[i];
            Near(picture.Offset.TotalSeconds, i * 0.056, 1d / 90000, "GOP wall normalization left the original media-clock drift");
            Check(unchecked(picture.RtpTimestamp - first) == (uint)(i * 5040), "RTP delta and paced picture offset disagree");
            Check(picture.AnnexB.SequenceEqual(originals[i]), "normalization changed encoded picture bytes");
        }
        long begun = clock.GetTimestamp();
        playout.BeginGop(completed);
        await playout.WaitAsync(completed.Pictures[0].Offset, CancellationToken.None);
        Near(clock.GetElapsedTime(begun).TotalMilliseconds, 1500, 0.001, "first complete GOP did not apply the configured reserve");
        foreach (var picture in completed.Pictures.Skip(1))
            await playout.WaitAsync(picture.Offset, CancellationToken.None);
        Near(clock.GetElapsedTime(begun).TotalSeconds, 1.5 + 39 * 0.056, 0.00002, "pacing did not follow the normalized outbound clock");
        Check(waits.Count > 0 && waits.All(delay => delay > TimeSpan.Zero), "waiter received a nonpositive delay");
        clock.WallClock = DateTimeOffset.UnixEpoch.AddYears(80);
        long unchanged = clock.GetTimestamp();
        await playout.WaitAsync(completed.Pictures[^1].Offset, CancellationToken.None);
        Check(clock.GetTimestamp() == unchanged, "calendar-time change delayed an already due picture");

        // Variable camera intervals retain their relative cadence after scaling.
        var variable = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(variable.Push(Packet(0, 1000, 0, true)) == null, "variable GOP emitted its first keyframe early");
        Check(variable.Push(Packet(1, 5500, 60_000)) == null, "variable GOP emitted before closing");
        Check(variable.Push(Packet(2, 14_500, 180_000)) == null, "variable GOP emitted before closing");
        var varied = Close(variable, Packet(3, 19_000, 240_000, true));
        Check(varied.Pictures.Count == 3, "variable-cadence GOP lost pictures");
        Near(varied.Pictures[1].Offset.TotalMilliseconds, 60, 0.02, "short camera interval was not scaled");
        Near(varied.Pictures[2].Offset.TotalMilliseconds, 180, 0.02, "long camera interval was flattened");
    }

    public static async Task WrapAndCancel()
    {
        const uint start = uint.MaxValue - 1000;
        var wrap = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(wrap.Push(Packet(0, start, 0, true)) == null, "wrapped GOP emitted early");
        Check(wrap.Push(Packet(1, unchecked(start + 4500), 60_000)) == null, "wrapped GOP emitted early");
        var first = Close(wrap, Packet(2, unchecked(start + 9000), 120_000, true));
        Check(first.Pictures.Count == 2, "ordinary RTP wrap was discarded as a discontinuity");
        Check(unchecked(first.Pictures[1].RtpTimestamp - first.Pictures[0].RtpTimestamp) == 5400,
            "unwrapped source interval was not converted back modulo 32 bits");

        // Reset drops builder/schedule state, but an established outgoing clock stays monotonic modulo uint.
        uint previous = first.Pictures[^1].RtpTimestamp;
        wrap.Reset();
        Check(wrap.BufferedBytes == 0 && wrap.BufferedFrames == 0, "reset retained pending pictures");
        Check(wrap.Push(Packet(3, 50, 1_000_000, epoch: 1)) == null && wrap.BufferedFrames == 0,
            "recovery accepted a predicted picture before a keyframe");
        Check(wrap.Push(Packet(4, 100, 1_050_000, true, epoch: 1)) == null, "reset recovery emitted an unclosed keyframe");
        var recovered = Close(wrap, Packet(5, 4600, 1_110_000, true, epoch: 1));
        uint forward = unchecked(recovered.Pictures[0].RtpTimestamp - previous);
        Check(forward is > 0 and <= 450_000, "reset moved the established outgoing RTP clock backwards");
        Check(recovered.SourceEpoch == 1 && recovered.Pictures.All(picture => picture.SourceEpoch == 1),
            "old publisher epoch survived recovery");

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ManualTimeProvider();
        var cancellable = new GopVideoPlayout(VideoCodec.H265, 1500, clock, async (_, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        });
        cancellable.Push(Packet(0, 0, 0, true));
        var ready = Close(cancellable, Packet(1, 4500, 50_000, true));
        cancellable.BeginGop(ready);
        using var cancel = new CancellationTokenSource();
        Task waiting = cancellable.WaitAsync(TimeSpan.Zero, cancel.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(1));
        cancel.Cancel();
        bool cancelled = false;
        try { await waiting.WaitAsync(TimeSpan.FromSeconds(1)); }
        catch (OperationCanceledException) { cancelled = true; }
        Check(cancelled, "PAUSE/TEARDOWN cancellation did not interrupt the playout reserve");
        using var alreadyCancelled = new CancellationTokenSource();
        alreadyCancelled.Cancel();
        cancelled = false;
        try { await cancellable.WaitAsync(TimeSpan.Zero, alreadyCancelled.Token); }
        catch (OperationCanceledException) { cancelled = true; }
        Check(cancelled, "an already cancelled pump could still output a due picture");

        // Recovery can cross an entire uint RTP-clock wrap; floating-point casts
        // to uint are not a substitute for explicit modulo integer arithmetic.
        var longClock = new ManualTimeProvider();
        var longPause = new GopVideoPlayout(VideoCodec.H265, 0, longClock);
        longPause.Push(Packet(0, 1000, 0, true));
        var beforeLongPause = longPause.BeginGop(Close(longPause, Packet(1, 5500, 50000, true)));
        longPause.MarkSent(beforeLongPause.Pictures[0]);
        longPause.Reset(); longClock.Advance(TimeSpan.FromHours(14));
        longPause.Push(Packet(2, 100, 100000, true, epoch: 1));
        var afterLongPause = longPause.BeginGop(Close(longPause, Packet(3, 4600, 150000, true, epoch: 1)));
        Check(unchecked(afterLongPause.Pictures[0].RtpTimestamp - beforeLongPause.Pictures[0].RtpTimestamp)
            == unchecked((uint)(14L * 3600 * 90000)), "long recovery did not wrap modulo 32-bit RTP time");

        // A real pause must appear in the same session's media clock. Only a
        // successfully sent picture establishes that reference, not reserved
        // timestamps belonging to the unsent remainder of a completed GOP.
        var recoveryClock = new ManualTimeProvider();
        var recovery = new GopVideoPlayout(VideoCodec.H265, 1500, recoveryClock, (delay, token) =>
        {
            token.ThrowIfCancellationRequested();
            recoveryClock.Advance(delay);
            return Task.CompletedTask;
        });
        recovery.Push(Packet(0, 1000, 0, true));
        recovery.Push(Packet(1, 5500, 50_000));
        var beforePause = Close(recovery, Packet(2, 10_000, 100_000, true));
        recoveryClock.Advance(TimeSpan.FromMilliseconds(100));
        beforePause = recovery.BeginGop(beforePause);
        await recovery.WaitAsync(beforePause.Pictures[0].Offset, CancellationToken.None);
        recovery.MarkSent(beforePause.Pictures[0]);
        uint lastSent = beforePause.Pictures[0].RtpTimestamp;
        long lastSentAt = recoveryClock.GetTimestamp();
        recovery.Reset(); // the second picture of beforePause was never sent
        recoveryClock.Advance(TimeSpan.FromSeconds(5));
        long sourceRestart = recoveryClock.GetTimestamp();
        recovery.Push(Packet(3, 100, sourceRestart, true, epoch: 1));
        var afterPause = Close(recovery, Packet(4, 9100, sourceRestart + 100_000, true, epoch: 1));
        recoveryClock.Advance(TimeSpan.FromMilliseconds(100));
        afterPause = recovery.BeginGop(afterPause);
        await recovery.WaitAsync(afterPause.Pictures[0].Offset, CancellationToken.None);
        long actualWallTicks = (long)Math.Round(recoveryClock.GetElapsedTime(lastSentAt).TotalSeconds * 90000);
        Check(unchecked(afterPause.Pictures[0].RtpTimestamp - lastSent) == (uint)actualWallTicks,
            "recovery hid the wall pause/reserve or counted an unsent old GOP tail as media time");
        recovery.MarkSent(afterPause.Pictures[0]);
    }

    private static byte[] SizedKeyframe(int totalBytes)
    {
        var bytes = new byte[totalBytes];
        Array.Fill(bytes, (byte)0x55);
        bytes[0] = bytes[1] = bytes[2] = 0; bytes[3] = 1;
        bytes[4] = 0x26; bytes[5] = 0x01; bytes[6] = 0x80;
        return bytes;
    }

    public static Task BoundsAndRecovery()
    {
        const int byteCap = 6 * 1024 * 1024;
        var bytes = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(bytes.Push(Packet(0, 0, 0, true, SizedKeyframe(byteCap))) == null,
            "exact byte-cap GOP was emitted before closing");
        Check(bytes.BufferedBytes == byteCap && bytes.BufferedFrames == 1, "exact 6 MiB boundary was rejected or miscounted");
        var exactBytes = Close(bytes, Packet(1, 90_000, 1_000_000, true));
        Check(exactBytes.Pictures.Count == 1 && exactBytes.Pictures[0].AnnexB.Length == byteCap,
            "closing an exactly bounded GOP changed its bytes");

        var overflow = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(overflow.Push(Packet(0, 0, 0, true, SizedKeyframe(byteCap + 1))) == null,
            "oversized single picture was sent");
        Check(overflow.BufferedBytes == 0 && overflow.BufferedFrames == 0, "oversized picture remained retained");
        Check(overflow.Push(Packet(1, 4500, 50_000)) == null && overflow.BufferedFrames == 0,
            "overflow recovery accepted a predicted picture");
        Check(overflow.Push(Packet(2, 9000, 100_000, true)) == null, "overflow recovery emitted before a complete GOP");
        Check(Close(overflow, Packet(3, 13_500, 150_000, true)).Pictures.Count == 1,
            "overflow recovery failed at the next keyframe");

        var frames = new GopVideoPlayout(VideoCodec.H265, 0);
        for (int i = 0; i < 900; i++)
            Check(frames.Push(Packet(i, (uint)(i * 450), i * 5000L, i == 0)) == null, "900-frame boundary emitted early");
        Check(frames.BufferedFrames == 900, "900th picture hit the frame cap prematurely");
        Check(Close(frames, Packet(900, 405_000, 4_500_000, true)).Pictures.Count == 900,
            "900-picture GOP did not close at the exact cap");
        var frameOverflow = new GopVideoPlayout(VideoCodec.H265, 0);
        for (int i = 0; i <= 900; i++) frameOverflow.Push(Packet(i, (uint)(i * 450), i * 5000L, i == 0));
        Check(frameOverflow.BufferedFrames == 0 && frameOverflow.BufferedBytes == 0,
            "901st picture did not discard the incomplete GOP");
        Check(frameOverflow.Push(Packet(901, 405_450, 4_505_000, true)) == null,
            "frame-cap recovery replayed the dropped GOP");
        Check(Close(frameOverflow, Packet(902, 405_900, 4_510_000, true)).Pictures.Count == 1,
            "frame-cap recovery did not reopen at a new keyframe");

        var fiveSeconds = new GopVideoPlayout(VideoCodec.H265, 0);
        fiveSeconds.Push(Packet(0, 0, 0, true));
        fiveSeconds.Push(Packet(1, 225_000, 2_500_000));
        Near(Close(fiveSeconds, Packet(2, 450_000, 5_000_000, true)).Duration.TotalSeconds, 5, 0.000001,
            "exact five-second wall boundary was rejected");
        foreach (long badArrival in new long[] { 0, -1, 5_000_001 })
        {
            var invalid = new GopVideoPlayout(VideoCodec.H265, 0);
            invalid.Push(Packet(0, 1000, 0, true));
            Check(invalid.Push(Packet(1, 5500, badArrival, true)) == null, "invalid GOP wall span was emitted");
        }
        var timestampJump = new GopVideoPlayout(VideoCodec.H265, 0);
        timestampJump.Push(Packet(0, 1000, 0, true));
        Check(timestampJump.Push(Packet(1, 1_115_000, 1_000_000)) == null && timestampJump.BufferedFrames == 0,
            "12-second camera timestamp jump invented a long picture interval");
        timestampJump.Push(Packet(2, 1_119_500, 1_050_000, true));
        Check(Close(timestampJump, Packet(3, 1_124_000, 1_100_000, true)).Pictures.Count == 1,
            "timestamp-discontinuity recovery did not wait for a new complete GOP");

        var epoch = new GopVideoPlayout(VideoCodec.H265, 0);
        epoch.Push(Packet(0, 1000, 0, true, epoch: 1));
        Check(epoch.Push(Packet(1, 5500, 50_000, epoch: 2)) == null && epoch.BufferedFrames == 0,
            "publisher change retained old-epoch pictures");
        epoch.Push(Packet(2, 10_000, 100_000, true, epoch: 2));
        Check(Close(epoch, Packet(3, 14_500, 150_000, true, epoch: 2)).Pictures.All(picture => picture.SourceEpoch == 2),
            "recovered GOP contains pictures from a stopped publisher");
        return Task.CompletedTask;
    }

    public static Task MultiAuAndMetadata()
    {
        byte[] bundled = Annex(Vps, Sps, Pps, Aud, PrefixSei, Idr, SuffixSei, Aud, PrefixSei, Predicted, Continuation);
        var split = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(split.Push(Packet(0, 1000, 0, true, bundled)) == null, "bundled buffer emitted before closing GOP");
        var complete = Close(split, Packet(1, 10_000, 100_000, true));
        Check(complete.Pictures.Count == 2, "two first slices or one continuation slice were counted incorrectly");
        Check(complete.Pictures[0].Keyframe && !complete.Pictures[1].Keyframe,
            "a keyframe anywhere in the source buffer was copied to every split picture");
        Check(complete.Pictures[0].HasSps && !complete.Pictures[1].HasSps, "parameter sets were stripped or assigned to every AU");
        var first = H26x.SplitNals(complete.Pictures[0].AnnexB);
        var second = H26x.SplitNals(complete.Pictures[1].AnnexB);
        Check(first.Count == 7 && second.Count == 4, "AUD/prefix/suffix metadata was lost or assigned to the wrong picture");
        Check(first.Select(nal => H26x.H265NalType(nal.Span)).SequenceEqual(new[] { 32, 33, 34, 35, 39, 19, 40 })
            && second.Select(nal => H26x.H265NalType(nal.Span)).SequenceEqual(new[] { 35, 39, 1, 1 }),
            "prefix SEI did not stay before its picture or suffix SEI did not stay after it");
        var reconstructed = first.Concat(second).SelectMany(nal => nal.ToArray()).ToArray();
        Check(reconstructed.SequenceEqual(H26x.SplitNals(bundled).SelectMany(nal => nal.ToArray())),
            "splitting changed or omitted original encoded NAL bytes");
        Near(complete.Pictures[1].Offset.TotalMilliseconds, 50, 0.02,
            "bundled pictures did not receive individual normalized timestamps");
        Check(complete.Pictures[0].RtpTimestamp != complete.Pictures[1].RtpTimestamp,
            "bundled pictures still share one RTP timestamp");

        var metadata = new GopVideoPlayout(VideoCodec.H265, 0);
        Check(metadata.Push(Packet(0, 0, 0, false, Annex(Vps, Sps, Pps, Aud, PrefixSei, SuffixSei))) == null
            && metadata.BufferedFrames == 0, "metadata-only input invented a picture/marker");
        var samePtsMetadata = new GopVideoPlayout(VideoCodec.H265, 0);
        samePtsMetadata.Push(Packet(0, 1000, 0, true));
        Check(samePtsMetadata.Push(Packet(1, 1000, 1000, false, Annex(PrefixSei))) == null
            && samePtsMetadata.BufferedFrames == 1, "same-PTS metadata discarded a legitimate pending GOP");
        samePtsMetadata.Push(Packet(2, 5500, 50_000));
        var withMetadata = Close(samePtsMetadata, Packet(3, 10_000, 100_000, true));
        Check(withMetadata.Pictures.Count == 2, "same-PTS metadata invented an extra picture");
        Check(withMetadata.Pictures.SelectMany(picture => H26x.SplitNals(picture.AnnexB))
            .Select(nal => H26x.H265NalType(nal.Span)).SequenceEqual(new[] { 19, 39, 1 }),
            "same-PTS metadata changed encoded NAL order or was dropped");
        var multiSlice = new GopVideoPlayout(VideoCodec.H265, 0);
        multiSlice.Push(Packet(0, 0, 0, true, Annex(Idr, [0x26, 0x01, 0x40, 0x80])));
        Check(Close(multiSlice, Packet(1, 4500, 50_000, true)).Pictures.Count == 1,
            "two slices of one HEVC picture became two output pictures");

        // first_mb_in_slice ue(v)=0 begins with 1; ue(v)=1 begins with 010.
        var h264 = new GopVideoPlayout(VideoCodec.H264, 0);
        byte[] avc = Annex([0x67, 0x80], [0x68, 0x80], [0x09, 0xF0], [0x65, 0x80, 0x80],
            [0x65, 0x40, 0x80], [0x09, 0xF0], [0x41, 0x80, 0x80]);
        h264.Push(Packet(0, 1000, 0, true, avc));
        var avcGop = Close(h264, Packet(1, 10_000, 100_000, true, Annex([0x65, 0x80, 0x80])));
        Check(avcGop.Pictures.Count == 2 && avcGop.Pictures[0].Keyframe && !avcGop.Pictures[1].Keyframe,
            "H264 Exp-Golomb zero/continuation boundaries were misclassified");
        Check(avcGop.Pictures.SelectMany(picture => H26x.SplitNals(picture.AnnexB))
            .SelectMany(nal => nal.ToArray()).SequenceEqual(H26x.SplitNals(avc).SelectMany(nal => nal.ToArray())),
            "H264 split stripped parameter/AUD NAL bytes");
        return Task.CompletedTask;
    }
}
