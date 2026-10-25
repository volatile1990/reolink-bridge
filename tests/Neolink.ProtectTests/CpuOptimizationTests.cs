using System.Runtime.InteropServices;
using System.Diagnostics;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class CpuOptimizationTests
{
    private static void Check(bool condition, string message)
    { if (!condition) throw new Exception(message); }

    private static byte[] Annex(params byte[][] nals) =>
        nals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();

    public static async Task SubMillisecondWaitYields()
    {
        var clock = new TimerClock();
        var playout = new GopVideoPlayout(VideoCodec.H265, 0, clock);
        byte[] data = Annex([0x26, 1, 0x80, 0x55]);
        HubVideo Packet(long arrival) => new(0, data, true, (uint)(arrival * 90 / 1000))
        { ArrivalTimestamp = arrival, ArrivalTimestampFrequency = 1_000_000 };
        playout.Push(Packet(0));
        var gop = playout.Push(Packet(50_000)) ?? throw new Exception("test GOP missing");
        playout.BeginGop(gop);
        Task waiting = playout.WaitAsync(TimeSpan.FromMicroseconds(500), CancellationToken.None);
        Check(!waiting.IsCompleted && clock.Pending != null,
            "positive sub-ms presentation wait completed synchronously instead of yielding");
        Check(clock.Pending!.Due == TimeSpan.FromMilliseconds(1), "sub-ms wait did not request the first usable timer tick");
        clock.Fire();
        await waiting.WaitAsync(TimeSpan.FromSeconds(1));
        Check(clock.Created == 1, "one presentation wait repeatedly allocated timers");
    }

    public static Task SharedNalOwnershipAndPacketParity()
    {
        foreach (var codec in new[] { VideoCodec.H264, VideoCodec.H265 })
        {
            byte[] bytes = codec == VideoCodec.H264
                ? Annex([0x67, 0x42, 0x80], [0x68, 0x80], [0x65, 0x80, 0x55], [0x41, 0x80, 0x66])
                : Annex([0x40, 1, 0x80], [0x42, 1, 0x80], [0x44, 1, 0x80], [0x26, 1, 0x80, 0x55], [0x02, 1, 0x80, 0x66]);
            var hub = new StreamHub("synthetic-shared-nals");
            var (firstId, first) = hub.Subscribe();
            var (secondId, second) = hub.Subscribe();
            try
            {
                hub.PublishVideo(new VideoFrame(codec, true, 0, null, bytes));
                Check(first.TryRead(out var a) && a is HubVideo, "first subscriber missed frame");
                Check(second.TryRead(out var b) && b is HubVideo, "second subscriber missed frame");
                var x = (HubVideo)a!; var y = (HubVideo)b!;
                var nals = x.ParsedNals ?? throw new Exception("published frame lacks shared NAL metadata");
                Check(ReferenceEquals(x.AnnexB, bytes) && ReferenceEquals(nals, y.ParsedNals),
                    "fan-out copied payload or recomputed NAL metadata");
                Check((x with { AnnexB = bytes.ToArray() }).ParsedNals == null,
                    "record clone changing the payload retained stale NAL slices");
                foreach (var nal in nals)
                    Check(MemoryMarshal.TryGetArray(nal, out var segment) && ReferenceEquals(segment.Array, bytes),
                        "shared NAL slice does not retain the original owned frame");
                var raw = FMp4.SplitAccessUnitsRaw(codec, bytes);
                var cached = FMp4.SplitAccessUnitsRaw(codec, nals);
                Check(raw.Units.SequenceEqual(cached.Units) && raw.Nals.Count == cached.Nals.Count,
                    "reused NAL boundaries changed diagnostic access units");
                var legacy = new RtpBatch(); var shared = new RtpBatch();
                new RtpPacketizer(96).PacketizeVideoInto(legacy, codec, bytes, 9000, 0);
                new RtpPacketizer(96).PacketizeVideoNalsInto(shared, codec, nals, 9000, 0);
                Check(legacy.Count == shared.Count, "shared packetization changed packet count");
                for (int i = 0; i < legacy.Count; i++)
                {
                    var old = legacy.PacketAt(i).Span; var current = shared.PacketAt(i).Span;
                    Check(old[1] == current[1] && old[4..8].SequenceEqual(current[4..8])
                        && old[12..].SequenceEqual(current[12..]), "shared packetization changed marker, RTP time or media bytes");
                }
                var (cachedId, replay) = hub.Subscribe();
                try
                {
                    Check(replay.TryRead(out var p) && p is HubVideo v && ReferenceEquals(nals, v.ParsedNals),
                        "GOP cache replay lost publisher-owned NAL metadata");
                }
                finally { hub.Unsubscribe(cachedId); }
            }
            finally { hub.Unsubscribe(firstId); hub.Unsubscribe(secondId); }
        }
        return Task.CompletedTask;
    }

    public static Task IncrementalLargeNalParity()
    {
        var large = Enumerable.Repeat((byte)0x55, 65_539).ToArray();
        large[0] = 0x26; large[1] = 1; large[2] = 0x80;
        byte[][] nals = [[0x40, 1, 0x80], large, [0x02, 1, 0x80, 0x66], [0x50, 1, 0x80]];
        byte[] canonical = Annex(nals);
        byte[] mixed = nals.SelectMany((nal, i) => (i % 2 == 0 ? new byte[] { 0, 0, 1 }
            : new byte[] { 0, 0, 0, 1 }).Concat(nal)).ToArray();
        byte[] padded = new byte[] { 0, 0 }.Concat(nals.SelectMany((nal, i) =>
            (i % 2 == 0 ? new byte[] { 0, 0, 1 } : new byte[] { 0, 0, 0, 0, 0, 1 }).Concat(nal)))
            .Concat(new byte[] { 0, 0 }).ToArray();
        foreach (byte[] input in new[] { canonical, mixed, padded, mixed.Concat(new byte[] { 0, 0, 1 }).ToArray() })
        {
            var expected = new HevcAccessUnitReader().Feed(input, endOfStream: true);
            foreach (int chunk in new[] { 1, 2, 3, 4, 7, 31, 4096, input.Length })
            {
                var reader = new HevcAccessUnitReader(); var actual = new List<HevcAccessUnit>();
                for (int i = 0; i < input.Length; i += chunk)
                    actual.AddRange(reader.Feed(input.AsSpan(i, Math.Min(chunk, input.Length - i))));
                actual.AddRange(reader.Feed([], endOfStream: true));
                Check(actual.Count == expected.Count, "large NAL chunking changed picture count");
                for (int i = 0; i < actual.Count; i++)
                    Check(actual[i].Keyframe == expected[i].Keyframe && actual[i].Data.SequenceEqual(expected[i].Data),
                        "large NAL chunk boundary changed encoded bytes or keyframe");
            }
        }
        return Task.CompletedTask;
    }

    public static async Task WriteGuardFastPathAndBudgets()
    {
        byte[] payload = [1, 2, 3];
        using var gate = new SemaphoreSlim(1, 1);
        using var immediate = new ProbeStream();
        int aborts = 0;
        Action abort = () => aborts++;
        for (int i = 0; i < 100; i++)
            await RtspWriteGuard.WriteAsync(immediate, gate, payload, TimeSpan.FromSeconds(1), abort, CancellationToken.None);
        long allocated = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
            await RtspWriteGuard.WriteAsync(immediate, gate, payload, TimeSpan.FromSeconds(1), abort, CancellationToken.None);
        allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
        Check(aborts == 0 && immediate.Writes == 1100 && gate.CurrentCount == 1,
            "immediate writes changed delivery, closed the connection or retained the gate");
        Check(allocated < 16_000, "immediate write path still allocates per packet: " + allocated);

        using var blocked = new ProbeStream(block: true);
        bool closedWhileHeld = false;
        Action close = () => { aborts++; closedWhileHeld = gate.CurrentCount == 0; blocked.Fail(); };
        await gate.WaitAsync();
        var elapsed = Stopwatch.StartNew();
        Task sending = RtspWriteGuard.WriteAsync(blocked, gate, payload, TimeSpan.FromMilliseconds(300), close, CancellationToken.None);
        await Task.Delay(180);
        gate.Release();
        bool timedOut = false;
        try { await sending.WaitAsync(TimeSpan.FromSeconds(2)); }
        catch (TimeoutException) { timedOut = true; }
        Check(timedOut && closedWhileHeld && aborts == 1 && gate.CurrentCount == 1,
            "stalled write did not abort before releasing its lock");
        Check(elapsed.Elapsed < TimeSpan.FromMilliseconds(440),
            "lock admission and socket write received separate timeout budgets");

        using var cancelled = new ProbeStream(block: true);
        using var stop = new CancellationTokenSource();
        Task pending = RtspWriteGuard.WriteAsync(cancelled, gate, payload, TimeSpan.FromSeconds(1), abort, stop.Token);
        stop.Cancel();
        bool sawCancellation = false;
        try { await pending.WaitAsync(TimeSpan.FromSeconds(2)); }
        catch (OperationCanceledException) { sawCancellation = true; }
        Check(sawCancellation && aborts == 1 && gate.CurrentCount == 1,
            "caller cancellation was changed into a stalled-client abort or lost the gate");
    }

    private sealed class ProbeStream(bool block = false) : Stream
    {
        private readonly TaskCompletionSource _pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Writes;
        public void Fail() => _pending.TrySetException(new IOException("synthetic closed stream"));
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            if (!block) return ValueTask.CompletedTask;
            return Block(cancellationToken);
        }
        private ValueTask Block(CancellationToken cancellationToken)
        {
            cancellationToken.Register(() => _pending.TrySetCanceled(cancellationToken));
            return new ValueTask(_pending.Task);
        }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class TimerClock : TimeProvider
    {
        private long _ticks;
        public PendingTimer? Pending;
        public int Created;
        public override long TimestampFrequency => 1_000_000;
        // A tiny monotonic advance also lets the unfixed synchronous loop finish
        // and fail the assertion, rather than hanging the regression runner.
        public override long GetTimestamp() => Interlocked.Increment(ref _ticks);
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        { Created++; return Pending = new PendingTimer(callback, state, dueTime); }
        public void Fire()
        {
            var pending = Pending ?? throw new Exception("no pending timer");
            Interlocked.Add(ref _ticks, (long)(pending.Due.TotalSeconds * TimestampFrequency));
            pending.Fire();
        }
    }

    private sealed class PendingTimer(TimerCallback callback, object? state, TimeSpan due) : ITimer
    {
        public TimeSpan Due { get; private set; } = due;
        public bool Change(TimeSpan dueTime, TimeSpan period) { Due = dueTime; return true; }
        public void Fire() => callback(state);
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
