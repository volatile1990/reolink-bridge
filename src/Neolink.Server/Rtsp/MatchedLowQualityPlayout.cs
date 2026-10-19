// Protect-only packet pacing against an observed HQ clock; AGPL-3.0.
using Neolink.Streaming;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Neolink.Rtsp;

/// <summary>Schedules a bounded set of encoded packet references against HQ timing. Encoded bytes,
/// RTP timestamps and AAC samples stay unchanged. Input provenance subtracts encoder
/// latency instead of blindly adding a fixed delay to the encoded output arrival.</summary>
internal sealed class MatchedLowQualityPlayout(IStreamHub hub, HqPlayoutReference reference,
    TimeProvider? clock = null, Func<TimeSpan, CancellationToken, Task>? wait = null)
{
    internal const double MaximumLateSeconds = 0.25;
    internal const double MaximumStartupSeconds = 8;
    internal const int MaxPendingPackets = 512, MaxPendingBytes = 6 * 1024 * 1024;
    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private readonly Func<TimeSpan, CancellationToken, Task>? _wait = wait;
    private readonly long _lowEpoch = hub.SourceEpoch;
    private HqPlayoutSnapshot? _binding;
    private long? _previousVideoSource;
    private uint? _previousAudioRtp;
    private long _audioSamples;
    private double _audioAnchor, _audioPhase;
    private AudioTrackInfo? _audioFormat;
    private long _videoPackets, _audioPackets, _failures;
    private double _totalWaitMs, _maximumLateMs, _lastEncoderAgeMs, _lastDelayMs,
        _lastDeliveredSourceAgeMs, _lastQueueAgeMs, _startupWaitMs;
    private readonly Queue<PlannedPacket> _videoQueue = new(), _audioQueue = new();
    private readonly PriorityQueue<PlannedPacket, (double Due, long Index)> _heads = new();
    private PlannedPacket? _readyPacket;
    private int _pendingPackets, _maximumPendingPackets, _unchangedTimerWakeups;
    private long _pendingBytes, _maximumPendingBytes;
    private bool _readerActive;
    private long? _startupBegan;
    private long _startupDiscardedGops, _startupDiscardedPackets, _startupDiscardedBytes;
    private double _maximumStartupDiscardLateMs;

    private sealed record PlannedPacket(HubPacket Packet, long SourceTimestamp, long Frequency,
        double Due, HqPlayoutSnapshot Timing, double? EncoderAgeMs, double? InputQueueAgeMs);

    /// <summary>Buffer immutable packet references, with one FIFO per track. Only
    /// their heads compete by deadline, so a small valid HQ-delay change cannot
    /// reorder predictive video or AAC. The extra queue is bounded at 512 packets
    /// and 6 MiB including its waiting head; typical two-second LQ uses about 72
    /// packets. The existing input channel retains its separate 2048-packet cap.</summary>
    public async IAsyncEnumerable<HubPacket> ReadAsync(ChannelReader<HubPacket> reader, AudioTrackInfo? audio,
        [EnumeratorCancellation] CancellationToken ct, bool videoEnabled = true)
    {
        if (_readerActive) throw new InvalidOperationException("Matched LQ reader already active");
        _readerActive = true;
        _startupBegan ??= _clock.GetTimestamp();
        long? lastIndex = null;
        bool started = !videoEnabled, inputCompleted = false;
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                CheckStartupDeadline();
                CheckLowFresh();
                if (_readyPacket != null) throw Failure("Matched LQ previous packet was not marked sent");
                while (reader.TryRead(out var packet))
                {
                    ct.ThrowIfCancellationRequested(); CheckStartupDeadline(); CheckLowFresh();
                    if (lastIndex is { } previous && packet.Index != previous + 1)
                        throw Failure("Matched LQ subscriber lost packets; reconnect at a keyframe");
                    lastIndex = packet.Index; // Includes Opus packets ignored by original-AAC sessions.
                    if (packet is HubVideo video)
                    {
                        if (!videoEnabled) continue;
                        ValidateVideoEnvelope(video);
                        if (!started && !video.Keyframe) continue;
                        started = true;
                    }
                    else if (packet is HubAudioAac aac)
                    {
                        if (audio == null) continue;
                        ValidateAudioEnvelope(aac, audio);
                        if (!started) continue;
                    }
                    else if (packet is HubAudioPcm && audio != null)
                        throw Failure("Matched LQ requires original AAC");
                    else continue;
                    Enqueue(await PlanAsync(packet, audio, ct).ConfigureAwait(false));
                    // The cached first GOP can age while its first HQ reference
                    // becomes available. Discard it before planning its backlog;
                    // a following keyframe still in the input channel is retained.
                    if (videoEnabled && DiscardExpiredStartupGop()) started = false;
                }
                if (!_heads.TryPeek(out var next, out _))
                {
                    if (inputCompleted || !await WaitForInputAsync(reader, ct).ConfigureAwait(false)) yield break;
                    continue;
                }
                VerifyPlan(next);
                if (videoEnabled && DiscardExpiredStartupGop()) { started = false; continue; }
                if (Remaining(next) > 0)
                {
                    if (inputCompleted) await WaitPlanAsync(next, ct).ConfigureAwait(false);
                    else
                    {
                        inputCompleted = await WaitForInputOrDeadlineAsync(reader, next, ct).ConfigureAwait(false);
                        continue; // New video can have an earlier source instant than queued AAC.
                    }
                }
                CheckStartupDeadline();
                if (videoEnabled && DiscardExpiredStartupGop()) { started = false; continue; }
                VerifyDeadline(next);
                Dequeue(next);
                _readyPacket = next;
                // Both readiness/timer tasks have completed before handing a packet to the sender.
                yield return next.Packet;
            }
        }
        finally
        {
            _videoQueue.Clear(); _audioQueue.Clear(); _heads.Clear();
            _pendingBytes = _pendingPackets = 0; _readyPacket = null; _readerActive = false;
        }
    }

    public LowQualityPlayoutAnchor MarkSent(HubPacket packet)
    {
        var ready = _readyPacket;
        if (ready == null || !ReferenceEquals(ready.Packet, packet))
            throw Failure("Matched LQ sent packet does not match its ready head");
        RecordSent(ready); _readyPacket = null;
        return new(_lowEpoch, ready.Due, ready.Frequency);
    }

    private void Enqueue(PlannedPacket packet)
    {
        int bytes = PayloadBytes(packet.Packet);
        if (_pendingPackets >= MaxPendingPackets || _pendingBytes + bytes > MaxPendingBytes)
            throw Failure("Matched LQ pending packet budget exceeded");
        var queue = packet.Packet is HubVideo ? _videoQueue : _audioQueue;
        queue.Enqueue(packet);
        if (queue.Count == 1) _heads.Enqueue(packet, (packet.Due, packet.Packet.Index));
        _pendingPackets++; _pendingBytes += bytes;
        _maximumPendingPackets = Math.Max(_maximumPendingPackets, _pendingPackets);
        _maximumPendingBytes = Math.Max(_maximumPendingBytes, _pendingBytes);
    }

    private void Dequeue(PlannedPacket packet)
    {
        _heads.Dequeue();
        var queue = packet.Packet is HubVideo ? _videoQueue : _audioQueue;
        queue.Dequeue(); _pendingPackets--; _pendingBytes -= PayloadBytes(packet.Packet);
        if (queue.TryPeek(out var next)) _heads.Enqueue(next, (next.Due, next.Packet.Index));
    }

    private static int PayloadBytes(HubPacket packet) => packet switch
    { HubVideo video => video.AnnexB.Length, HubAudioAac audio => audio.Au.Length, _ => 0 };

    private bool HasSent => _videoPackets != 0 || _audioPackets != 0;

    private double StartupRemainingSeconds => _startupBegan is { } began
        ? MaximumStartupSeconds - _clock.GetElapsedTime(began).TotalSeconds : MaximumStartupSeconds;

    private void CheckStartupDeadline()
    {
        if (_readerActive && !HasSent && StartupRemainingSeconds <= 0)
            throw Failure("Matched LQ initial keyframe and timing timed out");
    }

    private bool DiscardExpiredStartupGop()
    {
        if (HasSent || !_heads.TryPeek(out var first, out _)) return false;
        CheckStartupDeadline(); VerifyPlan(first);
        double late = -Remaining(first);
        if (late <= MaximumLateSeconds) return false;
        // Discarding cannot hide a timing-generation change or a queued plan
        // whose individual reference moved beyond the existing tolerance.
        foreach (var packet in _videoQueue) VerifyPlan(packet);
        foreach (var packet in _audioQueue) VerifyPlan(packet);
        _startupDiscardedGops++;
        _startupDiscardedPackets += _pendingPackets; _startupDiscardedBytes += _pendingBytes;
        _maximumStartupDiscardLateMs = Math.Max(_maximumStartupDiscardLateMs, late * 1000);
        _videoQueue.Clear(); _audioQueue.Clear(); _heads.Clear();
        _pendingPackets = 0; _pendingBytes = 0;
        _previousVideoSource = null; _previousAudioRtp = null;
        _audioSamples = 0; _audioAnchor = _audioPhase = 0;
        // Keep the fixed audio format, source/HQ binding, input sequence and all
        // high-water/failure history. This is only a pre-send GOP resynchronization.
        return true;
    }

    private async Task<bool> WaitForInputAsync(ChannelReader<HubPacket> reader, CancellationToken ct)
    {
        CheckStartupDeadline();
        if (HasSent) return await reader.WaitToReadAsync(ct).ConfigureAwait(false);
        double remaining = StartupRemainingSeconds;
        if (remaining <= 0) throw Failure("Matched LQ initial keyframe and timing timed out");
        using var wake = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<bool> input = reader.WaitToReadAsync(wake.Token).AsTask();
        try
        {
            return await input.WaitAsync(TimeSpan.FromSeconds(remaining), _clock, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            CheckLowFresh();
            if (_binding != null) CheckedReference();
            throw Failure("Matched LQ initial keyframe and timing timed out");
        }
        finally
        {
            wake.Cancel();
            try { await input.ConfigureAwait(false); }
            catch (OperationCanceledException) when (wake.IsCancellationRequested) { }
        }
    }

    public void CheckFresh() => CheckedReference();

    private HqPlayoutSnapshot CheckedReference()
    {
        CheckLowFresh();
        if (!reference.TryGetSnapshot(out var current))
            throw Failure("Matched LQ HQ timing unavailable");
        return BindReference(current);
    }

    private void CheckLowFresh()
    {
        if (hub.SourceEpoch != _lowEpoch || !hub.LiveVideo
            || hub.GetVideoDiagnostics().LastVideoAgeMs is not (>= 0 and <= 5000))
            throw Failure("Matched LQ source changed or became stale");
    }

    private HqPlayoutSnapshot BindReference(HqPlayoutSnapshot current)
    {
        if (_binding is { } previous && (previous.SourceEpoch != current.SourceEpoch
            || previous.Generation != current.Generation
            || Math.Abs(previous.DelaySeconds - current.DelaySeconds) > MaximumLateSeconds))
            throw Failure("Matched LQ HQ timing changed; reconnect");
        _binding = current;
        return current;
    }

    private async Task<HqPlayoutSnapshot> InitialReferenceAsync(CancellationToken ct)
    {
        long started = _clock.GetTimestamp();
        int polls = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            CheckStartupDeadline();
            CheckLowFresh();
            if (reference.TryGetSnapshot(out var current)) return BindReference(current);
            if (_binding != null) throw Failure("Matched LQ HQ timing unavailable");
            if (++polls > 160 || _clock.GetElapsedTime(started).TotalSeconds >= MaximumStartupSeconds)
                throw Failure("Matched LQ initial HQ timing timed out");
            await DelayAsync(0.05, ct).ConfigureAwait(false);
            _startupWaitMs = _clock.GetElapsedTime(started).TotalMilliseconds;
        }
    }

    public async Task WaitAsync(HubPacket packet, AudioTrackInfo? descriptionAudio, CancellationToken ct)
    {
        var plan = await PlanAsync(packet, descriptionAudio, ct).ConfigureAwait(false);
        await WaitPlanAsync(plan, ct).ConfigureAwait(false);
        RecordSent(plan);
    }

    private async Task<PlannedPacket> PlanAsync(HubPacket packet, AudioTrackInfo? descriptionAudio, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        CheckStartupDeadline();
        var timing = await InitialReferenceAsync(ct).ConfigureAwait(false);
        CheckStartupDeadline();
        long sourceTimestamp;
        long frequency;
        double? encoderAge = null;
        double? inputQueueAge = null;
        switch (packet)
        {
            case HubVideo video:
                ValidateVideoEnvelope(video);
                if (video.SourceArrivalTimestamp is { } sourceArrival)
                {
                    sourceTimestamp = sourceArrival;
                    frequency = video.SourceArrivalTimestampFrequency;
                }
                else if (video.ArrivalTimestamp is { } arrival)
                {
                    sourceTimestamp = arrival;
                    frequency = video.ArrivalTimestampFrequency;
                }
                else throw Failure("Matched LQ video lacks arrival provenance");
                if (_previousVideoSource is { } previous && sourceTimestamp <= previous)
                    throw Failure("Matched LQ video provenance is not increasing");
                _previousVideoSource = sourceTimestamp;
                long published = video.ArrivalTimestamp!.Value;
                encoderAge = (published - (double)sourceTimestamp) * 1000 / frequency;
                inputQueueAge = (_clock.GetTimestamp() - (double)published) * 1000 / frequency;
                break;
            case HubAudioAac audio:
                ValidateAudioEnvelope(audio, descriptionAudio);
                var format = descriptionAudio!;
                _audioFormat ??= format;
                frequency = audio.ArrivalTimestampFrequency;
                double observed = audio.ArrivalTimestamp!.Value / (double)frequency
                    - (audio.BlockUnits - 1 - audio.BlockIndex) * 1024d / format.SampleRate;
                if (_previousAudioRtp is { } previousRtp)
                {
                    if (unchecked(audio.RtpTs - previousRtp) != 1024)
                        throw Failure("Matched LQ AAC lost sample continuity");
                    _audioSamples += 1024;
                    double predicted = _audioAnchor + _audioSamples / (double)format.SampleRate;
                    double deviation = observed - predicted - _audioPhase;
                    if (Math.Abs(deviation) > 5)
                        throw Failure("Matched LQ AAC source clock jumped");
                    // Gradual ADC/arrival drift is corrected within one percent of the
                    // sample duration. TCP burst gaps do not become single-sample holds.
                    double step = 1024d / format.SampleRate * 0.01;
                    _audioPhase += Math.Clamp(deviation, -step, step);
                }
                else { _audioAnchor = observed; _audioSamples = 0; }
                _previousAudioRtp = audio.RtpTs;
                sourceTimestamp = checked((long)Math.Round((_audioAnchor
                    + _audioSamples / (double)format.SampleRate + _audioPhase) * frequency));
                break;
            default:
                throw Failure("Matched LQ supports only video and original AAC");
        }

        double due = sourceTimestamp + timing.DelaySeconds * frequency;
        double remaining = (due - _clock.GetTimestamp()) / frequency;
        if (remaining > HqPlayoutReference.MaximumDelaySeconds)
            throw Failure("Matched LQ wait exceeds its bound");
        return new(packet, sourceTimestamp, frequency, due, timing, encoderAge, inputQueueAge);
    }

    private double Remaining(PlannedPacket packet) => (packet.Due - _clock.GetTimestamp()) / packet.Frequency;

    private void VerifyPlan(PlannedPacket packet)
    {
        var current = CheckedReference();
        if (current.SourceEpoch != packet.Timing.SourceEpoch || current.Generation != packet.Timing.Generation
            || Math.Abs(current.DelaySeconds - packet.Timing.DelaySeconds) > MaximumLateSeconds)
            throw Failure("Matched LQ HQ delay moved during its wait");
    }

    private async Task DelayAsync(double seconds, CancellationToken ct)
    {
        CheckStartupDeadline();
        if (_readerActive && !HasSent) seconds = Math.Min(seconds, StartupRemainingSeconds);
        // Task.Delay truncates fractional milliseconds; never turn a positive
        // remainder into a synchronous zero-ms wait or spin until a guard fires.
        var delay = TimeSpan.FromMilliseconds(Math.Max(1, Math.Ceiling(seconds * 1000)));
        if (_wait != null) await _wait(delay, ct).ConfigureAwait(false);
        else await Task.Delay(delay, _clock, ct).ConfigureAwait(false);
    }

    private async Task WaitPlanAsync(PlannedPacket packet, CancellationToken ct)
    {
        int waits = 0;
        while (Remaining(packet) > 0)
        {
            ct.ThrowIfCancellationRequested(); CheckStartupDeadline(); VerifyPlan(packet);
            if (++waits > 64) throw Failure("Matched LQ clock failed to advance");
            long began = _clock.GetTimestamp();
            try { await DelayAsync(Remaining(packet), ct).ConfigureAwait(false); }
            finally { _totalWaitMs += _clock.GetElapsedTime(began).TotalMilliseconds; }
        }
        ct.ThrowIfCancellationRequested(); CheckStartupDeadline(); VerifyPlan(packet);
    }

    private async Task<bool> WaitForInputOrDeadlineAsync(ChannelReader<HubPacket> reader, PlannedPacket packet, CancellationToken ct)
    {
        VerifyPlan(packet);
        using var wake = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<bool> input = reader.WaitToReadAsync(wake.Token).AsTask();
        long began = _clock.GetTimestamp();
        Task timer = DelayAsync(Remaining(packet), wake.Token);
        bool completed = false, timerWon = false;
        try
        {
            var winner = await Task.WhenAny(input, timer).ConfigureAwait(false);
            if (winner == input) completed = !await input.ConfigureAwait(false);
            else { await timer.ConfigureAwait(false); timerWon = true; }
        }
        finally
        {
            wake.Cancel();
            // WhenAll observes both tasks even when one faults. Readiness never
            // consumes a packet, including when it wins concurrently with the timer.
            try { await Task.WhenAll(input, timer).ConfigureAwait(false); }
            catch (OperationCanceledException) when (wake.IsCancellationRequested) { }
            finally { _totalWaitMs += _clock.GetElapsedTime(began).TotalMilliseconds; }
        }
        ct.ThrowIfCancellationRequested(); CheckStartupDeadline(); VerifyPlan(packet);
        if (timerWon && _clock.GetTimestamp() <= began)
        {
            if (++_unchangedTimerWakeups >= 64) throw Failure("Matched LQ clock failed to advance");
        }
        else _unchangedTimerWakeups = 0;
        return completed;
    }

    private void VerifyDeadline(PlannedPacket packet)
    {
        VerifyPlan(packet);
        double late = (_clock.GetTimestamp() - packet.Due) / packet.Frequency;
        _maximumLateMs = Math.Max(_maximumLateMs, late * 1000);
        if (late > MaximumLateSeconds)
            throw Failure("Matched LQ packet missed its HQ deadline");
    }

    private void RecordSent(PlannedPacket packet)
    {
        _lastDelayMs = packet.Timing.DelaySeconds * 1000;
        _lastDeliveredSourceAgeMs = (_clock.GetTimestamp() - (double)packet.SourceTimestamp) * 1000 / packet.Frequency;
        if (packet.EncoderAgeMs is { } encoderAge) _lastEncoderAgeMs = encoderAge;
        // Input queue age excludes deliberate scheduling time, as on the former
        // serial path; delivered source age above includes the actual delay.
        if (packet.InputQueueAgeMs is { } inputQueueAge) _lastQueueAgeMs = inputQueueAge;
        if (packet.Packet is HubVideo) _videoPackets++; else _audioPackets++;
        // The production caller has already sent these bytes; retain the actual
        // delivery metrics even if the send itself crossed the deadline.
        VerifyDeadline(packet);
    }

    private void ValidateFrequency(long frequency)
    {
        if (frequency <= 0 || frequency != _clock.TimestampFrequency)
            throw Failure("Matched LQ provenance uses another monotonic clock");
    }

    private void ValidateVideoEnvelope(HubVideo video)
    {
        if (video.SourceEpoch != _lowEpoch)
            throw Failure("Matched LQ received video from another source epoch");
        if (video.AnnexB.Length is < 1 or > 6 * 1024 * 1024)
            throw Failure("Matched LQ video exceeds its payload bound");
        if (video.SourceArrivalTimestamp == null && video.ArrivalTimestamp == null)
            throw Failure("Matched LQ video lacks arrival provenance");
        ValidateFrequency(video.SourceArrivalTimestamp != null
            ? video.SourceArrivalTimestampFrequency : video.ArrivalTimestampFrequency);
        if (video.ArrivalTimestamp == null)
            throw Failure("Matched LQ video lacks output arrival");
        ValidateFrequency(video.ArrivalTimestampFrequency);
    }

    private void ValidateAudioEnvelope(HubAudioAac audio, AudioTrackInfo? format)
    {
        if (audio.SourceEpoch != _lowEpoch || audio.ArrivalTimestamp == null)
            throw Failure("Matched LQ AAC lacks current arrival provenance");
        if (format is not { IsAac: true, SampleRate: >= 8000 and <= 96000, Channels: >= 1 and <= 2,
                AudioSpecificConfig.Length: >= 2 } || !SameAudio(format, hub.Audio)
            || (_audioFormat != null && !SameAudio(format, _audioFormat)))
            throw Failure("Matched LQ requires unchanged original AAC");
        ValidateFrequency(audio.ArrivalTimestampFrequency);
        if (audio.BlockUnits is < 1 or > 512 || audio.BlockIndex < 0 || audio.BlockIndex >= audio.BlockUnits
            || audio.Au.Length is < 1 or > 8184)
            throw Failure("Matched LQ AAC metadata exceeds its bounds");
    }

    private static bool SameAudio(AudioTrackInfo? expected, AudioTrackInfo? actual) => expected != null
        && actual != null && expected.IsAac == actual.IsAac && expected.SampleRate == actual.SampleRate
        && expected.Channels == actual.Channels && expected.AudioSpecificConfig != null
        && actual.AudioSpecificConfig != null && expected.AudioSpecificConfig.SequenceEqual(actual.AudioSpecificConfig);

    private InvalidDataException Failure(string message)
    {
        _failures++;
        return new InvalidDataException(message);
    }

    public object Diagnostics() => new
    {
        sourceEpoch = _lowEpoch, videoPackets = _videoPackets, audioPackets = _audioPackets,
        failures = _failures, targetDelayMs = _lastDelayMs, estimatedEncoderAgeMs = _lastEncoderAgeMs,
        maximumLateMs = _maximumLateMs, totalWaitMs = _totalWaitMs, maximumAllowedLateMs = MaximumLateSeconds * 1000,
        deliveredSourceAgeMs = _lastDeliveredSourceAgeMs, inputQueueAgeMs = _lastQueueAgeMs,
        initialReferenceWaitMs = _startupWaitMs,
        startupDiscardedGops = _startupDiscardedGops, startupDiscardedPackets = _startupDiscardedPackets,
        startupDiscardedBytes = _startupDiscardedBytes, maximumStartupDiscardLateMs = _maximumStartupDiscardLateMs,
        pendingPackets = _pendingPackets, pendingBytes = _pendingBytes,
        maximumPendingPackets = _maximumPendingPackets, maximumPendingBytes = _maximumPendingBytes,
        pendingPacketLimit = MaxPendingPackets, pendingByteLimit = MaxPendingBytes,
        timeBasis = "source-arrival-estimate-and-original-aac-samples", captureSynchronizationVerified = false
    };
}
