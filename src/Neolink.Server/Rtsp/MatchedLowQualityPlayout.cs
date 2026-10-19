// Protect-only packet pacing against an observed HQ clock; AGPL-3.0.
using Neolink.Streaming;

namespace Neolink.Rtsp;

/// <summary>Waits on the RTSP pump's existing bounded subscriber queue. Encoded bytes,
/// RTP timestamps and AAC samples stay unchanged. Input provenance subtracts encoder
/// latency instead of blindly adding a fixed delay to the encoded output arrival.</summary>
internal sealed class MatchedLowQualityPlayout(IStreamHub hub, HqPlayoutReference reference,
    TimeProvider? clock = null, Func<TimeSpan, CancellationToken, Task>? wait = null)
{
    internal const double MaximumLateSeconds = 0.25;
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
            CheckLowFresh();
            if (reference.TryGetSnapshot(out var current)) return BindReference(current);
            if (_binding != null) throw Failure("Matched LQ HQ timing unavailable");
            if (++polls > 160 || _clock.GetElapsedTime(started).TotalSeconds >= 8)
                throw Failure("Matched LQ initial HQ timing timed out");
            var delay = TimeSpan.FromMilliseconds(50);
            if (_wait != null) await _wait(delay, ct).ConfigureAwait(false);
            else await Task.Delay(delay, _clock, ct).ConfigureAwait(false);
            _startupWaitMs = _clock.GetElapsedTime(started).TotalMilliseconds;
        }
    }

    public async Task WaitAsync(HubPacket packet, AudioTrackInfo? descriptionAudio, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var timing = await InitialReferenceAsync(ct).ConfigureAwait(false);
        long sourceTimestamp;
        long frequency;
        switch (packet)
        {
            case HubVideo video:
                if (video.SourceEpoch != _lowEpoch)
                    throw Failure("Matched LQ received video from another source epoch");
                if (video.AnnexB.Length is < 1 or > 6 * 1024 * 1024)
                    throw Failure("Matched LQ video exceeds its payload bound");
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
                ValidateFrequency(frequency);
                if (_previousVideoSource is { } previous && sourceTimestamp <= previous)
                    throw Failure("Matched LQ video provenance is not increasing");
                _previousVideoSource = sourceTimestamp;
                if (video.ArrivalTimestamp is not { } published)
                    throw Failure("Matched LQ video lacks output arrival");
                ValidateFrequency(video.ArrivalTimestampFrequency);
                _lastEncoderAgeMs = (published - (double)sourceTimestamp) * 1000 / frequency;
                _lastQueueAgeMs = (_clock.GetTimestamp() - (double)published) * 1000 / frequency;
                break;
            case HubAudioAac audio:
                if (audio.SourceEpoch != _lowEpoch || audio.ArrivalTimestamp == null)
                    throw Failure("Matched LQ AAC lacks current arrival provenance");
                var format = descriptionAudio;
                if (format is not { IsAac: true, SampleRate: >= 8000 and <= 96000, Channels: >= 1 and <= 2,
                    AudioSpecificConfig.Length: >= 2 } || !SameAudio(format, hub.Audio)
                    || (_audioFormat != null && !SameAudio(format, _audioFormat)))
                    throw Failure("Matched LQ requires unchanged original AAC");
                _audioFormat ??= format;
                frequency = audio.ArrivalTimestampFrequency;
                ValidateFrequency(frequency);
                if (audio.BlockUnits is < 1 or > 512 || audio.BlockIndex < 0 || audio.BlockIndex >= audio.BlockUnits
                    || audio.Au.Length is < 1 or > 8184)
                    throw Failure("Matched LQ AAC metadata exceeds its bounds");
                double observed = audio.ArrivalTimestamp.Value / (double)frequency
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

        _lastDelayMs = timing.DelaySeconds * 1000;
        double due = sourceTimestamp + timing.DelaySeconds * frequency;
        double remaining = (due - _clock.GetTimestamp()) / frequency;
        if (remaining > HqPlayoutReference.MaximumDelaySeconds)
            throw Failure("Matched LQ wait exceeds its bound");
        long began = _clock.GetTimestamp();
        int waits = 0;
        while (remaining > 0)
        {
            if (++waits > 64) throw Failure("Matched LQ clock failed to advance");
            var delay = TimeSpan.FromSeconds(remaining);
            if (_wait != null) await _wait(delay, ct).ConfigureAwait(false);
            else await Task.Delay(delay, _clock, ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
            var current = CheckedReference();
            if (Math.Abs(current.DelaySeconds - timing.DelaySeconds) > MaximumLateSeconds)
                throw Failure("Matched LQ HQ delay moved during its wait");
            // Keep this packet's original deadline: a changing reference must not
            // extend a queued packet indefinitely or silently replay an old source.
            remaining = (due - _clock.GetTimestamp()) / frequency;
        }
        CheckedReference();
        double late = (_clock.GetTimestamp() - due) / frequency;
        _maximumLateMs = Math.Max(_maximumLateMs, late * 1000);
        if (late > MaximumLateSeconds)
            throw Failure("Matched LQ packet missed its HQ deadline");
        _totalWaitMs += _clock.GetElapsedTime(began).TotalMilliseconds;
        _lastDeliveredSourceAgeMs = (_clock.GetTimestamp() - (double)sourceTimestamp) * 1000 / frequency;
        if (packet is HubVideo) _videoPackets++; else _audioPackets++;
    }

    private void ValidateFrequency(long frequency)
    {
        if (frequency <= 0 || frequency != _clock.TimestampFrequency)
            throw Failure("Matched LQ provenance uses another monotonic clock");
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
        timeBasis = "source-arrival-estimate-and-original-aac-samples", captureSynchronizationVerified = false
    };
}
