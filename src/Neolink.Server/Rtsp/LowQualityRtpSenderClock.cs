// Sender reports for explicit LQ mounts; original RTP and encoded media stay intact. AGPL-3.0.
using Neolink.Streaming;

namespace Neolink.Rtsp;

internal readonly record struct LowQualityPlayoutAnchor(long SourceEpoch, double PresentationTimestamp, long Frequency);
internal readonly record struct LowQualityRtpReport(uint? VideoRtpTimestamp, uint? AudioRtpTimestamp, DateTimeOffset UtcTimestamp);

/// <summary>One pump owns the two confirmed RTP-to-presentation mappings. Reports
/// describe observed source-arrival estimates, not recovered camera capture times.
/// There is no media queue, packet retimestamping or independent background task.</summary>
internal sealed class LowQualityRtpSenderClock(IStreamHub hub, TimeProvider? clock = null,
    bool requireSourceArrival = false)
{
    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private readonly (long At, DateTimeOffset Utc) _utcOrigin = CaptureClock(clock ?? TimeProvider.System);
    private long _epoch = hub.SourceEpoch;
    private TrackAnchor? _video, _audio;
    private TrackAnchor? _rawVideoObservation;
    private uint? _rawAudioPreviousRtp;
    private long _rawAudioSamples;
    private double _rawAudioOrigin, _rawAudioPhase;
    private AudioTrackInfo? _rawAudioFormat;
    private long _reports, _rejected;
    private long? _lastReportAt;
    private sealed record TrackAnchor(uint RtpTimestamp, double PresentationTimestamp, int Rate, long ConfirmedAt);

    public bool ObserveRawVideoSent(HubVideo packet)
    {
        ResetChangedEpoch();
        if (requireSourceArrival && packet.SourceArrivalTimestamp == null)
        { _video = null; _rejected++; return false; }
        long? source = packet.SourceArrivalTimestamp ?? packet.ArrivalTimestamp;
        long frequency = packet.SourceArrivalTimestamp != null
            ? packet.SourceArrivalTimestampFrequency : packet.ArrivalTimestampFrequency;
        if (source == null || !ValidInstant(packet.SourceEpoch, source.Value, frequency))
        { _video = null; _rejected++; return false; }
        var observation = new TrackAnchor(packet.RtpTs, source.Value, 90000, _clock.GetTimestamp());
        if (_rawVideoObservation is { } previous)
        {
            double sourceDelta = (source.Value - previous.PresentationTimestamp) / frequency;
            uint rtpDelta = unchecked(packet.RtpTs - previous.RtpTimestamp);
            // A wrap has a small positive modular delta. Backward/reset RTP has
            // a large one. Match the producer's five-second clock-jump bound,
            // without assuming fixed frame spacing or equal nominal rates.
            if (sourceDelta < 0)
            { _video = null; _rejected++; return false; }
            if (sourceDelta > 5)
            {
                // A long same-epoch pause needs a new baseline and a following
                // plausible observation before any report resumes.
                _rawVideoObservation = observation;
                _video = null; _rejected++; return false;
            }
            if (rtpDelta > 5 * 90000)
            { _video = null; _rejected++; return false; }
            // Do not refresh an old anchor with a duplicate RTP value or a
            // bundled observation whose source clock has not advanced.
            if (sourceDelta == 0 || rtpDelta == 0) return false;
        }
        _video = _rawVideoObservation = observation;
        return true;
    }

    public bool ObserveRawAacSent(HubAudioAac packet, AudioTrackInfo? format)
    {
        ResetChangedEpoch();
        if (packet.ArrivalTimestamp == null || !ValidInstant(packet.SourceEpoch,
                packet.ArrivalTimestamp.Value, packet.ArrivalTimestampFrequency)
            || format is not { IsAac: true, SampleRate: >= 8000 and <= 96000, Channels: >= 1 and <= 2,
                AudioSpecificConfig.Length: >= 2 } || !SameAudio(format, hub.Audio)
            || packet.BlockUnits is < 1 or > 512 || packet.BlockIndex < 0 || packet.BlockIndex >= packet.BlockUnits)
        { ResetRawAudio(); _rejected++; return false; }
        double observed = packet.ArrivalTimestamp.Value / (double)packet.ArrivalTimestampFrequency
            - (packet.BlockUnits - 1 - packet.BlockIndex) * 1024d / format.SampleRate;
        if (!ValidInstant(packet.SourceEpoch, Math.Round(observed * packet.ArrivalTimestampFrequency), packet.ArrivalTimestampFrequency))
        { ResetRawAudio(); _rejected++; return false; }

        if (_rawAudioPreviousRtp is { } previous && (unchecked(packet.RtpTs - previous) != 1024
            || !SameAudio(format, _rawAudioFormat))) ResetRawAudio();
        if (_rawAudioPreviousRtp != null)
        {
            _rawAudioSamples += 1024;
            double predicted = _rawAudioOrigin + _rawAudioSamples / (double)format.SampleRate;
            double deviation = observed - predicted - _rawAudioPhase;
            if (Math.Abs(deviation) > 5) ResetRawAudio();
            else
            {
                // The same bounded sample/arrival phase estimate as matched LQ.
                // Only the report's inverse mapping slews; RTP stays exactly 1024/AU.
                double step = 1024d / format.SampleRate * 0.01;
                _rawAudioPhase += Math.Clamp(deviation, -step, step);
            }
        }
        if (_rawAudioPreviousRtp == null)
        { _rawAudioOrigin = observed; _rawAudioSamples = 0; _rawAudioFormat = format; }
        _rawAudioPreviousRtp = packet.RtpTs;
        double presentation = Math.Round((_rawAudioOrigin + _rawAudioSamples / (double)format.SampleRate + _rawAudioPhase)
            * packet.ArrivalTimestampFrequency);
        if (!ValidInstant(packet.SourceEpoch, presentation, packet.ArrivalTimestampFrequency))
        { _audio = null; _rejected++; return false; }
        _audio = new(packet.RtpTs, presentation, format.SampleRate, _clock.GetTimestamp());
        return true;
    }

    public bool ObserveMatchedSent(HubPacket packet, LowQualityPlayoutAnchor timing)
    {
        ResetChangedEpoch();
        if (requireSourceArrival && packet is HubVideo required && (required.SourceArrivalTimestamp == null
            || required.SourceArrivalTimestampFrequency != timing.Frequency || required.SourceEpoch != timing.SourceEpoch))
        { _video = null; _rejected++; return false; }
        if (!ValidInstant(timing.SourceEpoch, timing.PresentationTimestamp, timing.Frequency))
        { _video = _audio = null; _rejected++; return false; }
        switch (packet)
        {
            case HubVideo video when video.SourceEpoch == timing.SourceEpoch:
                _video = new(video.RtpTs, timing.PresentationTimestamp, 90000, _clock.GetTimestamp());
                return true;
            case HubAudioAac audio when audio.SourceEpoch == timing.SourceEpoch
                && hub.Audio is { IsAac: true, SampleRate: >= 8000 and <= 96000 } format:
                _audio = new(audio.RtpTs, timing.PresentationTimestamp, format.SampleRate, _clock.GetTimestamp());
                return true;
            default:
                _video = _audio = null; _rejected++; return false;
        }
    }

    public bool TryGetReport(bool videoEnabled, bool audioEnabled, out LowQualityRtpReport report)
    {
        report = default;
        ResetChangedEpoch();
        long now = _clock.GetTimestamp();
        if ((!videoEnabled && !audioEnabled) || !hub.LiveVideo
            || hub.GetVideoDiagnostics().LastVideoAgeMs is not (>= 0 and <= 5000)
            || videoEnabled && !Fresh(_video, now) || audioEnabled && !Fresh(_audio, now)) return false;
        // One instant and UTC snapshot for both SSRCs. Never stamp two differently
        // aged last-packet timestamps with 'now' and pretend they are simultaneous.
        uint? video = videoEnabled ? At(_video!, now) : null;
        uint? audio = audioEnabled ? At(_audio!, now) : null;
        if (_epoch != hub.SourceEpoch) { ResetChangedEpoch(); return false; }
        report = new(video, audio, _utcOrigin.Utc + _clock.GetElapsedTime(_utcOrigin.At, now));
        return true;
    }

    public bool ReportDue => _lastReportAt == null
        || _clock.GetElapsedTime(_lastReportAt.Value).TotalSeconds >= 1;

    public void MarkReportSent() { _reports++; _lastReportAt = _clock.GetTimestamp(); }

    private static (long At, DateTimeOffset Utc) CaptureClock(TimeProvider clock)
        => (clock.GetTimestamp(), clock.GetUtcNow());

    private uint At(TrackAnchor anchor, long now) => unchecked(anchor.RtpTimestamp
        + (uint)(long)Math.Round((now - anchor.PresentationTimestamp) / _clock.TimestampFrequency * anchor.Rate));

    private bool Fresh(TrackAnchor? anchor, long now) => anchor != null
        && (now - anchor.ConfirmedAt) / (double)_clock.TimestampFrequency is >= 0 and <= 5
        && (now - anchor.PresentationTimestamp) / _clock.TimestampFrequency is >= 0 and <= 5;

    private bool ValidInstant(long epoch, double at, long frequency) => epoch == _epoch && epoch == hub.SourceEpoch && frequency > 0
        && frequency == _clock.TimestampFrequency && double.IsFinite(at)
        && (_clock.GetTimestamp() - at) / frequency is >= 0 and <= 5;

    private void ResetChangedEpoch()
    {
        if (_epoch == hub.SourceEpoch) return;
        _epoch = hub.SourceEpoch; _video = null; _rawVideoObservation = null;
        ResetRawAudio(); _lastReportAt = null;
    }

    private void ResetRawAudio()
    {
        _audio = null; _rawAudioPreviousRtp = null; _rawAudioSamples = 0;
        _rawAudioOrigin = _rawAudioPhase = 0; _rawAudioFormat = null;
    }

    private static bool SameAudio(AudioTrackInfo? expected, AudioTrackInfo? actual) => expected != null
        && actual != null && expected.IsAac == actual.IsAac && expected.SampleRate == actual.SampleRate
        && expected.Channels == actual.Channels && expected.AudioSpecificConfig != null
        && actual.AudioSpecificConfig != null && expected.AudioSpecificConfig.SequenceEqual(actual.AudioSpecificConfig);

    public object Diagnostics() => new
    {
        sourceEpoch = _epoch, videoAnchored = _video != null, audioAnchored = _audio != null,
        reportsSent = _reports, rejectedAnchors = _rejected,
        rawVideoRequiresSourceArrival = requireSourceArrival,
        timeBasis = "confirmed-source-or-scheduled-presentation", captureSynchronizationVerified = false
    };
}
