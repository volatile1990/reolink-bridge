// Original AAC on the existing GOP presentation clock. No capture PTS is invented. AGPL-3.0.
using Neolink.Streaming;

namespace Neolink.Rtsp;

internal sealed record GopAacPlan(HubAudioAac Packet, TimeSpan Offset);
internal sealed record GopAacDiagnostics(int BufferedPackets, int BufferedBytes, long PlannedPackets,
    long SentPackets, long DroppedPackets, long Resets, double? InitialAudioOffsetMs,
    double MaxArrivalEstimateDeviationMs, string TimeBasis = "arrival-estimate-and-aac-samples",
    bool CaptureSynchronizationVerified = false, long LateSamplePackets = 0,
    long FreshArrivalLateSamplePackets = 0, long InvalidPackets = 0,
    double MinSignedArrivalEstimateDeviationMs = 0, double MaxSignedArrivalEstimateDeviationMs = 0,
    double PhaseCorrectionMs = 0, double MaxPhaseCorrectionPerGopMs = 0, long ReconciledFreshPackets = 0,
    long PhaseRecoveries = 0, long RecoveryRtpOffsetSamples = 0);

/// <summary>A single media pump owns this bounded queue. AAC is paced by its real
/// 1024-sample clock and positioned relative to the unchanged video GOP's source-arrival window.
/// Arrival estimates cannot recover a camera capture timestamp; that limit is explicit.</summary>
internal sealed class GopAacTimeline(int sampleRate, long sourceEpoch)
{
    internal const int MaxPackets = 512, MaxBytes = 512 * 1024;
    // An independently sampled camera AAC clock can drift against video arrival.
    // Freeze one affine mapping per GOP, with <=1% phase slew; never copy a TCP
    // burst's individual arrival gaps into AAC sample spacing or sender reports.
    internal const double MaxPhaseSlewPerSecond = 0.01;
    internal const double ForwardPhaseRecoveryThresholdSeconds = 0.25;
    private sealed record Pending(HubAudioAac Packet, double SourceSeconds, double ObservedArrivalSeconds);
    private readonly List<Pending> _pending = [];
    private long _epoch = sourceEpoch, _samples, _planned, _sent, _drops, _resets;
    private int _bytes;
    private uint? _firstRtp, _lastRtp;
    private double _firstSourceSeconds, _maxDeviation;
    private long _lateSamples, _freshLateSamples = 0, _invalid;
    private double _minSignedDeviation, _maxSignedDeviation;
    private double _phaseCorrection, _maxPhaseStep;
    private long _reconciledFresh;
    private long _phaseRecoveries, _recoverySamples;
    private uint _outputRtpOffset;
    private double? _initialOffset;
    public bool HaveAnchor => _firstRtp != null;
    public GopAacDiagnostics Diagnostics => new(_pending.Count, _bytes, _planned, _sent, _drops, _resets, _initialOffset, _maxDeviation,
        LateSamplePackets: _lateSamples, FreshArrivalLateSamplePackets: _freshLateSamples, InvalidPackets: _invalid,
        MinSignedArrivalEstimateDeviationMs: _minSignedDeviation, MaxSignedArrivalEstimateDeviationMs: _maxSignedDeviation,
        PhaseCorrectionMs: _phaseCorrection * 1000, MaxPhaseCorrectionPerGopMs: _maxPhaseStep * 1000,
        ReconciledFreshPackets: _reconciledFresh, PhaseRecoveries: _phaseRecoveries,
        RecoveryRtpOffsetSamples: _recoverySamples);

    public void Reset(long epoch)
    {
        _drops += _pending.Count; _pending.Clear(); _bytes = 0;
        _firstRtp = _lastRtp = null; _samples = 0; _epoch = epoch; _resets++;
        _phaseCorrection = 0;
    }
    public void Push(HubAudioAac packet)
    {
        if (packet.SourceEpoch != _epoch || packet.ArrivalTimestamp == null || packet.ArrivalTimestampFrequency <= 0
            || packet.Au.Length is < 1 or > 8184 || packet.BlockUnits is < 1 or > MaxPackets
            || packet.BlockIndex < 0 || packet.BlockIndex >= packet.BlockUnits)
        { _drops++; _invalid++; return; }
        double actual = packet.ArrivalTimestamp.Value / (double)packet.ArrivalTimestampFrequency
            - (packet.BlockUnits - 1 - packet.BlockIndex) * 1024d / sampleRate;
        if (_firstRtp != null && (_pending.Count >= MaxPackets || _bytes + packet.Au.Length > MaxBytes
            || unchecked(packet.RtpTs - _lastRtp!.Value) != 1024)) Reset(_epoch);
        if (_firstRtp == null)
        { _firstRtp = packet.RtpTs; _firstSourceSeconds = actual; _samples = 0; }
        double predicted = _firstSourceSeconds + _samples / (double)sampleRate;
        double signedDeviation = (actual - predicted) * 1000;
        _minSignedDeviation = Math.Min(_minSignedDeviation, signedDeviation);
        _maxSignedDeviation = Math.Max(_maxSignedDeviation, signedDeviation);
        double deviation = Math.Abs(actual - predicted); _maxDeviation = Math.Max(_maxDeviation, deviation * 1000);
        // A real long discontinuity in audio must not replay packets on an obsolete sample anchor.
        // Ordinary burst jitter is observed, not mistaken for a camera capture timestamp.
        if (Math.Abs(actual - predicted - _phaseCorrection) > 5)
        { Reset(_epoch); _firstRtp = packet.RtpTs; _firstSourceSeconds = predicted = actual; }
        _pending.Add(new(packet, predicted, actual)); _bytes += packet.Au.Length;
        _lastRtp = packet.RtpTs; _samples += 1024;
    }

    public IReadOnlyList<GopAacPlan> Plan(CompletedVideoGop gop)
    {
        if (gop.SourceEpoch != _epoch || gop.SourceArrivalFrequency <= 0) { Reset(gop.SourceEpoch); return []; }
        double start = gop.SourceArrivalTimestamp / (double)gop.SourceArrivalFrequency, end = start + gop.Duration.TotalSeconds;
        // Use de-bursted arrivals from this actual source window. A median
        // resists a few delayed socket reads; gradual ADC/video clock drift is
        // reconciled well before the previous250-ms late-sample drop boundary.
        var phases = _pending.Where(p => p.ObservedArrivalSeconds >= start - 0.25
                && p.ObservedArrivalSeconds < end)
            .Select(p => p.ObservedArrivalSeconds - p.SourceSeconds).Order().ToArray();
        if (phases.Length > 0)
        {
            int middle = phases.Length / 2;
            double target = phases.Length % 2 == 0 ? (phases[middle - 1] + phases[middle]) / 2 : phases[middle];
            double step;
            if (target - _phaseCorrection > ForwardPhaseRecoveryThresholdSeconds)
            {
                // A sustained forward arrival pause must not leave fresh AAC
                // permanently behind the old estimate. Represent that pause as
                // one forward presentation-RTP gap. Matching sample-quantized
                // phase and RTP offsets cancel exactly in the inverse SR map;
                // no backward report, payload edit or video reserve reset.
                long samples = (long)Math.Round((target - _phaseCorrection) * sampleRate);
                _outputRtpOffset = unchecked(_outputRtpOffset + (uint)samples);
                _recoverySamples += samples; _phaseRecoveries++;
                step = samples / (double)sampleRate;
            }
            else step = Math.Clamp(target - _phaseCorrection,
                -gop.Duration.TotalSeconds * MaxPhaseSlewPerSecond, gop.Duration.TotalSeconds * MaxPhaseSlewPerSecond);
            _phaseCorrection += step; _maxPhaseStep = Math.Max(_maxPhaseStep, Math.Abs(step));
        }
        var result = new List<GopAacPlan>();
        for (int i = 0; i < _pending.Count;)
        {
            var pending = _pending[i];
            double sourceSeconds = pending.SourceSeconds + _phaseCorrection;
            // Observed-old audio remains stale even if a positive phase shift
            // would move its sample estimate into this GOP. Never replay it.
            if (pending.ObservedArrivalSeconds < start - 0.25)
            {
                _pending.RemoveAt(i); _bytes -= pending.Packet.Au.Length;
                _drops++; _lateSamples++; continue;
            }
            if (sourceSeconds >= end) { i++; continue; }
            _pending.RemoveAt(i); _bytes -= pending.Packet.Au.Length;
            // A sudden phase step can need several bounded corrections. Fresh
            // received AAC is retained at its real nominal negative offset,
            // never clamped to a common due time or discarded by the old anchor.
            // Existing WaitAsync safely sends overdue offsets; transient catchup
            // may burst, but the packet RTP and inverse SR mapping stay exact.
            if (sourceSeconds < start - 0.25) _reconciledFresh++;
            double offset = sourceSeconds - start;
            _initialOffset ??= offset * 1000;
            var packet = _outputRtpOffset == 0 ? pending.Packet
                : pending.Packet with { RtpTs = unchecked(pending.Packet.RtpTs + _outputRtpOffset) };
            result.Add(new(packet, TimeSpan.FromSeconds(offset))); _planned++;
        }
        return result;
    }
    public uint RtpAtSourceTime(double sourceSeconds)
    {
        if (_firstRtp == null) throw new InvalidOperationException("AAC needs an observed sample anchor");
        return unchecked(_firstRtp.Value + _outputRtpOffset
            + (uint)(long)Math.Round((sourceSeconds - _firstSourceSeconds - _phaseCorrection) * sampleRate));
    }
    public void MarkSent() => _sent++;
}
