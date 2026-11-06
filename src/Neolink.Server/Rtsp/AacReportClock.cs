// Phase-coherent AAC sender-report estimate; original payload and pacing stay separate. AGPL-3.0.
namespace Neolink.Rtsp;

/// <summary>Owned by one media pump. Observation times are arrival/presentation
/// estimates, never recovered camera capture times. Only a successfully emitted
/// report commits the continuous double-precision phase and its next frequency.</summary>
internal sealed class AacReportClock(int sampleRate, TimeProvider clock)
{
    internal const int MaximumBuckets = 60;
    internal const double MinimumFitSpanSeconds = 8, PhaseCorrectionSeconds = 60;
    internal const double MaximumFrequencyDeviation = .01, MaximumRateSlewPerSecond = .0001;
    internal const double MaximumPhaseRateCorrection = .0001;
    internal const double MaximumPhaseErrorSeconds = .250;
    private readonly record struct Point(double Seconds, double Samples);
    private readonly record struct Estimate(double Rate, double Intercept, double Span,
        double Residual, double RateUncertainty, double PhaseUncertainty)
    {
        public double Target(double now) => Intercept + Rate * now;
    }
    private readonly List<Point> _bucket = [];
    private readonly Queue<Point> _buckets = new();
    private readonly Queue<Point> _slowBuckets = new();
    private uint? _firstRtp, _lastRtp;
    private double _origin, _samples, _lastPresentation, _phase, _rate = sampleRate;
    private double? _reportAt;
    private long? _bucketNumber;
    private long _recoveryOffset, _observations, _resets;
    private long? _lastConfirmedAt;
    private long _outOfOrderPresentationEstimates;
    private double? _fitRate, _fitTarget, _fitSpan, _fitResidual;
    private double? _rateUncertainty, _phaseUncertainty, _phaseError;
    private bool _nonNominalRateKnown;
    private double _trustedRate = sampleRate, _slowSpacing = 60, _nextSlowAt;
    private long _observationRevision, _slowRevision, _fastCachedRevision = -1, _slowCachedRevision = -1;
    private Estimate? _fastCached, _slowCached;
    private long _fastFitEvaluations, _slowFitEvaluations;
    private string _rateEvidenceScale = "acquiring-prior";

    public bool HaveAnchor => _firstRtp.HasValue;
    public bool Published => _reportAt.HasValue;

    public void Reset()
    {
        _firstRtp = _lastRtp = null; _origin = _samples = _lastPresentation = _phase = 0;
        _rate = sampleRate; _reportAt = null; _recoveryOffset = 0; ClearFit(); _resets++;
        _nonNominalRateKnown = false;
        _trustedRate = sampleRate;
        _lastConfirmedAt = null;
    }

    private void ClearFit()
    {
        _bucket.Clear(); _buckets.Clear(); _bucketNumber = null;
        _slowBuckets.Clear(); _slowSpacing = 60; _nextSlowAt = 0;
        _observationRevision++; _slowRevision++; _fastCachedRevision = _slowCachedRevision = -1;
        _fastCached = _slowCached = null; _rateEvidenceScale = "acquiring-prior";
        _fitRate = _fitTarget = _fitSpan = _fitResidual = null;
        _rateUncertainty = _phaseUncertainty = _phaseError = null;
    }

    public void ObserveSent(uint rtp, double presentationTimestamp, bool independentObservation,
        long recoveryOffsetSamples = 0, double? observationTimestamp = null)
    {
        double seconds = presentationTimestamp / clock.TimestampFrequency;
        long confirmedAt = clock.GetTimestamp();
        if (!double.IsFinite(seconds)) throw new ArgumentOutOfRangeException(nameof(presentationTimestamp));
        if (_lastRtp is { } previous)
        {
            uint delta = unchecked(rtp - previous);
            bool recovery = recoveryOffsetSamples != _recoveryOffset;
            bool validRecovery = recovery && recoveryOffsetSamples >= _recoveryOffset
                && delta == unchecked(1024u + (uint)(recoveryOffsetSamples - _recoveryOffset));
            if (_lastConfirmedAt is { } lastSent && clock.GetElapsedTime(lastSent, confirmedAt).TotalSeconds > 5
                || delta != 1024 && !validRecovery || recovery && !validRecovery)
                Reset();
            else
            {
                _samples += delta;
                // An explicit existing forward-recovery offset is not evidence
                // of a changed oscillator rate. Keep published phase, clear fit.
                if (recovery) ClearFit();
            }
        }
        if (_firstRtp == null) { _firstRtp = rtp; _origin = seconds; _samples = 0; }
        if (_lastRtp != null && seconds < _lastPresentation) _outOfOrderPresentationEstimates++;
        _lastRtp = rtp; _lastPresentation = seconds; _recoveryOffset = recoveryOffsetSamples;
        _lastConfirmedAt = confirmedAt;
        if (!independentObservation) return;
        var point = new Point((observationTimestamp ?? presentationTimestamp) / clock.TimestampFrequency - _origin, _samples);
        if (!double.IsFinite(point.Seconds)) throw new ArgumentOutOfRangeException(nameof(observationTimestamp));
        _observations++;
        _observationRevision++;
        long number = (long)Math.Floor(point.Seconds);
        if (_bucketNumber != number)
        {
            if (_bucket.Count > 0)
            {
                var completed = BucketMedian(); _buckets.Enqueue(completed);
                ObserveSlow(completed);
            }
            while (_buckets.Count >= MaximumBuckets) _buckets.Dequeue();
            _bucket.Clear(); _bucketNumber = number;
        }
        if (_bucket.Count < 128) _bucket.Add(point);
    }

    // One REAL paired observation: independent coordinate medians would invent
    // a source instant, and all 16 AUs of a TCP bundle are not 16 clock samples.
    private Point BucketMedian() => _bucket.OrderBy(p => p.Seconds - p.Samples / sampleRate)
        .ElementAt(_bucket.Count / 2);

    private void ObserveSlow(Point point)
    {
        if (_slowBuckets.Count > 0 && point.Seconds < _nextSlowAt) return;
        if (_slowBuckets.Count == MaximumBuckets)
        {
            // Keep the earlier REAL member of each chronological pair. No
            // averaged sample/time point is invented, and the oldest source
            // baseline survives. Future spacing doubles with the age horizon.
            var retained = _slowBuckets.Where((_, index) => index % 2 == 0).ToArray();
            _slowBuckets.Clear(); foreach (var retainedPoint in retained) _slowBuckets.Enqueue(retainedPoint);
            _slowSpacing *= 2;
        }
        _slowBuckets.Enqueue(point); _nextSlowAt = point.Seconds + _slowSpacing; _slowRevision++;
    }

    private static double Median(IEnumerable<double> values)
    {
        var sorted = values.Order().ToArray(); int middle = sorted.Length / 2;
        return sorted.Length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
    }

    private Estimate? Calculate(Point[] points)
    {
        if (points.Length < 9) return null;
        var slopes = new List<double>();
        for (int i = 0; i < points.Length; i++)
            for (int j = i + 1; j < points.Length; j++)
            {
                double dt = points[j].Seconds - points[i].Seconds;
                if (dt >= MinimumFitSpanSeconds) slopes.Add((points[j].Samples - points[i].Samples) / dt);
            }
        if (slopes.Count == 0) return null;
        double rate = Median(slopes);
        if (!double.IsFinite(rate) || rate <= 0) return null;
        double intercept = Median(points.Select(p => p.Samples - rate * p.Seconds));
        double span = points[^1].Seconds - points[0].Seconds;
        double residual = points.Max(p => Math.Abs(p.Samples - (intercept + rate * p.Seconds)));
        // A bounded arrival phase can imitate a rate change by twice its span
        // over the fit baseline. Keep the nominal prior inside that uncertainty.
        // A fit through a delivery-phase step can put its endpoints on opposite
        // sides of the residual envelope. Use the full four-sided span; this is
        // a conservative observed ambiguity bound, not capture-clock proof.
        double uncertainty = Math.Max(1, 4 * residual) / span;
        if (Math.Abs(rate - sampleRate) > sampleRate * MaximumFrequencyDeviation + uncertainty)
            throw new IOException("AAC sender report clock drift exceeds supported uncertainty bound");
        return new(rate, intercept, span, residual, uncertainty, Math.Max(.5, residual));
    }

    private Estimate? FastFit()
    {
        if (_fastCachedRevision == _observationRevision) return _fastCached;
        _fastCached = Calculate(_buckets.Concat(_bucket.Count == 0 ? [] : new[] { BucketMedian() }).ToArray());
        _fastCachedRevision = _observationRevision; _fastFitEvaluations++;
        return _fastCached;
    }

    private Estimate? SlowFit()
    {
        if (_slowCachedRevision == _slowRevision) return _slowCached;
        _slowCached = Calculate(_slowBuckets.ToArray()); _slowCachedRevision = _slowRevision; _slowFitEvaluations++;
        return _slowCached;
    }

    private (Estimate Value, bool RateEvidence)? Fit(double now, double phase)
    {
        var fast = FastFit(); var slow = SlowFit();
        if (fast == null && slow == null) return null;
        // A clean short latency ramp can exclude nominal in the short fit.
        // It still cannot establish an oscillator change. Require a full AAC
        // access unit of accumulated displacement beyond observed uncertainty
        // before short evidence may replace an established long rate prior.
        // Initial acquisition retains the existing9-bucket evidence contract;
        // postponing genuine4000ppm acquisition would spend its250ms budget.
        bool fastEvidence = fast is { } f && Math.Abs(f.Rate - _trustedRate) > f.RateUncertainty
            && (slow == null || Math.Abs(f.Target(now) - phase) > 1024 + f.PhaseUncertainty);
        bool compatible = fast is not { } fastValue || slow is not { } slowValue
            || Math.Abs(fastValue.Rate - slowValue.Rate) <= fastValue.RateUncertainty + slowValue.RateUncertainty;
        Estimate selected;
        bool rateEvidence;
        if (fastEvidence && (slow == null || !compatible || fast!.Value.RateUncertainty < slow.Value.RateUncertainty))
        { selected = fast!.Value; rateEvidence = true; _rateEvidenceScale = "fast-accumulated-displacement"; }
        else if (slow is { } longValue)
        {
            selected = longValue;
            rateEvidence = _nonNominalRateKnown || Math.Abs(longValue.Rate - sampleRate) > longValue.RateUncertainty;
            _rateEvidenceScale = rateEvidence ? "adaptive-long-baseline" : "long-baseline-prior";
        }
        else { selected = fast!.Value; rateEvidence = false; _rateEvidenceScale = "acquiring-prior"; }
        _fitRate = selected.Rate; _fitSpan = selected.Span; _fitTarget = selected.Target(now);
        _fitResidual = selected.Residual / selected.Rate;
        _rateUncertainty = selected.RateUncertainty; _phaseUncertainty = selected.PhaseUncertainty;
        return (selected, rateEvidence);
    }

    private (double Phase, double Rate) Peek(double now)
    {
        if (!HaveAnchor) throw new InvalidOperationException("AAC report needs a successfully sent AU");
        if (_reportAt is { } at) return (_phase + _rate * (now - at), _rate);
        // A short clean TCP ramp is indistinguishable from real sample drift.
        // Start nominal and disclose acquisition; never promote a short fit.
        return (sampleRate * now, sampleRate);
    }

    public uint RtpAt(long reportTimestamp)
    {
        double now = reportTimestamp / (double)clock.TimestampFrequency - _origin;
        double phase = Peek(now).Phase;
        if (Published)
        {
            var fit = Fit(now, phase);
            // Retain the original short presentation bound as well as the
            // selected long estimate; old history cannot hide a current jump.
            if (FastFit() is { } fast && Math.Abs(fast.Target(now) - phase) / sampleRate > MaximumPhaseErrorSeconds
                || fit is { } selected && Math.Abs(selected.Value.Target(now) - phase) / sampleRate > MaximumPhaseErrorSeconds)
                throw new IOException("AAC report clock acquisition exceeded250ms presentation bound");
        }
        return unchecked(_firstRtp!.Value + (uint)(long)Math.Round(phase));
    }

    public void MarkReportSent(long reportTimestamp)
    {
        double now = reportTimestamp / (double)clock.TimestampFrequency - _origin;
        var current = Peek(now);
        double nextRate = current.Rate;
        if (_reportAt is { } previous && Fit(now, current.Phase) is { } fit)
        {
            double error = fit.Value.Target(now) - current.Phase; _phaseError = error;
            if (fit.RateEvidence)
            {
                _trustedRate = fit.Value.Rate;
                if (Math.Abs(_trustedRate - sampleRate) > _rateUncertainty!.Value) _nonNominalRateKnown = true;
            }
            double outside = Math.CopySign(Math.Max(0, Math.Abs(error) - _phaseUncertainty!.Value), error);
            double phaseRate = _nonNominalRateKnown ? Math.Clamp(outside / PhaseCorrectionSeconds,
                -sampleRate * MaximumPhaseRateCorrection, sampleRate * MaximumPhaseRateCorrection)
                : 0;
            // A delivery-offset step is not clock-rate evidence. Without an
            // independently resolved nonnominal rate, retain the original line.
            double desired = Math.Clamp(_trustedRate + phaseRate,
                sampleRate * (1 - MaximumFrequencyDeviation), sampleRate * (1 + MaximumFrequencyDeviation));
            double change = sampleRate * MaximumRateSlewPerSecond * Math.Max(0, now - previous);
            nextRate += Math.Clamp(desired - nextRate, -change, change);
        }
        // Do not use the rounded wire counter as the next phase: its fractional
        // sample residue would otherwise accumulate at every sender report.
        _phase = current.Phase; _rate = nextRate; _reportAt = now;
    }

    public object Diagnostics() => new
    {
        published = Published, independentObservations = _observations, resets = _resets,
        outOfOrderPresentationEstimates = _outOfOrderPresentationEstimates,
        fitRepresentativeCount = _buckets.Count + (_bucket.Count > 0 ? 1 : 0) + _slowBuckets.Count,
        slowSampleSpacingSeconds = _slowSpacing, fastFitEvaluations = _fastFitEvaluations,
        slowFitEvaluations = _slowFitEvaluations, rateEvidenceScale = _rateEvidenceScale, trustedRateHz = _trustedRate,
        rateHz = _rate, fittedRateHz = _fitRate, fitSpanSeconds = _fitSpan,
        fitResidualMs = _fitResidual * 1000,
        rateUncertaintyHz = _rateUncertainty, phaseUncertaintyMs = _phaseUncertainty / sampleRate * 1000,
        nonNominalRateKnown = _nonNominalRateKnown,
        phaseErrorMs = _phaseError / sampleRate * 1000,
        presentationOffsetUncertain = !_nonNominalRateKnown && _phaseError != null
            && Math.Abs(_phaseError.Value) > _phaseUncertainty + 1,
        acquiring = _fitSpan == null || _fitSpan < MinimumFitSpanSeconds
            || _nonNominalRateKnown && _phaseError != null && Math.Abs(_phaseError.Value) > _phaseUncertainty + 1,
        captureSynchronizationVerified = false, timeBasis = "arrival-or-planned-presentation-estimate"
    };
}
