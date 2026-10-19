// Shared observed HQ presentation delay, without a new stream or encoder. AGPL-3.0.
using Neolink.Streaming;

namespace Neolink.Rtsp;

internal readonly record struct HqPlayoutSnapshot(long SourceEpoch, long Generation, double DelaySeconds);

/// <summary>One stable HQ session owns the observed source-to-presentation mapping.
/// Other sessions can become a successor on release, but cannot move an active owner's clock.</summary>
internal sealed class HqPlayoutReference(IStreamHub source, TimeProvider? clock = null)
{
    internal const double MaximumDelaySeconds = 10;
    internal const double ReferenceTtlSeconds = 12;
    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private readonly object _gate = new();
    private readonly Dictionary<string, Observation> _observations = new(StringComparer.Ordinal);
    private string? _owner;
    private long _epoch = -1, _generation, _sequence, _accepted, _rejected;
    private sealed record Observation(long Epoch, double DelaySeconds, long At, long Order);

    // A missing owner observation is different from a changed/stale source.
    // Startup recovery must retain the actual hub epoch, even before its first observation.
    public long CurrentSourceEpoch => source.SourceEpoch;
    public bool SourceIsFresh => SourceFresh(source.SourceEpoch);

    public bool Observe(string owner, CompletedVideoGop gop, long presentationTimestamp)
    {
        if (string.IsNullOrWhiteSpace(owner) || owner.Length > 128 || !SourceFresh(gop.SourceEpoch)
            || gop.SourceArrivalFrequency != _clock.TimestampFrequency || gop.SourceArrivalFrequency <= 0)
        { lock (_gate) _rejected++; return false; }
        double delay = (presentationTimestamp - (double)gop.SourceArrivalTimestamp) / gop.SourceArrivalFrequency;
        if (!double.IsFinite(delay) || delay is < 0 or > MaximumDelaySeconds)
        { lock (_gate) _rejected++; return false; }
        lock (_gate)
        {
            if (_epoch != gop.SourceEpoch)
            {
                _observations.Clear(); _owner = null; _epoch = gop.SourceEpoch; _generation++;
            }
            long order = _observations.TryGetValue(owner, out var existing) ? existing.Order : ++_sequence;
            _observations[owner] = new(_epoch, delay, _clock.GetTimestamp(), order);
            if (_owner == null) { _owner = owner; _generation++; }
            _accepted++;
            return true;
        }
    }

    public void Release(string owner)
    {
        lock (_gate)
        {
            _observations.Remove(owner);
            if (_owner != owner) return;
            _owner = _observations.Where(pair => Fresh(pair.Value)).OrderBy(pair => pair.Value.Order)
                .Select(pair => pair.Key).FirstOrDefault();
            _generation++;
        }
    }

    public bool TryGetSnapshot(out HqPlayoutSnapshot snapshot)
    {
        snapshot = default;
        if (!SourceFresh(source.SourceEpoch)) return false;
        lock (_gate)
        {
            if (_epoch != source.SourceEpoch || _owner == null
                || !_observations.TryGetValue(_owner, out var observation) || !Fresh(observation)) return false;
            snapshot = new(_epoch, _generation, observation.DelaySeconds);
            return true;
        }
    }

    private bool SourceFresh(long epoch) => source.SourceEpoch == epoch && source.LiveVideo
        && source.GetVideoDiagnostics().LastVideoAgeMs is >= 0 and <= 5000;

    private bool Fresh(Observation observation) => observation.Epoch == _epoch
        && _clock.GetElapsedTime(observation.At).TotalSeconds is >= 0 and <= ReferenceTtlSeconds;

    public object Diagnostics()
    {
        bool ready = TryGetSnapshot(out var snapshot);
        lock (_gate)
        {
            var observation = _owner != null && _observations.TryGetValue(_owner, out var value) ? value : null;
            return new
            {
                ready, sourceEpoch = _epoch, generation = _generation, activeOwners = _observations.Count,
                delayMs = ready ? (double?)(snapshot.DelaySeconds * 1000) : null,
                referenceAgeMs = observation == null ? (double?)null : _clock.GetElapsedTime(observation.At).TotalMilliseconds,
                acceptedObservations = _accepted, rejectedObservations = _rejected,
                maximumDelayMs = MaximumDelaySeconds * 1000, referenceTtlMs = ReferenceTtlSeconds * 1000,
                captureSynchronizationVerified = false
            };
        }
    }
}
