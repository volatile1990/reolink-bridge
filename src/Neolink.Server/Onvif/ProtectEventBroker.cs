// Reolink Bridge bounded camera-state/PullPoint broker; AGPL-3.0.
using Neolink.Protocol;

namespace Neolink.Onvif;

public sealed record ProtectCameraEvent(string Kind, bool Active, DateTimeOffset At, string Operation);
public sealed record ProtectPullPoint(string Id, DateTimeOffset CurrentTime, DateTimeOffset TerminationTime);
public sealed record ProtectEventBatch(DateTimeOffset CurrentTime, DateTimeOffset TerminationTime,
    IReadOnlyList<ProtectCameraEvent> Events);
public sealed record ProtectAlarmPush(long Sequence, DateTimeOffset At, bool Active,
    string[] Classes, string[] RawAiTypes, string SourceStatus, long ConnectionEpoch);
public sealed class ProtectEventFault(string code, string message) : Exception(message)
{
    public string Code { get; } = code;
}

/// <summary>One real camera, bounded queues and monotonic leases. No camera I/O or credentials are stored here.</summary>
public sealed class ProtectEventBroker
{
    public const int MaxSubscriptions = 8, MaxQueue = 256, MaxMessages = 64, MaxPullSeconds = 30;
    public const int MaxLeaseSeconds = 3600;
    public const string MotionTopic = "tns1:RuleEngine/CellMotionDetector/Motion";
    public const string AiTopicRoot = "tns1:RuleEngine/tnsre:ReolinkAI/tnsre:";
    private readonly object _gate = new();
    private readonly TimeProvider _clock;
    private readonly TimeSpan _stale;
    private readonly int _channel;
    private readonly string _motionEventPolicy;
    private readonly string _instanceId = Guid.NewGuid().ToString("N");
    private readonly Queue<ProtectAlarmPush> _replay = new();
    private readonly Dictionary<string, Subscription> _subscriptions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, State> _states = new(StringComparer.Ordinal) { ["motion"] = new() };
    private long _cameraPushes, _changes, _delivered, _queueResets, _connectionEpoch;
    private long _suppressedUnclassifiedPushes, _forwardedMotionStarts, _forwardedMotionClears;
    private DateTimeOffset? _lastCameraPush;
    private bool _stopped;

    private sealed class State
    {
        public bool Active;
        public long LastActive;
    }
    private sealed class Subscription(string owner, Func<string, bool>? filter)
    {
        public readonly string Owner = owner;
        public readonly Func<string, bool>? Filter = filter;
        public readonly Queue<ProtectCameraEvent> Queue = new();
        public TaskCompletionSource Signal = NewSignal();
        public long LeaseAt;
        public TimeSpan Lease;
        public bool Pulling;
    }

    public ProtectEventBroker(int staleSeconds = 120, TimeProvider? clock = null, int channel = 0,
        string motionEventPolicy = "all")
    {
        if (staleSeconds is < 5 or > 600) throw new ArgumentOutOfRangeException(nameof(staleSeconds));
        if (motionEventPolicy is not ("all" or "classified" or "none")) throw new ArgumentOutOfRangeException(nameof(motionEventPolicy));
        _stale = TimeSpan.FromSeconds(staleSeconds);
        _clock = clock ?? TimeProvider.System;
        _channel = channel;
        _motionEventPolicy = motionEventPolicy;
    }

    public static string Topic(string kind) => kind == "motion" ? MotionTopic : AiTopicRoot + (kind switch
    {
        "person" => "Person", "vehicle" => "Vehicle", "animal" => "Animal",
        _ => throw new ArgumentException("Unsupported event class", nameof(kind))
    });

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private void Wake(Subscription sub) { var signal = sub.Signal; sub.Signal = NewSignal(); signal.TrySetResult(); }
    private TimeSpan Remaining(Subscription sub) => sub.Lease - _clock.GetElapsedTime(sub.LeaseAt);
    private void Prune()
    {
        foreach (var id in _subscriptions.Where(s => Remaining(s.Value) <= TimeSpan.Zero).Select(s => s.Key).ToArray())
        {
            Wake(_subscriptions[id]);
            _subscriptions.Remove(id);
        }
    }
    private Subscription Find(string id, string owner)
    {
        Prune();
        if (_stopped || !_subscriptions.TryGetValue(id, out var sub) || sub.Owner != owner)
            throw new ProtectEventFault("ResourceUnknown", "The subscription is unavailable");
        return sub;
    }
    private ProtectPullPoint Reference(string id, Subscription sub)
    {
        var now = _clock.GetUtcNow();
        return new(id, now, now + Remaining(sub));
    }
    private static void ValidateLease(TimeSpan lease)
    {
        if (lease < TimeSpan.FromSeconds(5) || lease > TimeSpan.FromSeconds(MaxLeaseSeconds))
            throw new ProtectEventFault("UnacceptableTerminationTime", "Lease must be between 5 and 3600 seconds");
    }

    public ProtectPullPoint Subscribe(string owner, TimeSpan lease, Func<string, bool>? filter = null)
    {
        ValidateLease(lease);
        if (string.IsNullOrEmpty(owner)) throw new ProtectEventFault("NotAuthorized", "Authentication required");
        lock (_gate)
        {
            Prune();
            if (_stopped) throw new ProtectEventFault("ResourceUnknown", "Event service stopped");
            if (_subscriptions.Count >= MaxSubscriptions)
                throw new ProtectEventFault("MaxPullPoints", "Pull point capacity reached");
            string id = Guid.NewGuid().ToString("N");
            var sub = new Subscription(owner, filter) { Lease = lease, LeaseAt = _clock.GetTimestamp() };
            _subscriptions.Add(id, sub);
            Synchronize(sub);
            return Reference(id, sub);
        }
    }
    public ProtectPullPoint Renew(string id, string owner, TimeSpan lease)
    {
        ValidateLease(lease);
        lock (_gate)
        {
            var sub = Find(id, owner);
            sub.Lease = lease; sub.LeaseAt = _clock.GetTimestamp();
            Wake(sub); // an existing long poll must re-evaluate its lease
            return Reference(id, sub);
        }
    }
    public void Unsubscribe(string id, string owner)
    {
        lock (_gate) { var sub = Find(id, owner); _subscriptions.Remove(id); Wake(sub); }
    }
    public void Synchronize(string id, string owner)
    {
        lock (_gate) { var sub = Find(id, owner); Synchronize(sub); Wake(sub); }
    }
    private void Synchronize(Subscription sub)
    {
        sub.Queue.Clear();
        var now = _clock.GetUtcNow();
        foreach (var (kind, state) in _states)
            if (sub.Filter?.Invoke(Topic(kind)) != false)
                sub.Queue.Enqueue(new(kind, state.Active, now, "Initialized"));
    }

    public async Task<ProtectEventBatch> PullAsync(string id, string owner, TimeSpan timeout, int limit, CancellationToken ct)
    {
        if (timeout < TimeSpan.Zero || timeout > TimeSpan.FromSeconds(MaxPullSeconds))
            throw new ProtectEventFault("InvalidArgVal", "Pull timeout must be between 0 and 30 seconds");
        if (limit is < 1 or > MaxMessages)
            throw new ProtectEventFault("InvalidArgVal", "Message limit must be between 1 and 64");
        Subscription sub;
        lock (_gate)
        {
            sub = Find(id, owner);
            if (sub.Pulling) throw new ProtectEventFault("PullMessagesFault", "A pull is already in progress");
            sub.Pulling = true;
        }
        long began = _clock.GetTimestamp();
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                Task signal;
                TimeSpan wait;
                lock (_gate)
                {
                    Find(id, owner);
                    wait = timeout - _clock.GetElapsedTime(began);
                    if (sub.Queue.Count > 0 || wait <= TimeSpan.Zero)
                    {
                        var events = new List<ProtectCameraEvent>();
                        while (events.Count < limit && sub.Queue.TryDequeue(out var ev)) events.Add(ev);
                        _delivered += events.Count;
                        var reference = Reference(id, sub);
                        return new(reference.CurrentTime, reference.TerminationTime, events);
                    }
                    wait = wait < Remaining(sub) ? wait : Remaining(sub);
                    signal = sub.Signal.Task;
                }
                try { await signal.WaitAsync(wait, _clock, ct).ConfigureAwait(false); }
                catch (TimeoutException) { }
            }
        }
        finally { lock (_gate) sub.Pulling = false; }
    }

    /// <summary>Accept camera-originated state only. Ordinary motion includes a real AI-only alarm.</summary>
    public void Publish(MotionPush push)
    {
        if (push.External) return;
        var classes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in push.AiTypes)
        {
            string? kind = raw.Trim().ToLowerInvariant() switch
            {
                "people" or "person" or "human" or "pedestrian" => "person",
                "vehicle" or "car" => "vehicle",
                "dog_cat" or "dogcat" or "animal" or "pet" or "dog" or "cat" => "animal",
                _ => null
            };
            if (kind != null) classes.Add(kind);
        }
        lock (_gate)
        {
            if (_stopped) return;
            Prune();
            _cameraPushes++; _lastCameraPush = _clock.GetUtcNow();
            _replay.Enqueue(new(_cameraPushes, _lastCameraPush.Value, push.Active, classes.Order(StringComparer.Ordinal).ToArray(),
                push.AiTypes.Take(16).Select(BoundedText).ToArray(), BoundedText(push.Status), _connectionEpoch));
            while (_replay.Count > MaxQueue) _replay.Dequeue();
            foreach (string kind in classes) _states.TryAdd(kind, new State());
            bool classified = push.AiTypes.Any(IsClassification);
            bool forwardMotion = push.Active && (_motionEventPolicy == "all" || (_motionEventPolicy == "classified" && classified));
            if (_motionEventPolicy == "classified" && push.Active && !classified) _suppressedUnclassifiedPushes++;
            Set("motion", forwardMotion);
            foreach (var kind in _states.Keys.Where(k => k != "motion").ToArray()) Set(kind, classes.Contains(kind));
        }
    }
    private void Set(string kind, bool active)
    {
        var state = _states[kind];
        if (active) state.LastActive = _clock.GetTimestamp();
        if (state.Active == active) return;
        if (kind == "motion")
        {
            if (active) _forwardedMotionStarts++;
            else _forwardedMotionClears++;
        }
        state.Active = active; _changes++;
        var ev = new ProtectCameraEvent(kind, active, _clock.GetUtcNow(), "Changed");
        foreach (var sub in _subscriptions.Values)
        {
            if (sub.Filter?.Invoke(Topic(kind)) == false) continue;
            if (sub.Queue.Count >= MaxQueue) { _queueResets++; Synchronize(sub); }
            else sub.Queue.Enqueue(ev);
            Wake(sub);
        }
    }
    public void Tick()
    {
        lock (_gate)
        {
            Prune();
            foreach (var (kind, state) in _states)
                if (state.Active && _clock.GetElapsedTime(state.LastActive) >= _stale) Set(kind, false);
        }
    }
    public void ResetActive()
    {
        lock (_gate)
        {
            _connectionEpoch++;
            foreach (var kind in _states.Keys) Set(kind, false);
        }
    }
    public void Stop()
    {
        lock (_gate)
        {
            _stopped = true;
            foreach (var sub in _subscriptions.Values) Wake(sub);
            _subscriptions.Clear();
        }
    }
    public string[] ObservedClasses
    {
        get { lock (_gate) return _states.Keys.Where(k => k != "motion").Order(StringComparer.Ordinal).ToArray(); }
    }
    private static string BoundedText(string text) => new(text.Where(c => !char.IsControl(c)).Take(64).ToArray());
    private static bool IsClassification(string raw)
    {
        // A real, possibly unfamiliar AItype is sufficient evidence. Generic
        // motion and empty/boolean placeholders must not reopen the motion gate.
        string token = BoundedText(raw).Trim().ToLowerInvariant();
        return token is not ("" or "none" or "md" or "motion" or "false" or "true" or "0" or "1");
    }

    /// <summary>Replay only camera-originated pushes, never timeout/reconnect resets or outside control.</summary>
    public object Replay(long after, int limit = 128)
    {
        if (after < 0 || limit is < 1 or > MaxQueue) throw new ArgumentOutOfRangeException(nameof(after));
        lock (_gate)
        {
            long oldest = _replay.TryPeek(out var first) ? first.Sequence : _cameraPushes + 1;
            return new
            {
                instanceId = _instanceId, currentSeq = _cameraPushes, oldestSeq = oldest,
                truncated = after < oldest - 1,
                events = _replay.Where(e => e.Sequence > after).Take(limit).Select(e => new
                {
                    seq = e.Sequence, timestampUtc = e.At, motion = e.Active, isActive = e.Active,
                    classes = e.Classes.ToArray(), rawAiTypes = e.RawAiTypes.ToArray(), sourceStatus = e.SourceStatus,
                    source = new { profile = "main", channel = _channel, connectionEpoch = e.ConnectionEpoch }, zones = Array.Empty<string>()
                }).ToArray()
            };
        }
    }
    public object Diagnostics()
    {
        lock (_gate)
        {
            Prune();
            return new
            {
                cameraPushes = _cameraPushes, stateChanges = _changes, deliveredMessages = _delivered,
                queueResets = _queueResets, subscriptions = _subscriptions.Count,
                queuedMessages = _subscriptions.Values.Sum(s => s.Queue.Count),
                active = _states.Where(s => s.Value.Active).Select(s => s.Key).Order(StringComparer.Ordinal).ToArray(),
                observedClasses = ObservedClasses, lastCameraPushUtc = _lastCameraPush,
                motionEventPolicy = _motionEventPolicy, suppressedUnclassifiedPushes = _suppressedUnclassifiedPushes,
                forwardedMotionStarts = _forwardedMotionStarts, forwardedMotionClears = _forwardedMotionClears
            };
        }
    }
}
