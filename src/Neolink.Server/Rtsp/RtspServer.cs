// Copyright (c) 2026 Oluwabori Olaleye
// Licensed under the GNU Affero General Public License v3.0; see the LICENSE file
// in the repository root.
using System.Net;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using Neolink.Streaming;

namespace Neolink.Rtsp;

/// <summary>Local resource budgets. Media activity keeps a playing connection alive.</summary>
public sealed record RtspResourceLimits
{
    public int MaxConnections { get; init; } = 128;
    public int MaxConnectionsPerPeer { get; init; } = 32;
    public int MaxSessionsPerConnection { get; init; } = 8;
    public int MaxRequestBodyBytes { get; init; } = 64 * 1024;
    public int MaxHeaderBytes { get; init; } = 64 * 1024;
    public int MaxHeaderCount { get; init; } = 64;
    public TimeSpan RequestDeadline { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan ConnectionIdleTimeout { get; init; } = TimeSpan.FromSeconds(120);
    public TimeSpan SessionIdleTimeout { get; init; } = TimeSpan.FromSeconds(60);
    internal TimeSpan WatchInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    internal void Validate()
    {
        if (MaxConnections < 1 || MaxConnectionsPerPeer < 1 || MaxSessionsPerConnection < 1
            || MaxRequestBodyBytes < 0 || MaxHeaderBytes < 128 || MaxHeaderCount < 1
            || RequestDeadline <= TimeSpan.Zero || ConnectionIdleTimeout <= TimeSpan.Zero
            || SessionIdleTimeout <= TimeSpan.Zero || WatchInterval <= TimeSpan.Zero)
            throw new ArgumentException("Invalid RTSP resource limits");
    }
}

public sealed class RtspMount
{
    public required string Path { get; init; }
    public required IStreamHub Hub { get; init; }
    /// <summary>Users allowed to access this mount; null means no authentication required.</summary>
    public HashSet<string>? PermittedUsers { get; init; }

    /// <summary>Camera control that backs the RTSP audio backchannel (two-way talk)
    /// for this mount, or null when talk is disabled or the source has no speaker.</summary>
    public ICameraControl? Talk { get; set; }
    /// <summary>Optional speaker-operation subset of already authenticated mount users.</summary>
    public HashSet<string>? TalkPermittedUsers { get; init; }
    public bool TalkAllowedFor(string? user) => TalkPermittedUsers == null
        || user != null && TalkPermittedUsers.Contains(user);

    /// <summary>The DEFAULT audio for sessions on this mount: true serves
    /// transcoded Opus, false the camera's original track. Seeded from the
    /// camera's audio_transcode config; each client overrides it per URL with
    /// ?audio=opus / ?audio=original.</summary>
    public bool Opus { get; init; }
    /// <summary>Complete-GOP video-only output normalization; disabled for existing mounts.</summary>
    public bool GopPlayout { get; init; }
    public bool GopAudio { get; init; }
    /// <summary>Source/presentation sender reports for explicit LQ original-AAC mounts only.</summary>
    public bool LowQualitySenderReports { get; init; }
    /// <summary>Encoded LQ cannot substitute output publication time for missing input provenance.</summary>
    internal bool LowQualityRequiresSourceArrival { get; init; }
    internal bool GopAacAvailable => GopAudio && Hub is StreamHub actual && Hub.LiveVideo
        && Hub.GetVideoDiagnostics().LastVideoAgeMs is >= 0 and <= 5000
        && actual.AudioMetadataSourceEpoch == Hub.SourceEpoch && actual.LastAudioAgeMs is >= 0 and <= 5000
        && actual.Audio is { IsAac: true, SampleRate: >= 8000 and <= 96000, Channels: >= 1 and <= 2, AudioSpecificConfig.Length: >= 2 };
    /// <summary>Additional startup reserve after the first complete GOP has arrived.</summary>
    public int PlayoutDelayMs { get; init; }
    /// <summary>Shared per-camera reference published by an existing HQ GOP consumer.</summary>
    internal HqPlayoutReference? PublishHqTiming { get; init; }
    /// <summary>Opt-in delayed Protect LQ; the direct LQ alias never uses this reference.</summary>
    internal HqPlayoutReference? MatchHqTiming { get; init; }
    internal bool SameMediaPolicy(RtspMount other) => ReferenceEquals(Hub, other.Hub)
        && GopPlayout == other.GopPlayout && GopAudio == other.GopAudio
        && LowQualitySenderReports == other.LowQualitySenderReports
        && LowQualityRequiresSourceArrival == other.LowQualityRequiresSourceArrival
        && PlayoutDelayMs == other.PlayoutDelayMs && Opus == other.Opus
        && ReferenceEquals(PublishHqTiming, other.PublishHqTiming)
        && ReferenceEquals(MatchHqTiming, other.MatchHqTiming);
}

/// <summary>Pure .NET RTSP server (RFC 2326 subset: OPTIONS/DESCRIBE/SETUP/PLAY/PAUSE/GET_PARAMETER/TEARDOWN).</summary>
public sealed class RtspServer
{
    /// <summary>Bridge mode accepts interleaved RTP over TCP only; never opens dynamic UDP media ports.</summary>
    public bool TcpOnly { get; init; }
    public RtspResourceLimits ResourceLimits { get; init; } = new();
    internal TimeProvider ResourceTimeProvider { get; init; } = TimeProvider.System;
    internal int ListeningPort { get; private set; }
    private readonly object _connectionGate = new();
    private readonly Dictionary<long, (TcpClient Client, string Peer, Task Completion)> _connections = new();
    private long _nextConnection;
    internal int ActiveConnectionCount { get { lock (_connectionGate) return _connections.Count; } }
    // Injected only by deterministic local contract tests; each pump owns its own timeline.
    internal TimeProvider PlayoutTimeProvider { get; init; } = TimeProvider.System;
    internal Func<TimeSpan, CancellationToken, Task>? PlayoutWaiter { get; init; }
    private readonly Dictionary<string, RtspMount> _mounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly IReadOnlyDictionary<string, string> _users;
    private readonly ConcurrentDictionary<string, GopAacDiagnostics> _gopAacSessions = new();
    private GopAacDiagnostics? _lastGopAac;
    internal void ObserveGopAac(string session, GopAacDiagnostics value, bool completed = false)
    {
        if (completed) { _gopAacSessions.TryRemove(session, out _); _lastGopAac = value; }
        else _gopAacSessions[session] = value;
    }
    public object GopAudioDiagnostics() => new
    {
        activeSessions = _gopAacSessions.Count,
        timeBasis = "arrival-estimate-and-aac-samples", captureSynchronizationVerified = false,
        sessions = _gopAacSessions.Values.ToArray(), lastCompleted = _lastGopAac
    };
    private readonly ConcurrentDictionary<string, object> _matchedLowSessions = new();
    private object? _lastMatchedLowSession;
    internal void ObserveMatchedLow(string session, object value, bool completed = false)
    {
        if (completed) { _matchedLowSessions.TryRemove(session, out _); _lastMatchedLowSession = value; }
        else _matchedLowSessions[session] = value;
    }
    private readonly ConcurrentDictionary<string, object> _lowSenderReports = new();
    private object? _lastLowSenderReport;
    internal void ObserveLowSenderReports(string session, object value, bool completed = false)
    {
        if (completed) { _lowSenderReports.TryRemove(session, out _); _lastLowSenderReport = value; }
        else _lowSenderReports[session] = value;
    }
    public object LowQualityTimingDiagnostics() => new
    {
        timeBasis = "source-arrival-estimate-and-observed-hq-presentation", captureSynchronizationVerified = false,
        references = _mounts.Values.Select(m => m.PublishHqTiming).OfType<HqPlayoutReference>()
            .Distinct().Select(reference => reference.Diagnostics()).ToArray(),
        activeSessions = _matchedLowSessions.Count, sessions = _matchedLowSessions.Values.ToArray(),
        lastCompleted = _lastMatchedLowSession,
        senderReports = new { activeSessions = _lowSenderReports.Count,
            sessions = _lowSenderReports.Values.ToArray(), lastCompleted = _lastLowSenderReport }
    };

    public RtspServer(IReadOnlyDictionary<string, string> users)
    {
        _users = users;
    }

    /// <summary>Where playing sessions subscribe and register for the Monitor page's viewer list.</summary>
    public ViewerRegistry Viewers { get; init; } = new();

    public void AddMount(RtspMount mount)
    {
        _mounts[Normalize(mount.Path)] = mount;
        Log.Debug($"RTSP mount ready: {mount.Path}");
    }

    public RtspMount? FindMount(string path) =>
        _mounts.TryGetValue(Normalize(path), out var m) ? m : null;

    private static string Normalize(string path)
    {
        path = Uri.UnescapeDataString(path);
        path = path.TrimEnd('/');
        if (!path.StartsWith('/')) path = "/" + path;
        return path;
    }

    /// <summary>Checks Basic authorization for a mount. Returns true if access is allowed.</summary>
    public bool Authorize(RtspMount mount, string? authorizationHeader) =>
        NetUtil.Allows(_users, mount.PermittedUsers, VerifiedUser(authorizationHeader));

    /// <summary>The configured user a Basic Authorization header proves, or null.</summary>
    public string? VerifiedUser(string? authorizationHeader)
    {
        var creds = NetUtil.DecodeBasicAuth(authorizationHeader);
        return NetUtil.Verified(_users, creds?.User, creds?.Pass);
    }

    public async Task RunAsync(string bindAddr, int port, CancellationToken ct)
    {
        ResourceLimits.Validate();
        var ip = bindAddr == "0.0.0.0" ? IPAddress.Any : IPAddress.Parse(bindAddr);
        var listener = new TcpListener(ip, port);
        listener.Start();
        ListeningPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        Log.Info($"RTSP server listening on rtsp://{bindAddr}:{port}/");

        var host = NetUtil.DisplayHost(bindAddr);
        var credentials = _users.Count > 0 ? "<user>:<pass>@" : "";
        foreach (var mount in _mounts.Values.OrderBy(m => m.Path, StringComparer.OrdinalIgnoreCase))
            Log.Info($"  Stream: rtsp://{credentials}{host}:{port}{mount.Path}");

        await using var reg = ct.Register(() => listener.Stop());
        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                }
                catch (Exception) when (ct.IsCancellationRequested)
                {
                    break;
                }
                client.NoDelay = true;
                string peer = client.Client.RemoteEndPoint is IPEndPoint remote
                    ? remote.Address.MapToIPv6().ToString() : "unknown";
                lock (_connectionGate)
                {
                    if (_connections.Count >= ResourceLimits.MaxConnections
                        || _connections.Values.Count(value => value.Peer == peer) >= ResourceLimits.MaxConnectionsPerPeer)
                    {
                        client.Dispose();
                        continue;
                    }
                    long id = ++_nextConnection;
                    // Start without a scheduling cancellation token: every accepted client owns cleanup,
                    // including a cancellation between Accept and execution of this delegate.
                    var completion = Task.Run(async () =>
                    {
                        try { await new RtspConnection(client, this).RunAsync(ct).ConfigureAwait(false); }
                        catch (Exception ex) { Log.Debug($"RTSP connection error: {Log.Flatten(ex)}"); }
                        finally
                        {
                            client.Dispose();
                            lock (_connectionGate) _connections.Remove(id);
                        }
                    }, CancellationToken.None);
                    _connections.Add(id, (client, peer, completion));
                }
            }
        }
        finally
        {
            listener.Stop();
            Task[] pending;
            lock (_connectionGate)
            {
                foreach (var connection in _connections.Values) connection.Client.Dispose();
                pending = _connections.Values.Select(value => value.Completion).ToArray();
            }
            await Task.WhenAll(pending).ConfigureAwait(false);
            ListeningPort = 0;
        }
    }
}
