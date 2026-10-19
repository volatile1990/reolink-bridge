// Copyright (c) 2026 Oluwabori Olaleye
// Licensed under the GNU Affero General Public License v3.0; see the LICENSE file
// in the repository root.
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Neolink.Media;
using Neolink.Streaming;

namespace Neolink.Rtsp;

/// <summary>One RTSP control connection and the sessions/pumps created on it.</summary>
public sealed class RtspConnection
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly RtspServer _server;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<string, RtspSession> _sessions = new();
    private readonly byte[] _readBuf = new byte[8192];
    private readonly EndPoint? _remote;
    /// <summary>Once-per-process flag for the "Opus requested but no usable ffmpeg" refusal log.</summary>
    private static int _opusRefusalLogged;
    /// <summary>Unsupported ?audio= values already logged: one line each (a retrying
    /// client would spam), capped so hostile scanners can't grow it unbounded.</summary>
    private static readonly HashSet<string> UnknownAudioLogged = new();
    /// <summary>The audio choice the last DESCRIBE on this connection settled on.
    /// SETUP falls back to it when its own URI carries no ?audio= — some clients
    /// resolve control URIs strictly per RFC 3986 and drop the query.</summary>
    private bool? _describedOpus;
    private StreamDescriptionSnapshot? _description;
    private RtspMount? _describedMount;
    private long _lastActivity;
    private CancellationToken _connectionToken;
    private int _readLen;
    private int _readPos;

    /// <summary>The user the last authorized request proved, or null (no login, or none needed).</summary>
    internal string? User { get; private set; }

    /// <summary>The client's IP, with IPv4-mapped addresses folded back to IPv4.</summary>
    internal string? RemoteAddress => Web.LoginGuard.ClientAddress((_remote as IPEndPoint)?.Address, null);

    internal RtspServer Server => _server;

    public RtspConnection(TcpClient client, RtspServer server)
    {
        _client = client;
        _stream = client.GetStream();
        _server = server;
        _remote = client.Client.RemoteEndPoint;
        _lastActivity = server.ResourceTimeProvider.GetTimestamp();
    }

    internal void Close() => _client.Dispose();
    internal void Touch() => Interlocked.Exchange(ref _lastActivity, _server.ResourceTimeProvider.GetTimestamp());

    public async Task RunAsync(CancellationToken ct)
    {
        Log.Debug($"RTSP client connected: {_remote}");
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _connectionToken = lifetime.Token;
        using var close = lifetime.Token.Register(Close);
        var watch = WatchIdleAsync(lifetime.Token);
        try
        {
            while (!lifetime.IsCancellationRequested)
            {
                int first = await PeekByteAsync(lifetime.Token).ConfigureAwait(false);
                if (first < 0) break;
                using var request = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
                request.CancelAfter(_server.ResourceLimits.RequestDeadline);

                if (first == '$')
                {
                    // Interleaved client data: RTCP receiver reports (ignored) or, on a
                    // backchannel channel, G.711 audio the client is talking to the camera.
                    var head = await ReadExactAsync(4, request.Token).ConfigureAwait(false);
                    byte channel = head[1];
                    int len = (head[2] << 8) | head[3];
                    var payload = await ReadExactAsync(len, request.Token).ConfigureAwait(false);
                    Touch();
                    RouteInterleaved(channel, payload);
                    continue;
                }

                var parsed = await ReadRequestAsync(request.Token).ConfigureAwait(false);
                if (parsed == null) break;
                Touch();
                await HandleRequestAsync(parsed, request.Token).ConfigureAwait(false);
            }
        }
        catch (IOException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { } // connection killed by the stalled-client guard
        catch (OperationCanceledException) { }
        finally
        {
            lifetime.Cancel();
            await Task.WhenAll(_sessions.Values.Select(session => session.StopAsync())).ConfigureAwait(false);
            _sessions.Clear();
            try { await watch.ConfigureAwait(false); } catch (OperationCanceledException) { }
            _client.Dispose();
            _writeLock.Dispose();
            Log.Debug($"RTSP client disconnected: {_remote}");
        }
    }

    private async Task WatchIdleAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_server.ResourceLimits.WatchInterval);
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            foreach (var (id, session) in _sessions)
                if (session.IdleAge >= _server.ResourceLimits.SessionIdleTimeout && _sessions.TryRemove(id, out var expired))
                    await expired.StopAsync().ConfigureAwait(false);
            if (_server.ResourceTimeProvider.GetElapsedTime(Interlocked.Read(ref _lastActivity))
                >= _server.ResourceLimits.ConnectionIdleTimeout)
            { Close(); return; }
        }
    }

    // ------------------------------------------------------------- request parsing

    private sealed class RtspRequest
    {
        public required string Method;
        public required string Uri;
        public readonly Dictionary<string, string> Headers = new(StringComparer.OrdinalIgnoreCase);
        public string CSeq => Headers.GetValueOrDefault("CSeq", "0");
        public string? Header(string name) => Headers.GetValueOrDefault(name);
    }

    private async Task<RtspRequest?> ReadRequestAsync(CancellationToken ct)
    {
        string? requestLine = await ReadLineAsync(ct).ConfigureAwait(false);
        while (requestLine == "")
            requestLine = await ReadLineAsync(ct).ConfigureAwait(false);
        if (requestLine == null) return null;

        var parts = requestLine.Split(' ', 3);
        if (parts.Length < 3)
        {
            Log.Debug($"Malformed RTSP request line: {requestLine}");
            return null;
        }
        var req = new RtspRequest { Method = parts[0].ToUpperInvariant(), Uri = parts[1] };
        int headerBytes = requestLine.Length + 2, headerCount = 0;
        if (headerBytes > _server.ResourceLimits.MaxHeaderBytes)
            throw new IOException("RTSP request line exceeds its resource budget");

        while (true)
        {
            var line = await ReadLineAsync(ct).ConfigureAwait(false);
            if (line == null) return null;
            if (line.Length == 0) break;
            headerBytes += line.Length + 2;
            if (++headerCount > _server.ResourceLimits.MaxHeaderCount || headerBytes > _server.ResourceLimits.MaxHeaderBytes)
                throw new IOException("RTSP request headers exceed their resource budget");
            int colon = line.IndexOf(':');
            if (colon <= 0 || !req.Headers.TryAdd(line[..colon].Trim(), line[(colon + 1)..].Trim()))
                throw new IOException("Malformed or repeated RTSP header");
        }

        if (req.Header("Content-Length") is { } length)
        {
            if (!int.TryParse(length, NumberStyles.None, CultureInfo.InvariantCulture, out int bodyLen)
                || bodyLen > _server.ResourceLimits.MaxRequestBodyBytes)
            {
                await RespondAsync(req, 413, "Request Entity Too Large", ct: ct).ConfigureAwait(false);
                throw new IOException("RTSP request body exceeds its resource budget");
            }
            if (bodyLen > 0) await ReadExactAsync(bodyLen, ct).ConfigureAwait(false); // bounded read and discard
        }

        return req;
    }

    // ------------------------------------------------------------- request handling

    private async Task HandleRequestAsync(RtspRequest req, CancellationToken ct)
    {
        Log.Debug($"RTSP {req.Method} {req.Uri}");
        switch (req.Method)
        {
            case "OPTIONS":
                await RespondAsync(req, 200, "OK",
                    "Public: OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, GET_PARAMETER, SET_PARAMETER, TEARDOWN",
                    ct: ct).ConfigureAwait(false);
                break;
            case "DESCRIBE":
                await HandleDescribeAsync(req, ct).ConfigureAwait(false);
                break;
            case "SETUP":
                await HandleSetupAsync(req, ct).ConfigureAwait(false);
                break;
            case "PLAY":
                await HandlePlayAsync(req, ct).ConfigureAwait(false);
                break;
            case "PAUSE":
                await HandlePauseAsync(req, ct).ConfigureAwait(false);
                break;
            case "GET_PARAMETER":
            case "SET_PARAMETER":
                FindSession(req)?.Touch();
                await RespondAsync(req, 200, "OK", sessionId: req.Header("Session"), ct: ct).ConfigureAwait(false);
                break;
            case "TEARDOWN":
                await HandleTeardownAsync(req, ct).ConfigureAwait(false);
                break;
            default:
                await RespondAsync(req, 501, "Not Implemented", ct: ct).ConfigureAwait(false);
                break;
        }
    }

    private (RtspMount? mount, string path, int trackId, string? audio) ResolveUri(string uri)
    {
        var (path, trackId, audio) = ParseUri(uri);
        return (_server.FindMount(path), path, trackId, audio);
    }

    /// <summary>Splits a request URI into mount path, trackID and the ?audio= query
    /// value. The SDP's control attribute is the relative "trackID=N", and clients
    /// resolve it against our Content-Base in two different-looking ways once a
    /// query is in play: a URL-aware resolver joins the PATH and keeps the query
    /// last ("/cam/trackID=1?audio=opus"), while one that concatenates the
    /// Content-Base string verbatim leaves the marker trailing the query
    /// ("/cam?audio=opus/trackID=1"). Both must resolve to the same track — reading
    /// the trackID out of only one of the two positions silently turns the audio
    /// SETUP into a second video track, and the stream plays with no sound.</summary>
    internal static (string path, int trackId, string? audio) ParseUri(string uri)
    {
        string rest = uri;
        if (Uri.TryCreate(uri, UriKind.Absolute, out var parsed))
            rest = parsed.PathAndQuery;

        string path = rest, query = "";
        int q = rest.IndexOf('?');
        if (q >= 0)
        {
            path = rest[..q];
            query = rest[(q + 1)..];
        }

        // Query first: that is where a verbatim Content-Base concatenation puts it.
        int trackId = TakeTrackId(ref query);
        if (trackId < 0) trackId = TakeTrackId(ref path);

        string? audio = null;
        // Only "audio" is understood today; unknown KEYS are ignored so future
        // parameters can be added without breaking older servers or clients.
        foreach (var pair in query.Split('&'))
        {
            var kv = pair.Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals("audio", StringComparison.OrdinalIgnoreCase))
                audio = Uri.UnescapeDataString(kv[1]).Trim().ToLowerInvariant();
        }
        return (Uri.UnescapeDataString(path), trackId, audio);
    }

    /// <summary>Cuts a "/trackID=N" marker out of <paramref name="s"/> and returns N,
    /// or -1 when there is none. Only the DIGITS are consumed, so whatever follows
    /// the id (a query the client kept on the end) survives in place.</summary>
    private static int TakeTrackId(ref string s)
    {
        const string marker = "/trackID=";
        int idx = s.LastIndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return -1;
        int start = idx + marker.Length, end = start;
        while (end < s.Length && char.IsAsciiDigit(s[end])) end++;
        if (end == start || !int.TryParse(s[start..end], out int id)) return -1;
        s = s[..idx] + s[end..];
        return id;
    }

    /// <summary>Maps a ?audio= value to a session's Opus choice: null when the URL
    /// names none (the mount's audio_transcode default applies), true/false for
    /// opus/original. Returns false for a value that is not a supported format.</summary>
    internal static bool TryMapAudio(string? audio, out bool? opus)
    {
        opus = audio switch { "opus" => true, "original" => false, _ => null };
        return audio is null or "opus" or "original";
    }

    private async Task<bool> CheckAuthAsync(RtspRequest req, RtspMount mount, CancellationToken ct)
    {
        var header = req.Header("Authorization");
        if (_server.Authorize(mount, header))
        {
            User = _server.VerifiedUser(header);
            return true;
        }
        await RespondAsync(req, 401, "Unauthorized", "WWW-Authenticate: Basic realm=\"neolink\"", ct: ct).ConfigureAwait(false);
        return false;
    }

    private async Task HandleDescribeAsync(RtspRequest req, CancellationToken ct)
    {
        var (mount, path, _, audio) = ResolveUri(req.Uri);
        if (mount == null)
        {
            await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        if (!await CheckAuthAsync(req, mount, ct).ConfigureAwait(false)) return;

        // ?audio= picks this client's codec. An unsupported format is refused
        // loudly — a silent fallback would leave the user debugging why the
        // codec they asked for never arrived.
        if (!TryMapAudio(audio, out var audioChoice))
        {
            bool logIt;
            lock (UnknownAudioLogged)
                logIt = UnknownAudioLogged.Count < 16 && UnknownAudioLogged.Add(audio!);
            if (logIt)
                Log.Warn($"DESCRIBE {path}: unsupported audio format \"{audio}\" — " +
                         "supported: ?audio=opus, ?audio=original — answering 404");
            await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        bool opus = !mount.GopPlayout && (audioChoice ?? mount.Opus);
        if (mount.MatchHqTiming != null && opus)
        { await RespondAsync(req, 404, "Matched LQ Requires Original AAC", ct: ct).ConfigureAwait(false); return; }
        if (mount.GopAacAvailable && audioChoice == true)
        { await RespondAsync(req, 404, "Unsupported Audio Format", ct: ct).ConfigureAwait(false); return; }

        // Opus is only honest when the located ffmpeg can actually encode it
        // (probed once, lazily). Refusing here beats promising Opus in the SDP
        // and delivering silence.
        if (opus && !Media.Ffmpeg.SupportsOpus)
        {
            if (Interlocked.Exchange(ref _opusRefusalLogged, 1) == 0)
                Log.Warn($"DESCRIBE {path}: Opus audio was requested but " +
                         (Media.Ffmpeg.ExePath == null
                             ? "no ffmpeg was found (install one on PATH or set NEOLINK_FFMPEG)"
                             : "this ffmpeg has no libopus encoder") +
                         " — ?audio=opus answers 404 until that changes");
            await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        if (!await mount.Hub.WaitForDescribeInfoAsync(TimeSpan.FromSeconds(12), ct).ConfigureAwait(false))
        {
            Log.Warn($"DESCRIBE {path}: stream not ready (camera offline or still connecting)");
            await RespondAsync(req, 503, "Service Unavailable", "Retry-After: 5", ct: ct).ConfigureAwait(false);
            return;
        }

        // ONVIF backchannel is opt-in: only add the sendonly talk track when the
        // client asks for it (Require: ...backchannel) AND the camera has a speaker.
        // Plain players (VLC, ffmpeg, go2rtc without backchannel) never send the
        // header, so their SDP is unchanged.
        bool backchannel = false;
        if (!mount.GopPlayout && WantsBackchannel(req) && mount.Talk != null && mount.TalkAllowedFor(User))
        {
            try
            {
                var caps = await mount.Talk.GetCapabilitiesAsync(ct).ConfigureAwait(false);
                backchannel = caps.Features.Talk;
            }
            catch (Exception ex)
            {
                Log.Debug($"{mount.Hub.Name}: backchannel capability probe failed: {Log.Flatten(ex)}");
            }
        }

        if (!mount.Hub.TryGetCurrentDescription(out var description))
        {
            await RespondAsync(req, 503, "Service Unavailable", "Retry-After: 5", ct: ct).ConfigureAwait(false);
            return;
        }
        if (mount.GopPlayout && !mount.GopAacAvailable) description = description with { Audio = null };
        if (mount.MatchHqTiming != null && description.Audio is { IsAac: false })
        { await RespondAsync(req, 503, "Matched LQ Requires Original AAC", "Retry-After: 5", ct: ct).ConfigureAwait(false); return; }
        _description = description;
        _describedMount = mount;
        _describedOpus = opus;
        string sdp = Sdp.Build(description, mount.Hub.Name, backchannel: backchannel, opus: opus);
        string contentBase = req.Uri.Split('?')[0].TrimEnd('/') + "/";
        await RespondAsync(req, 200, "OK",
            $"Content-Base: {contentBase}\r\nContent-Type: application/sdp",
            body: Encoding.ASCII.GetBytes(sdp), ct: ct).ConfigureAwait(false);
    }

    /// <summary>Whether the request opts into the ONVIF two-way-talk backchannel.</summary>
    private static bool WantsBackchannel(RtspRequest req)
    {
        var require = req.Header("Require");
        return require != null && require.Contains("backchannel", StringComparison.OrdinalIgnoreCase);
    }

    private async Task HandleSetupAsync(RtspRequest req, CancellationToken ct)
    {
        var (mount, _, trackId, audio) = ResolveUri(req.Uri);
        if (mount == null)
        {
            await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        if (!await CheckAuthAsync(req, mount, ct).ConfigureAwait(false)) return;
        if (trackId == Sdp.BackchannelTrackId && !mount.TalkAllowedFor(User))
        { await RespondAsync(req, 403, "Forbidden", ct: ct).ConfigureAwait(false); return; }
        if (trackId is not (0 or 1 or Sdp.BackchannelTrackId))
        { await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false); return; }
        if (mount.GopPlayout && trackId != 0 && !(trackId == 1 && mount.GopAacAvailable && audio != "opus"))
        {
            await RespondAsync(req, 404, "Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        if (mount.MatchHqTiming != null && audio == "opus")
        { await RespondAsync(req, 404, "Matched LQ Requires Original AAC", ct: ct).ConfigureAwait(false); return; }

        var transportHeader = req.Header("Transport");
        if (transportHeader == null)
        {
            await RespondAsync(req, 461, "Unsupported Transport", ct: ct).ConfigureAwait(false);
            return;
        }
        var spec = ParseTransport(transportHeader);
        if (spec == null || _server.TcpOnly && !spec.Tcp
            || trackId == Sdp.BackchannelTrackId && (mount.Talk == null || !spec.Tcp))
        { await RespondAsync(req, 461, "Unsupported Transport", ct: ct).ConfigureAwait(false); return; }

        // Existing PLAY state takes precedence over a new track's metadata. This preserves
        // the RTSP state contract while still checking current metadata before any new SETUP.
        RtspSession? session = null;
        var sessionId = req.Header("Session");
        if (sessionId != null)
        {
            _sessions.TryGetValue(sessionId.Split(';')[0].Trim(), out session);
            if (session == null)
            { await RespondAsync(req, 454, "Session Not Found", ct: ct).ConfigureAwait(false); return; }
            if (!session.SameMediaPolicy(mount) || session.Playing)
            { await RespondAsync(req, 455, "Method Not Valid in This State", sessionId: session.Id, ct: ct).ConfigureAwait(false); return; }
        }
        if (!mount.Hub.TryGetCurrentDescription(out var current))
        { await RespondAsync(req, 503, "Service Unavailable", ct: ct).ConfigureAwait(false); return; }
        bool describedPolicyMatches = _describedMount?.SameMediaPolicy(mount) is true;
        var description = describedPolicyMatches && _description is { } described
            ? described : current;
        if (description.SourceEpoch != current.SourceEpoch || description.Codec != current.Codec
            || trackId == 1 && (description.Audio == null || !SameAudio(description.Audio, current.Audio)))
        { await RespondAsync(req, 503, "Describe Current Stream First", ct: ct).ConfigureAwait(false); return; }

        if (session == null)
        {
            if (_sessions.Count >= _server.ResourceLimits.MaxSessionsPerConnection)
            { await RespondAsync(req, 453, "Not Enough Bandwidth", ct: ct).ConfigureAwait(false); return; }
            // The codec choice travels on the query: this URI's own ?audio= wins,
            // else what the DESCRIBE on this connection settled on, else the mount
            // default. Unknown values were already policed at DESCRIBE.
            bool opus = !mount.GopPlayout && ((TryMapAudio(audio, out var choice) ? choice : null)
                ?? (describedPolicyMatches ? _describedOpus : null) ?? mount.Opus);
            if (mount.MatchHqTiming != null && opus)
            { await RespondAsync(req, 404, "Matched LQ Requires Original AAC", ct: ct).ConfigureAwait(false); return; }
            if (opus && !Media.Ffmpeg.SupportsOpus)
                opus = false; // DESCRIBE-less client; never promise silence
            session = new RtspSession(this, mount, opus, description);
            _sessions[session.Id] = session;
        }

        string responseTransport;

        if (trackId == Sdp.BackchannelTrackId)
        {
            // Backchannel: the client SENDS G.711 audio to us. Only TCP-interleaved
            // is accepted (go2rtc's default; forces a retry for the rare UDP case).
            if (mount.Talk == null || !spec.Tcp)
            {
                await RespondAsync(req, 461, "Unsupported Transport", sessionId: session.Id, ct: ct).ConfigureAwait(false);
                return;
            }
            int ch = spec.InterleavedRtp ?? Sdp.BackchannelTrackId * 2;
            await session.SetupBackchannelAsync((byte)ch, mount.Talk!).ConfigureAwait(false);
            responseTransport = $"RTP/AVP/TCP;unicast;mode=record;interleaved={ch}-{ch + 1}";
        }
        else
        {
            var packetizer = new RtpPacketizer(trackId == 0 ? Sdp.VideoPayloadType : Sdp.AudioPayloadType);
            if (spec.Tcp)
            {
                int ch = spec.InterleavedRtp ?? trackId * 2;
                session.SetTrack(trackId, TrackTransport.ForTcp((byte)ch, packetizer));
                responseTransport = $"RTP/AVP/TCP;unicast;interleaved={ch}-{ch + 1};ssrc={packetizer.Ssrc:X8}";
            }
            else if (spec.ClientRtpPort.HasValue)
            {
                var remoteIp = ((IPEndPoint)_client.Client.RemoteEndPoint!).Address;
                var target = new IPEndPoint(remoteIp, spec.ClientRtpPort.Value);
                var (transport, rtpPort, rtcpPort) = TrackTransport.ForUdp(target, packetizer);
                session.SetTrack(trackId, transport);
                responseTransport =
                    $"RTP/AVP;unicast;client_port={spec.ClientRtpPort}-{spec.ClientRtpPort + 1};" +
                    $"server_port={rtpPort}-{rtcpPort};ssrc={packetizer.Ssrc:X8}";
            }
            else
            {
                await RespondAsync(req, 461, "Unsupported Transport", ct: ct).ConfigureAwait(false);
                return;
            }
        }

        session.Touch();
        await RespondAsync(req, 200, "OK",
            $"Transport: {responseTransport}",
            sessionId: session.Id, ct: ct).ConfigureAwait(false);
    }

    internal static bool SameAudio(AudioTrackInfo? expected, AudioTrackInfo? actual) => expected != null && actual != null
        && expected.IsAac == actual.IsAac && expected.SampleRate == actual.SampleRate && expected.Channels == actual.Channels
        && (expected.AudioSpecificConfig == actual.AudioSpecificConfig
            || expected.AudioSpecificConfig != null && actual.AudioSpecificConfig != null
            && expected.AudioSpecificConfig.SequenceEqual(actual.AudioSpecificConfig));

    private sealed record TransportSpec(bool Tcp, int? InterleavedRtp, int? ClientRtpPort);

    private static TransportSpec? ParseTransport(string header)
    {
        // Clients may offer several transports separated by ','; take the first supported one.
        foreach (var offer in header.Split(','))
        {
            var fields = offer.Trim().Split(';');
            var proto = fields[0].Trim().ToUpperInvariant();
            bool tcp = proto == "RTP/AVP/TCP";
            if (proto is not ("RTP/AVP" or "RTP/AVP/TCP")) continue;

            int? interleaved = null, clientPort = null;
            bool invalid = false;
            foreach (var f in fields.Skip(1))
            {
                if (f.Trim().Equals("multicast", StringComparison.OrdinalIgnoreCase)) { invalid = true; break; }
                var kv = f.Split('=', 2);
                var key = kv[0].Trim().ToLowerInvariant();
                if (kv.Length == 2)
                {
                    var range = kv[1].Split('-');
                    if (key is "interleaved" or "client_port")
                    {
                        int maximum = key == "interleaved" ? 254 : 65534;
                        int minimum = key == "interleaved" ? 0 : 1;
                        if (!int.TryParse(range[0], out int value) || value < minimum || value > maximum
                            || range.Length > 2 || range.Length == 2 && (!int.TryParse(range[1], out int end) || end != value + 1))
                        { invalid = true; break; }
                        if (key == "interleaved") interleaved = value;
                        else clientPort = value;
                    }
                }
            }
            if (!invalid && (tcp || clientPort.HasValue))
                return new TransportSpec(tcp, interleaved, clientPort);
        }
        return null;
    }

    private async Task HandlePlayAsync(RtspRequest req, CancellationToken ct)
    {
        var session = FindSession(req);
        if (session == null)
        {
            await RespondAsync(req, 454, "Session Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        if (session.GopAudioWithoutVideo)
        { await RespondAsync(req, 455, "GOP audio requires its video track", sessionId: session.Id, ct: ct).ConfigureAwait(false); return; }
        await session.PlayAsync(_connectionToken).ConfigureAwait(false);

        var rtpInfoParts = new List<string>();
        string baseUri = req.Uri.TrimEnd('/');
        if (session.Video != null)
            rtpInfoParts.Add($"url={baseUri}/trackID=0;seq={session.Video.Packetizer.Seq}");
        if (session.Audio != null)
            rtpInfoParts.Add($"url={baseUri}/trackID=1;seq={session.Audio.Packetizer.Seq}");

        await RespondAsync(req, 200, "OK",
            $"Range: npt=now-\r\nRTP-Info: {string.Join(",", rtpInfoParts)}",
            sessionId: session.Id, ct: ct).ConfigureAwait(false);
    }

    private async Task HandlePauseAsync(RtspRequest req, CancellationToken ct)
    {
        var session = FindSession(req);
        if (session == null)
        {
            await RespondAsync(req, 454, "Session Not Found", ct: ct).ConfigureAwait(false);
            return;
        }
        await session.PauseAsync().ConfigureAwait(false);
        await RespondAsync(req, 200, "OK", sessionId: session.Id, ct: ct).ConfigureAwait(false);
    }

    private async Task HandleTeardownAsync(RtspRequest req, CancellationToken ct)
    {
        var session = FindSession(req);
        if (session != null)
        {
            _sessions.TryRemove(session.Id, out _);
            await session.StopAsync().ConfigureAwait(false);
        }
        await RespondAsync(req, 200, "OK", sessionId: session?.Id, ct: ct).ConfigureAwait(false);
    }

    /// <summary>Routes an interleaved client frame to the backchannel receiver that
    /// owns its channel; anything else (RTCP receiver reports) is ignored.</summary>
    private void RouteInterleaved(byte channel, byte[] rtpPacket)
    {
        foreach (var s in _sessions.Values)
            if (s.BackchannelChannel == channel)
            {
                s.FeedBackchannel(rtpPacket);
                return;
            }
    }

    private RtspSession? FindSession(RtspRequest req)
    {
        var id = req.Header("Session");
        if (id == null)
            return _sessions.Count == 1 ? _sessions.Values.First() : null;
        return _sessions.GetValueOrDefault(id.Split(';')[0].Trim());
    }

    // ------------------------------------------------------------- responses & I/O

    private async Task RespondAsync(RtspRequest req, int code, string reason,
        string? extraHeaders = null, string? sessionId = null, byte[]? body = null, CancellationToken ct = default)
    {
        var sb = new StringBuilder();
        sb.Append($"RTSP/1.0 {code} {reason}\r\n");
        sb.Append($"CSeq: {req.CSeq}\r\n");
        sb.Append("Server: Neolink.NET\r\n");
        if (sessionId != null) sb.Append($"Session: {sessionId};timeout={Math.Max(1, (int)_server.ResourceLimits.SessionIdleTimeout.TotalSeconds)}\r\n");
        if (extraHeaders != null) sb.Append(extraHeaders).Append("\r\n");
        sb.Append($"Content-Length: {body?.Length ?? 0}\r\n");
        sb.Append("\r\n");

        var head = Encoding.ASCII.GetBytes(sb.ToString());
        byte[] response = new byte[head.Length + (body?.Length ?? 0)];
        head.CopyTo(response, 0);
        body?.CopyTo(response, head.Length);
        await WriteGuardedAsync(response, ct).ConfigureAwait(false);
    }

    /// <summary>Sends an RTP packet over the control connection (interleaved framing).</summary>
    internal async Task SendInterleavedAsync(byte channel, byte[] rtpPacket, CancellationToken ct)
    {
        var frame = new byte[4 + rtpPacket.Length];
        frame[0] = 0x24; // '$'
        frame[1] = channel;
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(2), (ushort)rtpPacket.Length);
        rtpPacket.CopyTo(frame, 4);
        await WriteGuardedAsync(frame, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends one frame's worth of RTP packets as a SINGLE write. Interleaved video
    /// used to cost one send syscall per RTP packet — ~700/s per viewer on a 4K
    /// stream, which is real CPU under virtualization. The batch arrives already
    /// '$'-framed (see <see cref="RtpBatch"/>), so no copy happens here either.
    /// </summary>
    internal Task SendInterleavedRawAsync(ReadOnlyMemory<byte> framed, CancellationToken ct) =>
        framed.IsEmpty ? Task.CompletedTask : WriteGuardedAsync(framed, ct);

    private async Task WriteGuardedAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
        // Responses and media both use this guard, including the shared lock wait.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10)); // slow-client guard
        try
        {
            await _writeLock.WaitAsync(cts.Token).ConfigureAwait(false);
            try
            {
                await _stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // The client stopped reading. A half-dead connection is worse than a dead
            // one (the client sits blind until its own watchdog fires), so close it
            // outright; that also wakes the request loop, which cleans up the sessions.
            Log.Warn($"RTSP client {_remote} stalled (did not accept data for 10s); closing connection");
            _client.Dispose();
            throw new IOException("RTSP client stalled");
        }
    }

    private async Task<int> PeekByteAsync(CancellationToken ct)
    {
        if (_readPos >= _readLen)
        {
            _readLen = await _stream.ReadAsync(_readBuf, ct).ConfigureAwait(false);
            _readPos = 0;
            if (_readLen == 0) return -1;
        }
        return _readBuf[_readPos];
    }

    private async Task<byte[]> ReadExactAsync(int count, CancellationToken ct)
    {
        var result = new byte[count];
        int done = 0;
        while (done < count)
        {
            if (_readPos >= _readLen)
            {
                _readLen = await _stream.ReadAsync(_readBuf, ct).ConfigureAwait(false);
                _readPos = 0;
                if (_readLen == 0) throw new IOException("connection closed");
            }
            int n = Math.Min(count - done, _readLen - _readPos);
            Array.Copy(_readBuf, _readPos, result, done, n);
            _readPos += n;
            done += n;
        }
        return result;
    }

    private async Task<string?> ReadLineAsync(CancellationToken ct)
    {
        var sb = new StringBuilder();
        while (true)
        {
            if (_readPos >= _readLen)
            {
                _readLen = await _stream.ReadAsync(_readBuf, ct).ConfigureAwait(false);
                _readPos = 0;
                if (_readLen == 0) return sb.Length > 0 ? sb.ToString() : null;
            }
            byte b = _readBuf[_readPos++];
            if (b == '\n')
                return sb.ToString().TrimEnd('\r');
            sb.Append((char)b);
            if (sb.Length > 16384) throw new IOException("RTSP line too long");
        }
    }
}

// ===================================================================== session

internal sealed class TrackTransport
{
    public bool Tcp { get; private init; }
    public byte RtpChannel { get; private init; }
    public IPEndPoint? ClientEndpoint { get; private init; }
    public Socket? UdpSocket { get; private init; }
    public Socket? UdpRtcpSocket { get; private init; }
    public required RtpPacketizer Packetizer { get; init; }

    public static TrackTransport ForTcp(byte channel, RtpPacketizer packetizer) =>
        new() { Tcp = true, RtpChannel = channel, Packetizer = packetizer };

    public static (TrackTransport transport, int rtpPort, int rtcpPort) ForUdp(IPEndPoint client, RtpPacketizer packetizer)
    {
        var rtp = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        var rtcp = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        rtp.Bind(new IPEndPoint(IPAddress.Any, 0));
        rtcp.Bind(new IPEndPoint(IPAddress.Any, 0));
        var t = new TrackTransport
        {
            Tcp = false,
            ClientEndpoint = client,
            UdpSocket = rtp,
            UdpRtcpSocket = rtcp,
            Packetizer = packetizer,
        };
        return (t, ((IPEndPoint)rtp.LocalEndPoint!).Port, ((IPEndPoint)rtcp.LocalEndPoint!).Port);
    }

    public void Close()
    {
        UdpSocket?.Dispose();
        UdpRtcpSocket?.Dispose();
    }
}

internal sealed class RtspSession
{
    internal bool SameMediaPolicy(RtspMount mount) => _mount.SameMediaPolicy(mount);
    public string Id { get; } = Convert.ToHexString(Guid.NewGuid().ToByteArray()[..8]);
    public TrackTransport? Video { get; private set; }
    public TrackTransport? Audio { get; private set; }
    public bool Playing => _pumpTask is { IsCompleted: false };
    public bool GopAudioWithoutVideo => _mount.GopPlayout && _mount.GopAudio && Audio != null && Video == null;

    /// <summary>Interleaved channel the client sends backchannel audio on, or null.</summary>
    public byte? BackchannelChannel { get; private set; }

    private readonly RtspConnection _conn;
    private readonly RtspMount _mount;
    /// <summary>This session's audio: transcoded Opus or the camera's original
    /// track. Settled at SETUP from the URL's ?audio= (falling back to the mount's
    /// audio_transcode default) and fixed for the session's lifetime.</summary>
    private readonly bool _opus;
    private readonly StreamDescriptionSnapshot _description;
    private readonly object _stateGate = new();
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private bool _stopped;
    private long _lastActivity;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;
    private BackchannelReceiver? _backchannel;

    /// <summary>The user the session's SETUP proved; fixed for the session's life.</summary>
    internal string? User { get; }

    internal IStreamHub Hub => _mount.Hub;
    internal TimeSpan IdleAge => _conn.Server.ResourceTimeProvider.GetElapsedTime(Interlocked.Read(ref _lastActivity));
    internal void Touch()
    {
        Interlocked.Exchange(ref _lastActivity, _conn.Server.ResourceTimeProvider.GetTimestamp());
        _conn.Touch();
    }

    public RtspSession(RtspConnection conn, RtspMount mount, bool opus, StreamDescriptionSnapshot? description = null)
    {
        _conn = conn;
        _mount = mount;
        _opus = opus;
        if (description == null)
        {
            if (!mount.Hub.TryGetCurrentDescription(out var current))
                throw new IOException("RTSP stream description is unavailable");
            description = current;
        }
        _description = description;
        User = conn.User;
        Touch();
    }

    public void SetTrack(int trackId, TrackTransport transport)
    {
        lock (_stateGate)
        {
            if (_stopped) { transport.Close(); throw new IOException("RTSP session has expired"); }
            if (trackId == 0) { Video?.Close(); Video = transport; }
            else { Audio?.Close(); Audio = transport; }
        }
    }

    /// <summary>Registers a two-way-talk receive track on an interleaved channel.</summary>
    public void SetupBackchannel(byte channel, ICameraControl talk) => _ = SetupBackchannelAsync(channel, talk);

    public async Task SetupBackchannelAsync(byte channel, ICameraControl talk)
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_stopped) throw new IOException("RTSP session has expired");
            if (_backchannel != null) await _backchannel.StopAsync().ConfigureAwait(false);
            BackchannelChannel = channel;
            _backchannel = new BackchannelReceiver(talk);
        }
        finally { _lifecycle.Release(); }
    }

    public void FeedBackchannel(byte[] rtpPacket) { _backchannel?.OnRtp(rtpPacket); Touch(); }

    public void Play(CancellationToken ct) => _ = PlayAsync(ct);

    public async Task PlayAsync(CancellationToken ct)
    {
        await _lifecycle.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_stopped) throw new IOException("RTSP session has expired");
            Touch();
            // PAUSE retains talk; TEARDOWN and disconnect close it.
            _backchannel?.Start(ct);
            if (Playing) return;
            await StopPumpAsync().ConfigureAwait(false);
            _pumpCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var token = _pumpCts.Token;
            _pumpTask = Task.Run(() => PumpAsync(token), CancellationToken.None);
        }
        finally { _lifecycle.Release(); }
    }

    public void Pause() => _ = PauseAsync();

    public async Task PauseAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try { Touch(); await StopPumpAsync().ConfigureAwait(false); }
        finally { _lifecycle.Release(); }
    }

    public void Stop() => _ = StopAsync();

    public async Task StopAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            lock (_stateGate) _stopped = true;
            await StopPumpAsync().ConfigureAwait(false);
            if (_backchannel != null) await _backchannel.StopAsync().ConfigureAwait(false);
            lock (_stateGate) { Video?.Close(); Audio?.Close(); }
        }
        finally { _lifecycle.Release(); }
    }

    private async Task StopPumpAsync()
    {
        _pumpCts?.Cancel();
        try { if (_pumpTask != null) await _pumpTask.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Log.Debug($"RTSP media pump stopped: {ex.GetType().Name}"); }
        _pumpTask = null;
        _pumpCts?.Dispose();
        _pumpCts = null;
    }

    private async Task PumpAsync(CancellationToken ct)
    {
        var hub = _mount.Hub;
        var watch = _conn.Server.Viewers.Add(hub, "RTSP", _conn.RemoteAddress, User);
        var reader = watch.Reader;
        long lastIndex = -1;
        bool waitKeyframe = true; // always start on a keyframe
        // The explicit opt-in mount advertises/accepts video only. Audio's sample
        // clock has no shared camera PTS epoch and cannot use this normalization.
        var playout = _mount.GopPlayout && Video != null && (Audio == null || _mount.GopAudio)
            ? new GopVideoPlayout(_description.Codec ?? VideoCodec.H264, _mount.PlayoutDelayMs,
                _conn.Server.PlayoutTimeProvider, _conn.Server.PlayoutWaiter)
            : null;
        int aacRate = _mount.GopAudio && _description.Audio is { IsAac: true } describedAudio ? describedAudio.SampleRate : 0;
        byte[]? aacAsc = aacRate > 0 ? _description.Audio!.AudioSpecificConfig?.ToArray() : null;
        var aacTimeline = playout != null && Audio != null && aacRate > 0 ? new GopAacTimeline(aacRate, hub.SourceEpoch) : null;
        var matchedLow = _mount.MatchHqTiming is { } timing
            ? new MatchedLowQualityPlayout(hub, timing, _conn.Server.PlayoutTimeProvider, _conn.Server.PlayoutWaiter) : null;
        var lowSenderClock = _mount.LowQualitySenderReports && !_mount.GopPlayout && !_opus
            ? new LowQualityRtpSenderClock(hub, _conn.Server.PlayoutTimeProvider, _mount.LowQualityRequiresSourceArrival) : null;
        long? lastRtcpAt = null;
        string cname = "reolink-gop-" + Id.ToLowerInvariant();
        long sourceEpoch = hub.SourceEpoch;
        bool? uniformCadence = null;
        // Owned by THIS pump, reused per frame: each send is awaited before the
        // next frame packetizes, and a PAUSE/PLAY successor pump gets its own
        // buffer rather than resetting one a cancelled write may still be reading.
        var batch = new RtpBatch();

        // Opus sessions vote for the transcoder: ffmpeg runs only while at least
        // one Opus session is playing, and stops with the last one.
        if (_opus) hub.AcquireOpus();
        if (lowSenderClock != null) _conn.Server.ObserveLowSenderReports(Id, lowSenderClock.Diagnostics());
        Log.Info($"{hub.Name}: client started streaming (session {Id}" +
                 $"{(_opus ? ", Opus audio" : "")})");
        if (playout != null)
            Log.Info($"{hub.Name}: RTSP GOP playout enabled, reserve={_mount.PlayoutDelayMs}ms ({(aacTimeline == null ? "video-only" : "original AAC; arrival-estimated synchronization")})");
        try
        {
            if (matchedLow != null)
            {
                await PumpMatchedLowAsync(hub, reader, matchedLow, lowSenderClock, batch, ct).ConfigureAwait(false);
                return;
            }
            await foreach (var packet in reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                bool gap = lastIndex >= 0 && packet.Index != lastIndex + 1;
                lastIndex = packet.Index;
                if (gap)
                {
                    waitKeyframe = true;
                    playout?.Reset();
                    aacTimeline?.Reset(hub.SourceEpoch);
                }
                if (playout != null && sourceEpoch != hub.SourceEpoch)
                {
                    sourceEpoch = hub.SourceEpoch;
                    waitKeyframe = true;
                    playout.Reset();
                    aacTimeline?.Reset(sourceEpoch);
                }

                switch (packet)
                {
                    case HubVideo v when Video != null:
                    {
                        if (playout != null && v.SourceEpoch != sourceEpoch) continue;
                        if (waitKeyframe)
                        {
                            if (!v.Keyframe) continue;
                            waitKeyframe = false;
                        }
                        if (playout != null)
                        {
                            var complete = playout.Push(v);
                            if (complete == null) continue;
                            if (uniformCadence.HasValue && uniformCadence.Value != complete.UniformCadence)
                                Log.Info($"{hub.Name}: RTSP GOP cadence changed to {(complete.UniformCadence ? "uniform" : "source")}, " +
                                    $"pictures={complete.Pictures.Count}, duration={complete.Duration.TotalMilliseconds:F1}ms");
                            uniformCadence = complete.UniformCadence;
                            complete = playout.BeginGop(complete);
                            _mount.PublishHqTiming?.Observe(Id, complete, playout.PresentationTimestamp(TimeSpan.Zero));
                            var aacPlans = aacTimeline?.Plan(complete) ?? [];
                            var presentation = complete.Pictures.Select(picture => (Offset: picture.Offset, Video: (GopPlayoutPicture?)picture, Audio: (GopAacPlan?)null))
                                .Concat(aacPlans.Select(aac => (Offset: aac.Offset, Video: (GopPlayoutPicture?)null, Audio: (GopAacPlan?)aac)))
                                .OrderBy(item => item.Offset).ThenBy(item => item.Video == null ? 1 : 0);
                            foreach (var item in presentation)
                            {
                                await playout.WaitAsync(item.Offset, ct).ConfigureAwait(false);
                                // A source may stop during the deliberate wait.
                                if (complete.SourceEpoch != hub.SourceEpoch)
                                {
                                    waitKeyframe = true;
                                    playout.Reset();
                                    aacTimeline?.Reset(hub.SourceEpoch);
                                    break;
                                }
                                if (item.Video is { } picture)
                                {
                                    await SendVideoNalsAsync(hub, picture.Nals, picture.Keyframe, picture.HasSps,
                                        picture.RtpTimestamp, batch, ct).ConfigureAwait(false);
                                    playout.MarkSent(picture);
                                }
                                else if (item.Audio is { } aac && Audio != null)
                                {
                                    if (hub.Audio is not { IsAac: true } actual || actual.SampleRate != aacRate)
                                        throw new IOException("GOP AAC source format changed; reconnect for current SDP");
                                    await SendAsync(Audio, Audio.Packetizer.PacketizeAac(aac.Packet.Au, aac.Packet.RtpTs), ct).ConfigureAwait(false);
                                    aacTimeline!.MarkSent();
                                }
                                if (aacTimeline is { HaveAnchor: true } && Audio != null)
                                {
                                    var clock = _conn.Server.PlayoutTimeProvider;
                                    long now = clock.GetTimestamp();
                                    if (lastRtcpAt == null || clock.GetElapsedTime(lastRtcpAt.Value, now) >= TimeSpan.FromSeconds(1))
                                    {
                                        double elapsed = clock.GetElapsedTime(playout.PresentationTimestamp(TimeSpan.Zero), now).TotalSeconds;
                                        uint videoTs = unchecked(complete.Pictures[0].RtpTimestamp + (uint)(long)Math.Round(elapsed * 90000));
                                        uint audioTs = aacTimeline.RtpAtSourceTime(complete.SourceArrivalTimestamp / (double)complete.SourceArrivalFrequency + elapsed);
                                        DateTimeOffset utc = clock.GetUtcNow();
                                        await SendRtcpAsync(Video!, RtcpSenderReport.Build(Video!.Packetizer, videoTs, utc, cname), ct).ConfigureAwait(false);
                                        await SendRtcpAsync(Audio, RtcpSenderReport.Build(Audio.Packetizer, audioTs, utc, cname), ct).ConfigureAwait(false);
                                        lastRtcpAt = now;
                                    }
                                }
                            }
                            if (aacTimeline != null) _conn.Server.ObserveGopAac(Id, aacTimeline.Diagnostics);
                            continue;
                        }
                        await SendVideoAsync(hub, v.AnnexB, v.Keyframe, v.HasSps, v.RtpTs, batch, ct).ConfigureAwait(false);
                        if (lowSenderClock != null)
                        {
                            lowSenderClock.ObserveRawVideoSent(v);
                            await SendLowQualityReportsAsync(lowSenderClock, ct).ConfigureAwait(false);
                        }
                        break;
                    }
                    // The URL decided this session's audio: on an Opus session the
                    // transcoded packets ARE the track and the originals are
                    // skipped; on an original session it's the exact opposite. Both
                    // kinds flow through the hub side by side.
                    case HubAudioOpus o when Audio != null && _opus:
                        await SendAsync(Audio, Audio.Packetizer.PacketizeOpus(o.Packet, o.RtpTs), ct).ConfigureAwait(false);
                        break;
                    case HubAudioAac a when Audio != null && !_opus:
                        if (!RtspConnection.SameAudio(_description.Audio, hub.Audio))
                            throw new IOException("RTSP audio source format changed; reconnect for current SDP");
                        if (aacTimeline != null)
                        {
                            if (hub.Audio is not { IsAac: true } actual || actual.SampleRate != aacRate
                                || aacAsc == null || actual.AudioSpecificConfig?.SequenceEqual(aacAsc) != true)
                                throw new IOException("GOP AAC source format changed; reconnect for current SDP");
                            aacTimeline.Push(a); break;
                        }
                        await SendAsync(Audio, Audio.Packetizer.PacketizeAac(a.Au, a.RtpTs), ct).ConfigureAwait(false);
                        if (lowSenderClock != null)
                        {
                            lowSenderClock.ObserveRawAacSent(a, _description.Audio);
                            await SendLowQualityReportsAsync(lowSenderClock, ct).ConfigureAwait(false);
                        }
                        break;
                    case HubAudioPcm p when Audio != null && !_opus:
                        if (!RtspConnection.SameAudio(_description.Audio, hub.Audio))
                            throw new IOException("RTSP audio source format changed; reconnect for current SDP");
                        if (aacTimeline != null) throw new IOException("GOP AAC source changed to PCM; reconnect for current SDP");
                        foreach (var rtp in Audio.Packetizer.PacketizePcm(p.Pcm, p.RtpTs))
                            await SendAsync(Audio, rtp, ct).ConfigureAwait(false);
                        break;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.Debug($"{hub.Name}: session {Id} pump ended: {Log.Flatten(ex)}");
            _conn.Close(); // force a new SDP negotiation after a fatal media error
        }
        finally
        {
            _mount.PublishHqTiming?.Release(Id);
            if (matchedLow != null) _conn.Server.ObserveMatchedLow(Id, matchedLow.Diagnostics(), completed: true);
            if (lowSenderClock != null) _conn.Server.ObserveLowSenderReports(Id, lowSenderClock.Diagnostics(), completed: true);
            if (aacTimeline != null)
            {
                var diagnostic = aacTimeline.Diagnostics;
                _conn.Server.ObserveGopAac(Id, diagnostic, completed: true);
                Log.Info($"{hub.Name}: GOP AAC summary: sent={diagnostic.SentPackets}, dropped={diagnostic.DroppedPackets}, resets={diagnostic.Resets}, " +
                    $"initialEstimatedOffsetMs={diagnostic.InitialAudioOffsetMs:F1}, maxArrivalEstimateDeviationMs={diagnostic.MaxArrivalEstimateDeviationMs:F1}; capture PTS unavailable");
            }
            if (_opus) hub.ReleaseOpus();
            watch.Dispose();
            Log.Info($"{hub.Name}: client stopped streaming (session {Id})");
        }
    }

    private async Task PumpMatchedLowAsync(IStreamHub hub, System.Threading.Channels.ChannelReader<HubPacket> reader,
        MatchedLowQualityPlayout playout, LowQualityRtpSenderClock? senderClock, RtpBatch batch, CancellationToken ct)
    {
        if (_opus) throw new IOException("Matched low-quality requires original AAC");
        await foreach (var packet in playout.ReadAsync(reader, Audio != null ? _description.Audio : null,
            ct, videoEnabled: Video != null).ConfigureAwait(false))
        {
            switch (packet)
            {
                case HubVideo video when Video != null:
                    await SendVideoAsync(hub, video.AnnexB, video.Keyframe, video.HasSps,
                        video.RtpTs, batch, ct).ConfigureAwait(false);
                    break;
                case HubAudioAac audio when Audio != null:
                    if (!RtspConnection.SameAudio(_description.Audio, hub.Audio))
                        throw new IOException("RTSP audio source format changed; reconnect for current SDP");
                    await SendAsync(Audio, Audio.Packetizer.PacketizeAac(audio.Au, audio.RtpTs), ct).ConfigureAwait(false);
                    break;
                default:
                    throw new IOException("Matched low-quality yielded an unrequested track");
            }
            var sent = playout.MarkSent(packet);
            if (senderClock != null)
            {
                senderClock.ObserveMatchedSent(packet, sent);
                await SendLowQualityReportsAsync(senderClock, ct).ConfigureAwait(false);
            }
            _conn.Server.ObserveMatchedLow(Id, playout.Diagnostics());
        }
    }

    private async Task SendLowQualityReportsAsync(LowQualityRtpSenderClock clock, CancellationToken ct)
    {
        if (!clock.ReportDue || !clock.TryGetReport(Video != null, Audio != null, out var report)) return;
        string cname = "reolink-lq-" + Id.ToLowerInvariant();
        if (Video != null && report.VideoRtpTimestamp is { } video)
            await SendRtcpAsync(Video, RtcpSenderReport.Build(Video.Packetizer, video, report.UtcTimestamp, cname), ct).ConfigureAwait(false);
        if (Audio != null && report.AudioRtpTimestamp is { } audio)
            await SendRtcpAsync(Audio, RtcpSenderReport.Build(Audio.Packetizer, audio, report.UtcTimestamp, cname), ct).ConfigureAwait(false);
        clock.MarkReportSent();
        _conn.Server.ObserveLowSenderReports(Id, clock.Diagnostics());
    }

    private async Task SendVideoAsync(IStreamHub hub, byte[] annexB, bool keyframe, bool hasSps,
        uint timestamp, RtpBatch batch, CancellationToken ct)
    {
        var video = Video ?? throw new InvalidOperationException("Video transport missing");
        var codec = _description.Codec ?? VideoCodec.H264;
        if (hub.Codec != codec) throw new IOException("RTSP source codec changed; reconnect for current SDP");
        var au = keyframe ? EnsureParameterSets(_description, codec, annexB, hasSps) : annexB;
        // Packetization follows the cancellable wait, into this pump's private batch.
        video.Packetizer.PacketizeVideoInto(batch, codec, au, timestamp, video.RtpChannel);
        await SendVideoBatchAsync(video, batch, ct).ConfigureAwait(false);
    }

    private async Task SendVideoNalsAsync(IStreamHub hub, IReadOnlyList<ReadOnlyMemory<byte>> nals,
        bool keyframe, bool hasSps, uint timestamp, RtpBatch batch, CancellationToken ct)
    {
        // Preserve the original parameter-set insertion and Annex-B boundary
        // rules for keyframes missing SPS, including malformed cached metadata.
        if (keyframe && !hasSps)
        {
            await SendVideoAsync(hub, RtpAccessUnitSplitter.AnnexB(nals), true, false,
                timestamp, batch, ct).ConfigureAwait(false);
            return;
        }
        var video = Video ?? throw new InvalidOperationException("Video transport missing");
        var codec = _description.Codec ?? VideoCodec.H264;
        if (hub.Codec != codec) throw new IOException("RTSP source codec changed; reconnect for current SDP");
        video.Packetizer.PacketizeVideoNalsInto(batch, codec, nals, timestamp, video.RtpChannel);
        await SendVideoBatchAsync(video, batch, ct).ConfigureAwait(false);
    }

    private async Task SendVideoBatchAsync(TrackTransport video, RtpBatch batch, CancellationToken ct)
    {
        if (video.Tcp)
        {
            await _conn.SendInterleavedRawAsync(batch.Framed, ct).ConfigureAwait(false);
            for (int i = 0; i < batch.Count; i++) video.Packetizer.RecordSent(batch.PacketAt(i).Span);
            Touch();
        }
        else if (video.UdpSocket != null && video.ClientEndpoint != null)
            for (int i = 0; i < batch.Count; i++)
            {
                await video.UdpSocket.SendToAsync(batch.PacketAt(i), SocketFlags.None,
                    video.ClientEndpoint, ct).ConfigureAwait(false);
                video.Packetizer.RecordSent(batch.PacketAt(i).Span);
                Touch();
            }
    }

    /// <summary>Prepends cached SPS/PPS(/VPS) to keyframes that lack them (players
    /// need them to decode). <paramref name="hasSps"/> comes from the hub's own
    /// keyframe scan, so no second scan of the buffer happens per viewer.</summary>
    private static byte[] EnsureParameterSets(StreamDescriptionSnapshot hub, VideoCodec codec, byte[] annexB, bool hasSps)
    {
        if (hasSps) return annexB;

        using var ms = new MemoryStream();
        static void Write(byte[]? nal, MemoryStream stream)
        {
            if (nal == null) return;
            stream.Write(new byte[] { 0, 0, 0, 1 });
            stream.Write(nal);
        }
        if (codec == VideoCodec.H265) Write(hub.Vps, ms);
        Write(hub.Sps, ms);
        Write(hub.Pps, ms);
        ms.Write(annexB);
        return ms.ToArray();
    }

    private async Task SendAsync(TrackTransport track, byte[] rtp, CancellationToken ct)
    {
        if (track.Tcp)
        {
            await _conn.SendInterleavedAsync(track.RtpChannel, rtp, ct).ConfigureAwait(false);
        }
        else if (track.UdpSocket != null && track.ClientEndpoint != null)
        {
            await track.UdpSocket.SendToAsync(rtp, SocketFlags.None, track.ClientEndpoint, ct).ConfigureAwait(false);
        }
        track.Packetizer.RecordSent(rtp);
        Touch();
    }

    private async Task SendRtcpAsync(TrackTransport track, byte[] rtcp, CancellationToken ct)
    {
        if (track.Tcp) await _conn.SendInterleavedAsync((byte)(track.RtpChannel + 1), rtcp, ct).ConfigureAwait(false);
        else if (track.UdpRtcpSocket != null && track.ClientEndpoint is { Port: < 65535 } peer)
            await track.UdpRtcpSocket.SendToAsync(rtcp, SocketFlags.None, new IPEndPoint(peer.Address, peer.Port + 1), ct).ConfigureAwait(false);
    }
}
