// Reolink Bridge ONVIF transport and authentication.
// HTTP transport/auth derived from Neolink.NET OnvifPtzServer (Oluwabori Olaleye, 2026).
// Licensed under AGPL-3.0; see LICENSE.
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using Neolink.Config;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed record ProtectOnvifStream(string Token, string Path, IStreamHub Hub);

/// <summary>Read-only ONVIF device/media endpoint for one independently addressable camera.
/// Camera controls, encoder writes, time writes and extra camera logins are deliberately absent.</summary>
public sealed partial class ProtectOnvifServer
{
    public const string NsSoap = "http://www.w3.org/2003/05/soap-envelope";
    public const string NsDevice = "http://www.onvif.org/ver10/device/wsdl";
    public const string NsMedia = "http://www.onvif.org/ver10/media/wsdl";
    public const string NsMedia2 = "http://www.onvif.org/ver20/media/wsdl";
    public const string NsSchema = "http://www.onvif.org/ver10/schema";
    private const int MaxHeader = 16 * 1024, MaxBody = 256 * 1024;
    private readonly BridgeOnvifConfig _config;
    private readonly IReadOnlyDictionary<string, string> _users;
    private readonly IReadOnlyList<ProtectOnvifStream> _streams;
    private readonly int _rtspPort;
    private readonly long _startedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
    private readonly Dictionary<string, DateTime> _nonces = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _connections = new(StringComparer.Ordinal);

    public ProtectOnvifServer(BridgeOnvifConfig config, IReadOnlyDictionary<string, string> users,
        IReadOnlyList<ProtectOnvifStream> streams, int rtspPort)
    {
        config.Validate();
        if (rtspPort is < 1 or > 65535 || config.Port == rtspPort)
            throw new ArgumentException("ONVIF and RTSP need separate valid ports");
        if (streams.Count != config.Profiles.Count || streams.Select(s => s.Token).Distinct().Count() != streams.Count)
            throw new ArgumentException("Each ONVIF profile needs a distinct stream token");
        if (streams.Any(s => !s.Path.StartsWith('/') || s.Path.Contains('?') || s.Path.Contains('#')))
            throw new ArgumentException("RTSP paths must be absolute mount paths");
        _config = config;
        _users = users;
        _streams = streams;
        _rtspPort = rtspPort;
    }

    public bool Healthy => _streams.All(s => s.Hub.VideoReady && s.Hub.LiveVideo && Matches(s)
        && s.Hub.GetVideoDiagnostics().LastVideoAgeMs is >= 0 and <= 5000);

    private string MetricsJson() => JsonSerializer.Serialize(new
    {
        status = Healthy ? "ready" : "waiting-for-camera",
        uptimeSeconds = System.Diagnostics.Stopwatch.GetElapsedTime(_startedTimestamp).TotalSeconds,
        streams = _streams.Select(stream =>
        {
            var diagnostics = stream.Hub.GetVideoDiagnostics();
            return new
            {
                profile = stream.Token,
                codec = stream.Hub.Codec?.ToString(),
                width = stream.Hub.Width,
                height = stream.Hub.Height,
                incomingFrames = diagnostics.IncomingFrames,
                incomingVideoBytes = diagnostics.IncomingVideoBytes,
                lastVideoAgeMs = diagnostics.LastVideoAgeMs,
                maxArrivalGapMs = diagnostics.MaxArrivalGapMs,
                sourceUptimeSeconds = diagnostics.UptimeSeconds,
                videoReady = stream.Hub.VideoReady,
                liveVideo = stream.Hub.LiveVideo,
                viewers = stream.Hub.ViewerCount
            };
        }).ToArray()
    });

    private bool AuthenticatedBasic(string? authorization)
    {
        var basic = NetUtil.DecodeBasicAuth(authorization);
        return basic is { } credentials && _users.TryGetValue(credentials.User, out var expected)
            && NetUtil.FixedTimeEquals(expected, credentials.Pass);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Parse(_config.Bind), _config.Port);
        listener.Start();
        Log.Info($"ONVIF camera {_config.Name} at {_config.DeviceUrl}; identity {_config.EndpointReference}");
        using var stop = ct.Register(listener.Stop);
        using var slots = new SemaphoreSlim(16, 16);
        var clients = new List<Task>();
        var discovery = _config.Discovery ? RunDiscoveryAsync(ct) : Task.CompletedTask;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try { client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false); }
                catch (Exception) when (ct.IsCancellationRequested) { break; }
                var remote = (client.Client.RemoteEndPoint as IPEndPoint)?.Address.ToString() ?? "?";
                bool accepted;
                lock (_connections)
                {
                    var n = _connections.GetValueOrDefault(remote);
                    accepted = n < 8 && slots.Wait(0);
                    if (accepted) _connections[remote] = n + 1;
                }
                if (!accepted) { client.Dispose(); continue; }
                clients.RemoveAll(t => t.IsCompleted);
                clients.Add(Task.Run(async () =>
                {
                    try { await ServeAsync(client, ct).ConfigureAwait(false); }
                    catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or ObjectDisposedException) { }
                    catch { Log.Warn("ONVIF request failed"); } // never log request bodies, auth or secrets
                    finally
                    {
                        client.Dispose();
                        lock (_connections)
                        {
                            if (--_connections[remote] == 0) _connections.Remove(remote);
                        }
                        slots.Release();
                    }
                }, CancellationToken.None));
            }
        }
        finally
        {
            listener.Stop();
            await Task.WhenAll(clients).ConfigureAwait(false);
            try { await discovery.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
    }

    private async Task ServeAsync(TcpClient client, CancellationToken ct)
    {
        client.NoDelay = true;
        var stream = client.GetStream();
        var buffer = new byte[MaxHeader];
        int filled = 0;
        for (bool first = true; !ct.IsCancellationRequested; first = false)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(first ? 10 : 60));
            int end;
            while ((end = buffer.AsSpan(0, filled).IndexOf("\r\n\r\n"u8)) < 0)
            {
                if (filled == buffer.Length) { await RespondAsync(stream, 431, "", true, ct); return; }
                int n = await stream.ReadAsync(buffer.AsMemory(filled), timeout.Token).ConfigureAwait(false);
                if (n == 0) return;
                filled += n;
            }
            var lines = Encoding.ASCII.GetString(buffer, 0, end).Split("\r\n");
            var request = lines[0].Split(' ');
            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in lines.Skip(1))
            {
                int colon = line.IndexOf(':');
                if (colon <= 0 || !headers.TryAdd(line[..colon].Trim(), line[(colon + 1)..].Trim()))
                { await RespondAsync(stream, 400, "", true, ct); return; }
            }
            if (request.Length != 3 || !request[2].StartsWith("HTTP/1."))
            { await RespondAsync(stream, 400, "", true, ct); return; }
            if (request[0] == "GET")
            {
                if (request[1] == "/metrics")
                {
                    bool authorized = AuthenticatedBasic(headers.GetValueOrDefault("Authorization"));
                    await RespondAsync(stream, authorized ? 200 : 401,
                        authorized ? MetricsJson() : "{\"error\":\"authentication-required\"}", true, ct, "application/json").ConfigureAwait(false);
                    return;
                }
                bool health = request[1] == "/health";
                bool ready = Healthy;
                int status = health ? ready ? 200 : 503 : 404;
                string body = health ? ready ? "{\"status\":\"ready\"}" : "{\"status\":\"waiting-for-camera\"}" : "";
                await RespondAsync(stream, status, body, true, ct, "application/json");
                return;
            }
            if (request[0] != "POST") { await RespondAsync(stream, 405, "", true, ct); return; }
            if (request[1] is not ("/onvif/device_service" or "/onvif/media_service" or "/onvif/media2_service"))
            { await RespondAsync(stream, 404, "", true, ct); return; }
            if (headers.ContainsKey("Transfer-Encoding") || !int.TryParse(headers.GetValueOrDefault("Content-Length"),
                    NumberStyles.None, CultureInfo.InvariantCulture, out var length))
            { await RespondAsync(stream, 411, "", true, ct); return; }
            if (length > MaxBody) { await RespondAsync(stream, 413, "", true, ct); return; }
            if (headers.GetValueOrDefault("Expect")?.Equals("100-continue", StringComparison.OrdinalIgnoreCase) == true)
                await stream.WriteAsync("HTTP/1.1 100 Continue\r\n\r\n"u8.ToArray(), ct).ConfigureAwait(false);
            int consumed = end + 4;
            var bodyBytes = new byte[length];
            int have = Math.Min(length, filled - consumed);
            Buffer.BlockCopy(buffer, consumed, bodyBytes, 0, have);
            int rest = filled - consumed - have;
            Buffer.BlockCopy(buffer, consumed + have, buffer, 0, rest);
            filled = rest;
            while (have < length)
            {
                int n = await stream.ReadAsync(bodyBytes.AsMemory(have), timeout.Token).ConfigureAwait(false);
                if (n == 0) return;
                have += n;
            }
            bool close = request[2] != "HTTP/1.1" || headers.GetValueOrDefault("Connection")?.Equals("close", StringComparison.OrdinalIgnoreCase) == true;
            using var work = CancellationTokenSource.CreateLinkedTokenSource(ct);
            work.CancelAfter(TimeSpan.FromSeconds(15));
            var reply = await HandleAsync(Encoding.UTF8.GetString(bodyBytes), headers.GetValueOrDefault("Authorization"), work.Token).ConfigureAwait(false);
            await RespondAsync(stream, reply.Status, reply.Body, close, ct).ConfigureAwait(false);
            if (close) return;
        }
    }

    private static async Task RespondAsync(NetworkStream stream, int status, string body, bool close,
        CancellationToken ct, string contentType = "application/soap+xml; charset=utf-8")
    {
        var data = Encoding.UTF8.GetBytes(body);
        var head = $"HTTP/1.1 {status} {status switch { 200 => "OK", 400 => "Bad Request", 401 => "Unauthorized", 404 => "Not Found", 503 => "Service Unavailable", _ => "Error" }}\r\n" +
            $"Content-Length: {data.Length}\r\nContent-Type: {contentType}\r\n" +
            "Cache-Control: no-store\r\n" +
            (status == 401 ? "WWW-Authenticate: Basic realm=\"reolink-bridge\"\r\n" : "") +
            (close ? "Connection: close\r\n" : "") + "\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(head), ct).ConfigureAwait(false);
        if (data.Length > 0) await stream.WriteAsync(data, ct).ConfigureAwait(false);
    }

    public async Task<(int Status, string Body)> HandleAsync(string soap, string? authorization, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        XElement envelope;
        try { envelope = ParseXml(soap); }
        catch (XmlException) { return Fault(400, "ter:WellFormed", "Malformed SOAP XML"); }
        var body = envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Body");
        var operations = body?.Elements().Take(2).ToArray();
        var op = operations?.FirstOrDefault();
        if (envelope.Name.LocalName != "Envelope" || body == null || op == null || operations!.Length != 1)
            return Fault(400, "ter:WellFormed", "A SOAP envelope and single operation are required");
        string service = op.Name.NamespaceName, action = op.Name.LocalName;
        if (service == NsDevice)
            switch (action)
            {
                case "GetSystemDateAndTime": return Ok(SystemDateAndTime());
                case "GetServices": return Ok(Services());
                case "GetCapabilities": return Ok(Capabilities());
                case "GetServiceCapabilities": return Ok(DeviceServiceCapabilities());
            }
        if (!Authenticated(envelope, authorization)) return Fault(401, "ter:NotAuthorized", "Authentication required");
        if (service == NsDevice)
            return action switch
            {
                "GetDeviceInformation" => Ok(DeviceInformation()),
                "GetNetworkInterfaces" => Ok(NetworkInterfaces()),
                "GetEndpointReference" => Ok($"<tds:GetEndpointReferenceResponse><tds:GUID>{Esc(_config.EndpointReference)}</tds:GUID></tds:GetEndpointReferenceResponse>"),
                "GetScopes" => Ok(Scopes()),
                "GetHostname" => Ok($"<tds:GetHostnameResponse><tds:HostnameInformation><tt:FromDHCP>false</tt:FromDHCP><tt:Name>{Esc(_config.Name)}</tt:Name></tds:HostnameInformation></tds:GetHostnameResponse>"),
                "GetDNS" => Ok("<tds:GetDNSResponse><tds:DNSInformation><tt:FromDHCP>false</tt:FromDHCP></tds:DNSInformation></tds:GetDNSResponse>"),
                "GetNetworkProtocols" => Ok($"<tds:GetNetworkProtocolsResponse><tds:NetworkProtocols><tt:Name>HTTP</tt:Name><tt:Enabled>true</tt:Enabled><tt:Port>{_config.Port}</tt:Port></tds:NetworkProtocols><tds:NetworkProtocols><tt:Name>RTSP</tt:Name><tt:Enabled>true</tt:Enabled><tt:Port>{_rtspPort}</tt:Port></tds:NetworkProtocols></tds:GetNetworkProtocolsResponse>"),
                _ => Fault(400, "ter:ActionNotSupported", "This read-only device does not offer that operation")
            };
        if (service != NsMedia && service != NsMedia2)
            return Fault(400, "ter:ActionNotSupported", "Unsupported service");
        bool media2 = service == NsMedia2;
        string prefix = media2 ? "tr2" : "trt";
        if (action == "GetServiceCapabilities") return Ok(MediaCapabilities(prefix));
        // Metadata comes from the stream already carried by RTSP, never another camera connection.
        if (_streams.Any(s => !Matches(s)))
            return Fault(503, "ter:Action", "Camera encoding differs from the configured profile; correct the bridge configuration");
        if (action is "GetProfiles" or "GetVideoSources" or "GetVideoEncoderConfigurations")
            return Ok(action switch
            {
                "GetProfiles" => $"<{prefix}:GetProfilesResponse>{string.Concat(_streams.Select(s => Profile(prefix + ":Profiles", s, media2)))}</{prefix}:GetProfilesResponse>",
                "GetVideoSources" => $"<{prefix}:GetVideoSourcesResponse>{VideoSources(prefix)}</{prefix}:GetVideoSourcesResponse>",
                _ => $"<{prefix}:GetVideoEncoderConfigurationsResponse>{string.Concat(_streams.Select(s => Encoder(prefix + ":Configurations", s, media2)))}</{prefix}:GetVideoEncoderConfigurationsResponse>"
            });
        string? token = Child(op, "ProfileToken") ?? Child(op, "ConfigurationToken");
        var selected = _streams.FirstOrDefault(s => s.Token == token || "encoder_" + s.Token == token);
        if (selected == null) return Fault(400, "ter:InvalidArgVal", "Unknown profile or configuration token");
        switch (action)
        {
            case "GetProfile": return Ok($"<{prefix}:GetProfileResponse>{Profile(prefix + ":Profile", selected, media2)}</{prefix}:GetProfileResponse>");
            case "GetStreamUri":
                if (Child(op, "Protocol") is { } protocol && protocol is not ("RTSP" or "TCP" or "UDP"))
                    return Fault(400, "ter:InvalidArgVal", "Unsupported transport protocol");
                var uri = $"rtsp://{_config.AdvertisedHost}:{_rtspPort}{selected.Path}";
                return Ok(media2 ? $"<tr2:GetStreamUriResponse><tr2:Uri>{Esc(uri)}</tr2:Uri></tr2:GetStreamUriResponse>"
                    : $"<trt:GetStreamUriResponse><trt:MediaUri><tt:Uri>{Esc(uri)}</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect><tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT60S</tt:Timeout></trt:MediaUri></trt:GetStreamUriResponse>");
            case "GetVideoEncoderConfiguration":
                return Ok($"<{prefix}:GetVideoEncoderConfigurationResponse>{Encoder(prefix + ":Configuration", selected, media2)}</{prefix}:GetVideoEncoderConfigurationResponse>");
            default: return Fault(400, "ter:ActionNotSupported", "This read-only media service does not offer that operation");
        }
    }

    private bool Matches(ProtectOnvifStream stream)
    {
        var profile = _config.Profiles[Index(stream)];
        return stream.Hub.Codec == null || stream.Hub.Codec.Value.ToString() == profile.Codec;
    }

    private bool Authenticated(XElement envelope, string? authorization)
    {
        if (_users.Count == 0) return true;
        if (authorization != null)
        {
            var basic = NetUtil.DecodeBasicAuth(authorization);
            if (basic is not { } creds) return false;
            return _users.TryGetValue(creds.User, out var password) && NetUtil.FixedTimeEquals(password, creds.Pass);
        }
        var token = envelope.Elements().FirstOrDefault(e => e.Name.LocalName == "Header")?
            .Descendants().FirstOrDefault(e => e.Name.LocalName == "UsernameToken");
        if (token == null) return false;
        var user = Child(token, "Username") ?? "";
        var passwordElement = token.Elements().FirstOrDefault(e => e.Name.LocalName == "Password");
        if (passwordElement == null || !_users.TryGetValue(user, out var expected)) return false;
        if (passwordElement.Attribute("Type")?.Value.EndsWith("#PasswordDigest", StringComparison.Ordinal) != true)
            return NetUtil.FixedTimeEquals(expected, passwordElement.Value);
        var nonce = Child(token, "Nonce");
        var created = Child(token, "Created");
        if (nonce == null || created == null || nonce.Length > 1024 || !DateTime.TryParse(created, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var at)) return false;
        var now = DateTime.UtcNow;
        if ((now - at).Duration() > TimeSpan.FromMinutes(5)) return false;
        byte[] bytes;
        try { bytes = Convert.FromBase64String(nonce); } catch (FormatException) { return false; }
        if (!NetUtil.FixedTimeEquals(Protocol.OnvifClient.PasswordDigest(bytes, created, expected), passwordElement.Value.Trim())) return false;
        lock (_nonces)
        {
            string key = nonce + "|" + created;
            if (_nonces.ContainsKey(key)) return false;
            foreach (var old in _nonces.Where(n => now - n.Value > TimeSpan.FromMinutes(10)).Select(n => n.Key).ToArray()) _nonces.Remove(old);
            if (_nonces.Count >= 4096) return false;
            _nonces[key] = now;
        }
        return true;
    }

    private static string? Child(XElement parent, string name) => parent.Elements().FirstOrDefault(e => e.Name.LocalName == name)?.Value.Trim();
    private static XElement ParseXml(string text)
    {
        using var reader = XmlReader.Create(new StringReader(text), new XmlReaderSettings
        { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null, MaxCharactersInDocument = MaxBody });
        return XElement.Load(reader);
    }
}
