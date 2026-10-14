// Separate authenticated JSON camera controls, preserving the ONVIF encoder contract. AGPL-3.0.
using System.Text.Json;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    public async Task<(int Status, string Body)> HandleControlAsync(string method, string path, string body,
        IReadOnlyDictionary<string, string> headers, CancellationToken ct)
    {
        static (int, string) Error(int status, string code) => (status, JsonSerializer.Serialize(new { error = code }));
        // Authentication is checked before parsing, capability reads, or any camera command.
        if (!AuthenticatedBasic(headers.GetValueOrDefault("Authorization"))) return Error(401, "authentication-required");
        if (method == "POST" && !ControlUserAllowed(headers.GetValueOrDefault("Authorization")))
            return Error(403, "camera-control-permission-required");
        if (!_config.Controls || _controls == null) return Error(404, "controls-disabled");
        if (method is not ("GET" or "POST")) return Error(405, "method-not-allowed");
        if (method == "POST" && (headers.GetValueOrDefault("X-Reolink-Control-Intent") != "user"
            || headers.ContainsKey("Origin"))) return Error(403, "explicit-control-intent-required");
        if (method == "POST" && headers.GetValueOrDefault("Content-Type")?.Split(';')[0].Trim() != "application/json")
            return Error(415, "json-content-type-required");
        try
        {
            var (resource, type) = ParseControlPath(path);
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(TimeSpan.FromSeconds(60));
            object result;
            if (method == "GET") result = await _controls.ReadAsync(resource, type, deadline.Token,
                capabilitiesOnly: path == "/control/capabilities").ConfigureAwait(false);
            else
            {
                if (resource == null || type != null) return Error(400, "invalid-write-resource");
                result = await _controls.WriteAsync(resource, body, deadline.Token).ConfigureAwait(false);
            }
            return (200, JsonSerializer.Serialize(result, ProtectCameraControls.JsonOptions));
        }
        catch (Exception ex) when (ex is ArgumentException or JsonException or InvalidOperationException or FormatException)
        { return Error(400, "invalid-control-request"); }
        catch (NotSupportedException) { return Error(422, "control-not-supported-by-camera"); }
        catch (ProtectControlBusyException) { return Error(429, "camera-control-busy"); }
        catch (CameraOfflineException) { return Error(503, "camera-offline"); }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return Error(504, "camera-control-timeout"); }
        catch (OperationCanceledException) { throw; }
        catch { return Error(502, "camera-control-failed"); } // Exception text may contain access credentials.
    }

    internal static (string? Resource, string? Type) ParseControlPath(string path)
    {
        string[] parts = path.Split('?', 2);
        if (!parts[0].StartsWith("/control/", StringComparison.Ordinal)) throw new ArgumentException();
        string resource = parts[0][9..];
        if (resource is "state" or "capabilities")
        { if (parts.Length != 1) throw new ArgumentException(); return (null, null); }
        if (!ProtectCameraControls.ResourceIds.Contains(resource)) throw new ArgumentException();
        string? type = null;
        if (parts.Length == 2)
        {
            if (resource is not ("ai" or "zone" or "ai-diagnostic") || !parts[1].StartsWith("type=", StringComparison.Ordinal)
                || parts[1].Contains('&')) throw new ArgumentException();
            type = parts[1][5..];
            if (type is not ("md" or "people" or "vehicle" or "dog_cat" or "face" or "package")) throw new ArgumentException();
            if (resource == "ai" && type == "md") throw new ArgumentException();
        }
        return (resource, type);
    }
}
