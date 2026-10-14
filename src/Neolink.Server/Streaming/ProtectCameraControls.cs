// Opt-in, capability-driven Protect control adapter. AGPL-3.0.
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Neolink.Onvif;

namespace Neolink.Streaming;

public sealed record ProtectControlOption(object Value, string Label);
public sealed record ProtectControlField(string Id, string Label, string Type,
    double? Min = null, double? Max = null, double? Step = null,
    IReadOnlyList<ProtectControlOption>? Options = null, bool ReadOnly = false);
public sealed record ProtectControlResource(string Id, string Label, string Group,
    bool Supported, bool Writable, bool IsActuator, IReadOnlyList<ProtectControlField> Fields,
    string? Reason = null);
internal sealed record ProtectControlReading(ProtectControlResource Descriptor, Dictionary<string, object?> Values,
    string? ReadError = null);

/// <summary>Reuses camera controls without starting the full Neolink web application.
/// No raw support XML, credentials, network/firmware writes, or invented capabilities are exposed.</summary>
public sealed class ProtectCameraControls(string name, ICameraControl control,
    Func<bool?>? sirenState = null, bool talkEnabled = false, Func<long>? sourceEpoch = null,
    CancellationToken lifetime = default)
{
    internal static readonly string[] ResourceIds = ["led", "whiteled", "floodlight", "siren", "privacy",
        "image", "volume", "audio", "ptz", "zoomfocus", "preset", "autotrack", "motion", "ai", "zone", "osd", "services", "ai-diagnostic"];
    private readonly SemaphoreSlim _requests = new(1, 1);
    private long _nextZoneDiscoveryAt = long.MinValue;
    private readonly OnvifPtzCamera _restPtz = new(name, control, null);
    private readonly object _capabilityGate = new();
    private Task<CameraCapabilities>? _capabilityRead;
    private readonly CancellationTokenSource _capabilityStop = new();
    private long _capabilityReadEpoch, _knownEpoch, _knownAt;
    private readonly Dictionary<string, ProtectControlReading> _readings = new(StringComparer.Ordinal);
    private long _readingsEpoch = long.MinValue;
    private long _readFailures;
    private string? _lastReadError;
    private Task? _shutdown;
    private CameraCapabilities? _knownCapabilities;
    internal ICameraControl Control => control;
    internal OnvifPtzCamera PtzCamera => _restPtz;
    internal CameraCapabilities? KnownCapabilities
    { get { lock (_capabilityGate) return _knownEpoch == Epoch ? _knownCapabilities : null; } }
    private long Epoch => sourceEpoch?.Invoke() ?? 0;
    internal bool IsStopping { get { lock (_capabilityGate) return _shutdown != null; } }
    public Func<object>? NativeTalkDiagnostics { get; set; }
    public Func<object>? ListenMetadata { get; set; }
    internal async Task<CameraCapabilities> CapabilitiesAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Task<CameraCapabilities> pending;
        lock (_capabilityGate)
        {
            long epoch = Epoch;
            if (sourceEpoch != null && _knownCapabilities != null && _knownEpoch == epoch
                && Environment.TickCount64 - _knownAt < 5000) return _knownCapabilities;
            if (_capabilityRead is not { IsCompleted: false } || _capabilityReadEpoch != epoch)
            {
                _capabilityReadEpoch = epoch;
                _capabilityRead = ReadCapabilitiesAsync(epoch);
            }
            pending = _capabilityRead;
        }
        // A cancelled HTTP caller never cancels discovery shared by another caller.
        return await pending.WaitAsync(ct).ConfigureAwait(false);
    }
    private async Task<CameraCapabilities> ReadCapabilitiesAsync(long epoch)
    {
        await Task.Yield();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(lifetime, _capabilityStop.Token);
        budget.CancelAfter(TimeSpan.FromSeconds(15));
        var caps = await control.GetCapabilitiesAsync(budget.Token).ConfigureAwait(false);
        lock (_capabilityGate)
        {
            if (Epoch != epoch) return caps with { Provisional = true };
            if (!caps.Provisional) { _knownCapabilities = caps; _knownEpoch = epoch; _knownAt = Environment.TickCount64; }
        }
        return caps;
    }
    internal Task StopAsync()
    {
        lock (_capabilityGate)
        {
            // Shared discovery is independent of individual HTTP callers, but
            // must release CameraControl's command gate before the final stop.
            _capabilityStop.Cancel();
            return _shutdown ??= _restPtz.ShutdownAsync();
        }
    }
    internal static readonly JsonSerializerOptions JsonOptions = new()
    { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public async Task<object> ReadAsync(string? resource, string? type, CancellationToken ct, bool capabilitiesOnly = false)
    {
        if (capabilitiesOnly) return CapabilitySnapshot(await CapabilitiesAsync(ct).ConfigureAwait(false));
        if (!await _requests.WaitAsync(TimeSpan.FromSeconds(2), ct).ConfigureAwait(false))
            throw new ProtectControlBusyException();
        try
        {
            long operationEpoch = Epoch;
            var caps = await CapabilitiesAsync(ct).ConfigureAwait(false);
            lock (_capabilityGate)
                if (_readingsEpoch != Epoch) { _readings.Clear(); _readingsEpoch = Epoch; _nextZoneDiscoveryAt = long.MinValue; }
            if (resource != null)
            {
                var one = CachedReadingIfUnavailable(resource,
                    await ReadResourceAsync(resource, type, caps, null, false, ct).ConfigureAwait(false), operationEpoch, type);
                if (operationEpoch != Epoch) throw new CameraOfflineException(name);
                Remember(resource, one, operationEpoch);
                return new { schemaVersion = 1, ok = true, resource, descriptor = one.Descriptor, state = one.Values,
                    readError = one.ReadError };
            }
            // Query only resources exposed by this bridge. The upstream HTTP sweep
            // also queries Wi-Fi, SD storage and quick replies which this API never uses.
            if (!caps.Provisional) await DiscoverZoneTypesAsync(null, ct).ConfigureAwait(false);
            var readings = new List<ProtectControlReading>();
            foreach (var id in ResourceIds)
            {
                var reading = CachedReadingIfUnavailable(id,
                    await ReadResourceAsync(id, null, caps, null, false, ct).ConfigureAwait(false), operationEpoch, null);
                readings.Add(reading); Remember(id, reading, operationEpoch);
            }
            if (operationEpoch != Epoch) throw new CameraOfflineException(name);
            return new
            {
                schemaVersion = 1,
                camera = new { name, model = caps.Version?.Model, provisional = caps.Provisional,
                    talkSupported = !caps.Provisional && caps.Features.Talk, talkEnabled },
                resources = readings.Select(r => r.Descriptor).ToArray(),
                values = readings.ToDictionary(r => r.Descriptor.Id, r => r.Values),
                readErrors = readings.Where(r => r.ReadError != null).ToDictionary(r => r.Descriptor.Id, r => r.ReadError),
                nativeTalk = NativeTalkDiagnostics?.Invoke(),
                listen = ListenMetadata?.Invoke()
            };
        }
        finally { _requests.Release(); }
    }

    private void Remember(string resource, ProtectControlReading reading, long operationEpoch)
    {
        lock (_capabilityGate)
        {
            if (operationEpoch != Epoch) return;
            if (_readingsEpoch != Epoch) { _readings.Clear(); _readingsEpoch = Epoch; _nextZoneDiscoveryAt = long.MinValue; }
            _readings[resource] = reading;
        }
    }
    private ProtectControlReading CachedReadingIfUnavailable(string resource, ProtectControlReading fresh,
        long operationEpoch, string? type)
    {
        if (fresh.Descriptor.Supported) return fresh;
        lock (_capabilityGate)
        {
            if (operationEpoch != Epoch || _readingsEpoch != operationEpoch
                || !_readings.TryGetValue(resource, out var previous) || !previous.Descriptor.Supported
                || type != null && previous.Values.GetValueOrDefault("type")?.ToString() != type) return fresh;
            // Preserve the former HTTP sweep's last-known values on a hiccup,
            // with an explicit freshness warning. Writes never use this fallback.
            return previous with { Descriptor = previous.Descriptor with { Reason = "cached-state-read-unavailable" },
                ReadError = fresh.ReadError ?? "state-unavailable" };
        }
    }
    private object CapabilitySnapshot(CameraCapabilities caps)
    {
        var snapshots = new List<ProtectControlReading>();
        lock (_capabilityGate)
        {
            if (_readingsEpoch != Epoch) { _readings.Clear(); _readingsEpoch = Epoch; }
            foreach (var id in ResourceIds)
            {
                if (_readings.TryGetValue(id, out var known)) { snapshots.Add(known); continue; }
                bool supported = !caps.Provisional && (id switch
                {
                    "led" => caps.Features.Led, "whiteled" => caps.Features.WhiteLed, "floodlight" => caps.Features.Floodlight,
                    "siren" => caps.Features.Siren, "privacy" => caps.Features.Privacy,
                    "ptz" or "preset" => caps.Features.Ptz, "zoomfocus" => caps.Features.Zoom, _ => false
                });
                snapshots.Add(new(new(id, Label(id), "Kamera", supported, false,
                    id is "led" or "whiteled" or "floodlight" or "siren" or "privacy" or "ptz" or "preset" or "zoomfocus",
                    [], caps.Provisional ? "camera-not-ready" : "state-not-read"), new()));
            }
        }
        return new
        {
            schemaVersion = 1,
            camera = new { name, model = caps.Version?.Model, provisional = caps.Provisional,
                talkSupported = !caps.Provisional && caps.Features.Talk, talkEnabled },
            resources = snapshots.Select(r => r.Descriptor).ToArray(),
            values = snapshots.ToDictionary(r => r.Descriptor.Id, r => r.Values),
            readErrors = snapshots.Where(r => r.ReadError != null).ToDictionary(r => r.Descriptor.Id, r => r.ReadError),
            stateReadRequired = true, nativeTalk = NativeTalkDiagnostics?.Invoke(), listen = ListenMetadata?.Invoke()
        };
    }

    private async Task DiscoverZoneTypesAsync(HttpFeatures? features, CancellationToken ct)
    {
        long now = Environment.TickCount64;
        if (now < _nextZoneDiscoveryAt) return;
        _nextZoneDiscoveryAt = now + 30_000;
        // A CGI AI config can expose sensitivity without a grid, while cmd342
        // exposes a real native grid. Probe only the camera's known AI types so
        // the initial zone selector does not depend on a prior manual query.
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        budget.CancelAfter(TimeSpan.FromSeconds(8));
        try
        {
            var detections = features?.AiSensitivities ?? await QuietAsync(() => control.GetAiSensitivitiesAsync(budget.Token), budget.Token).ConfigureAwait(false);
            foreach (string type in detections?.Select(d => d.Type).Where(CameraControl.AiAlarmTypes.Contains).Distinct().Take(3) ?? [])
                if (!control.ZoneTypes().Contains(type))
                    await QuietAsync(() => control.GetDetectionZoneAsync(type, budget.Token), budget.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { /* a slow optional probe leaves other controls available */ }
        catch (OperationCanceledException) { _nextZoneDiscoveryAt = long.MinValue; throw; }
    }

    public async Task<object> WriteAsync(string resource, string body, CancellationToken ct)
    {
        // Parse before any camera request. Duplicate/unknown fields are never silently discarded.
        var values = ParseWrite(body);
        if (!ResourceIds.Contains(resource)) throw new ArgumentException("Unknown control resource");
        if (IsStopping) throw new CameraOfflineException(name);
        // Stop must not wait behind the duration of a previous button nudge or a slow settings read.
        if (resource == "ptz" && values["command"] is JsonValue stopValue && stopValue.TryGetValue<string>(out var command) && command == "stop")
        {
            var caps = await CapabilitiesAsync(ct).ConfigureAwait(false);
            var descriptor = await ReadResourceAsync(resource, null, caps, null, false, ct).ConfigureAwait(false);
            ValidateWrite(descriptor.Descriptor, values);
            await _restPtz.MoveAsync("stop", values["speed"] == null ? 32 : (int)values["speed"]!.GetValue<double>(), TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
            return new { ok = true, resource, state = descriptor.Values, descriptor = descriptor.Descriptor };
        }
        if (!await _requests.WaitAsync(TimeSpan.FromSeconds(2), ct).ConfigureAwait(false))
            throw new ProtectControlBusyException();
        try
        {
            long operationEpoch = Epoch;
            var caps = await CapabilitiesAsync(ct).ConfigureAwait(false);
            string? type = values["type"]?.GetValue<string>();
            var before = await ReadResourceAsync(resource, type, caps, null, false, ct).ConfigureAwait(false);
            ValidateWrite(before.Descriptor, values);
            int? Int(string key) => values[key] == null ? null : (int)values[key]!.GetValue<double>();
            bool? Bool(string key) => values[key]?.GetValue<bool>();
            string? Str(string key) => values[key]?.GetValue<string>();
            switch (resource)
            {
                case "led": await control.SetLedStateAsync(Str("state"), Str("lightState"),
                    Str("doorbellLightState"), Int("irBrightness"), ct); break;
                case "whiteled": await control.SetWhiteLedAsync(Int("brightness"), Bool("on"), Int("mode"), ct); break;
                case "floodlight":
                    var task = await control.GetFloodlightTasksAsync(ct) ?? throw new NotSupportedException();
                    if (Int("brightness") is { } brightness) task.SetElementValue("brightness_cur", brightness);
                    if (Bool("auto") is { } auto) task.SetElementValue("enable", auto ? 1 : 0);
                    await control.SetFloodlightTasksAsync(task, ct); break;
                case "siren": await control.SirenAsync(Bool("on")!.Value, ct); break;
                case "privacy": await control.SetPrivacyModeAsync(Bool("on")!.Value, ct); break;
                case "image":
                    await control.SetImageSettingsAsync(Int("bright"), Int("contrast"), Int("saturation"),
                        Int("hue"), Int("sharpen"), Str("dayNight"), Str("antiFlicker"), Bool("flip"), Bool("mirror"), ct);
                    if (Int("hdr") is { } hdr) await control.SetHdrAsync(hdr, ct); break;
                case "volume": await control.SetVolumeAsync(Int("volume")!.Value, ct); break;
                case "audio":
                    if (Bool("recordAudio") is { } rec) await control.SetRecordAudioAsync(rec, ct);
                    if (Int("talkVolume") != null || Int("visitorVolume") != null)
                        await control.SetAudioVolumesAsync(Int("talkVolume"), Int("visitorVolume"), ct); break;
                case "ptz": await _restPtz.NudgeAsync(Str("command")!, Int("speed") ?? 32,
                    TimeSpan.FromMilliseconds(Int("durationMs") ?? 500), ct); break;
                case "zoomfocus":
                    if (Int("zoom") is { } z) await _restPtz.SetLensPositionAsync("zoomPos", (uint)z, ct);
                    if (Int("focus") is { } f) await _restPtz.SetLensPositionAsync("focusPos", (uint)f, ct); break;
                case "preset": await _restPtz.GotoPresetAsync(Int("id")!.Value, ct); break;
                case "autotrack": await control.SetAutoTrackAsync(Bool("on")!.Value, ct); break;
                case "motion": await control.SetMdSensitivityAsync(Int("sensitivity")!.Value, ct); break;
                case "ai":
                    Require(values, "type");
                    var parameters = await control.GetAiAlarmParametersAsync(Str("type")!, ct);
                    if (parameters == null) { Require(values, "sensitivity"); await control.SetAiSensitivityAsync(Str("type")!, Int("sensitivity")!.Value, ct); }
                    else await control.SetAiAlarmParametersAsync(Str("type")!, values.Where(p => p.Key is not ("type" or "enabled"))
                        .ToDictionary(p => p.Key, p => p.Value!.GetValue<double>()), Bool("enabled"), ct);
                    break;
                case "zone":
                    Require(values, "type", "table");
                    var zone = await control.GetDetectionZoneAsync(Str("type")!, ct) ?? throw new NotSupportedException();
                    string table = Str("table")!;
                    if (table.Length != zone.Cols * zone.Rows || table.Any(c => c is not ('0' or '1')))
                        throw new ArgumentException("Zone grid must match the camera's current dimensions");
                    await control.SetDetectionZoneAsync(Str("type")!, table, ct); break;
                case "osd": await control.SetOsdSettingsAsync(Bool("showName"), Str("namePos"),
                    Bool("showTime"), Str("timePos"), Bool("watermark"), ct); break;
            }
            var after = await ReadResourceAsync(resource, type, caps, null, false, ct).ConfigureAwait(false);
            Remember(resource, after, operationEpoch);
            string? readError = operationEpoch == Epoch ? after.ReadError : "source-epoch-changed";
            return new { ok = true, resource, state = after.Values, descriptor = after.Descriptor,
                readbackAvailable = readError == null && after.Values.Count > 0, readError };
        }
        finally { _requests.Release(); }
    }

    private async Task<ProtectControlReading> ReadResourceAsync(string resource, string? type,
        CameraCapabilities caps, HttpFeatures? http, bool combined, CancellationToken ct)
    {
        if (!ResourceIds.Contains(resource)) throw new ArgumentException("Unknown control resource");
        long failuresBefore = Interlocked.Read(ref _readFailures);
        var fields = new List<ProtectControlField>();
        var state = new Dictionary<string, object?>();
        string group = resource is "motion" or "ai" or "zone" or "autotrack" ? "Erkennung" :
            resource is "ptz" or "zoomfocus" or "preset" ? "Bewegung" :
            resource is "audio" or "volume" or "siren" ? "Audio" : "Kamera";
        bool actuator = resource is "led" or "whiteled" or "floodlight" or "siren" or "ptz" or "zoomfocus" or "preset" or "privacy";
        void Add(string id, string label, string kind, object? value, double? min = null, double? max = null,
            IReadOnlyList<ProtectControlOption>? options = null, bool readOnly = false, double step = 1)
        {
            if (value == null && !actuator && !readOnly) return;
            fields.Add(new(id, label, kind, min, max, kind == "number" ? step : null, options, readOnly));
            state[id] = value;
        }
        if (!caps.Provisional)
        switch (resource)
        {
            case "led" when caps.Features.Led:
                var led = await QuietAsync(() => control.GetLedStateAsync(ct), ct);
                foreach (var pair in new[] { ("state", "Infrarot"), ("lightState", "Licht"), ("doorbellLightState", "Klingellicht") })
                    if (led?.Element(pair.Item1) is { } el && (pair.Item1 != "doorbellLightState" || caps.Features.Doorbell))
                    {
                        bool known = el.Value is "open" or "close" or "on" or "off" || pair.Item1 == "state" && el.Value == "auto";
                        string[] options = el.Value is "on" or "off" ? pair.Item1 == "state" ? ["auto", "on", "off"] : ["on", "off"]
                            : known ? pair.Item1 == "state" ? ["auto", "open", "close"] : ["open", "close"] : [el.Value];
                        Add(pair.Item1, pair.Item2, "select", el.Value, options: Options(options), readOnly: !known);
                    }
                if (int.TryParse(led?.Element("IRLedBrightness")?.Value, out var ir)) Add("irBrightness", "IR-Helligkeit", "number", ir, 0, 100);
                break;
            case "whiteled" when caps.Features.WhiteLed:
                var white = await QuietAsync(() => control.GetWhiteLedAsync(ct), ct);
                if (white != null)
                { Add("brightness", "Helligkeit", "number", white.Bright, 0, 100); Add("on", "Ein", "boolean", white.On);
                    Add("mode", "Modus", "select", white.Mode, options: Options(0, 1, 2, 3)); }
                break;
            case "floodlight" when caps.Features.Floodlight:
                var flood = await QuietAsync(() => control.GetFloodlightTasksAsync(ct), ct);
                if (flood != null)
                { Add("brightness", "Helligkeit", "number", (int?)flood.Element("brightness_cur"),
                    (int?)flood.Element("brightness_min") ?? 1, (int?)flood.Element("brightness_max") ?? 100);
                    if (flood.Element("enable") != null) Add("auto", "Bei Bewegung nachts", "boolean", (int?)flood.Element("enable") == 1); }
                break;
            case "siren" when caps.Features.Siren:
                Add("on", "Sirene", "boolean", sirenState?.Invoke()); break;
            case "privacy" when caps.Features.Privacy:
                var privacy = await QuietAsync(() => control.GetPrivacyModeAsync(ct), ct);
                if (privacy != null) Add("on", "Privatsphäre", "boolean", privacy, readOnly: true); break;
            case "image":
                var image = combined ? http?.Image : await QuietAsync(() => control.GetImageSettingsAsync(ct), ct);
                if (image == null) break;
                Add("bright", "Helligkeit", "number", image.Bright, 0, 255);
                Add("contrast", "Kontrast", "number", image.Contrast, 0, 255);
                Add("saturation", "Sättigung", "number", image.Saturation, 0, 255);
                Add("hue", "Farbton", "number", image.Hue, 0, 255);
                Add("sharpen", "Schärfe", "number", image.Sharpen, 0, 255);
                Add("dayNight", "Tag/Nacht", "select", image.DayNight, options: Options("Auto", "Color", "Black&White"));
                Add("antiFlicker", "Flimmerkorrektur", "select", image.AntiFlicker, options: Options(ImageSettings.AntiFlickerValues));
                Add("flip", "Vertikal spiegeln", "boolean", image.Flip); Add("mirror", "Horizontal spiegeln", "boolean", image.Mirror);
                if (image.HdrMax is > 0) Add("hdr", "HDR", "number", image.Hdr, 0, image.HdrMax); break;
            case "volume":
                Add("volume", "Lautstärke", "number", combined ? http?.Volume : await QuietAsync(() => control.GetVolumeAsync(ct), ct), 0, 100); break;
            case "audio":
                var audio = combined ? http?.Audio : await QuietAsync(() => control.GetAudioStateAsync(ct), ct);
                if (audio != null)
                { Add("recordAudio", "Ton in Reolink-Aufnahmen", "boolean", audio.RecordAudio);
                    Add("talkVolume", "Sprechlautstärke", "number", audio.TalkVolume, 0, 100);
                    Add("visitorVolume", "Besucherlautstärke", "number", audio.VisitorVolume, 0, 100); } break;
            case "ptz" when caps.Features.Ptz:
                Add("command", "Richtung", "select", "stop", options: Options("up", "down", "left", "right", "stop"));
                Add("speed", "Geschwindigkeit", "number", 32, 1, 64);
                Add("durationMs", "Impulsdauer (Millisekunden)", "number", 500, 50, 1000); break;
            case "zoomfocus" when caps.Features.Zoom:
                var lens = await QuietAsync(() => control.GetZoomFocusAsync(ct), ct);
                foreach (string key in new[] { "zoom", "focus" })
                    if (lens?.Element(key) is { } pos && (int?)pos.Element("maxPos") is > 0)
                        Add(key, key == "zoom" ? "Optischer Zoom" : "Fokus", "number", (int?)pos.Element("curPos"),
                            (int?)pos.Element("minPos") ?? 0, (int?)pos.Element("maxPos")); break;
            case "preset" when caps.Features.Ptz:
                var presets = combined ? http?.PtzPresets : await QuietAsync(() => control.GetPtzPresetsAsync(ct), ct);
                if (presets?.Any(p => p.Enabled) == true)
                { var enabled = presets.Where(p => p.Enabled).ToArray(); Add("id", "Position", "select", enabled[0].Id,
                    options: enabled.Select(p => new ProtectControlOption(p.Id, p.Name)).ToArray()); } break;
            case "autotrack":
                Add("on", "Automatische Verfolgung", "boolean", combined ? http?.AutoTrack : await QuietAsync(() => control.GetAutoTrackAsync(ct), ct)); break;
            case "motion":
                Add("sensitivity", "Bewegungsempfindlichkeit", "number", combined ? http?.MdSensitivity : await QuietAsync(() => control.GetMdSensitivityAsync(ct), ct), 1, 50); break;
            case "ai":
                var ai = combined && http?.AiSensitivities != null ? http.AiSensitivities : await QuietAsync(() => control.GetAiSensitivitiesAsync(ct), ct);
                if (ai?.Count > 0)
                {
                    var selected = ai.FirstOrDefault(a => a.Type == (type ?? ai[0].Type));
                    if (selected == null) throw new NotSupportedException("This AI type was not reported by the camera");
                    Add("type", "Erkennungstyp", "select", selected.Type, options: Options(ai.Select(a => a.Type).ToArray()));
                    var parameters = await QuietAsync(() => control.GetAiAlarmParametersAsync(selected.Type, ct), ct);
                    if (parameters == null)
                    { Add("sensitivity", "KI-Empfindlichkeit", "number", selected.Sensitivity, 0, 100);
                        if (selected.StayTime != null) Add("stayTime", "Alarmverzögerung (Sekunden)", "number", selected.StayTime, readOnly: true); }
                    else
                    { Add("enabled", "KI-Erkennung aktiv", "boolean", parameters.Enabled);
                        foreach (var parameter in parameters.Fields)
                            Add(parameter.Id, AiLabel(parameter.Id) + (parameter.Step < 1 ? " (Bildanteil 0–1)" : ""), "number", parameter.Value,
                                parameter.Min, parameter.Max, readOnly: !parameter.Writable, step: parameter.Step); }
                    state["types"] = ai.Select(a => new { type = a.Type, sensitivity = a.Sensitivity, stayTime = a.StayTime }).ToArray();
                } break;
            case "zone":
                type ??= "md";
                if (type != "md" && !CameraControl.AiAlarmTypes.Contains(type)) throw new ArgumentException("Unknown detection type");
                var zone = await QuietAsync(() => control.GetDetectionZoneAsync(type, ct), ct);
                if (zone == null && type == "md")
                    foreach (string aiType in control.ZoneTypes().Where(t => t != "md"))
                        if ((zone = await QuietAsync(() => control.GetDetectionZoneAsync(aiType, ct), ct)) != null) break;
                if (zone != null)
                {
                    // Only the actual per-type grids reported by the camera are offered.
                    Add("type", "Zonentyp", "select", zone.Type, options: Options(control.ZoneTypes().ToArray()));
                    Add("table", "Erkennungsmaske", "mask", zone.Table);
                    Add("cols", "Spalten", "number", zone.Cols, readOnly: true); Add("rows", "Zeilen", "number", zone.Rows, readOnly: true);
                } break;
            case "osd":
                var osd = combined ? http?.Osd : await QuietAsync(() => control.GetOsdSettingsAsync(ct), ct);
                if (osd != null)
                { if (osd.CanShowName) Add("showName", "Kameraname anzeigen", "boolean", osd.ShowName);
                    if (osd.CanShowTime) Add("showTime", "Zeit anzeigen", "boolean", osd.ShowTime);
                    Add("watermark", "Reolink-Logo", "boolean", osd.Watermark);
                    if (osd.PosOptions.Count > 0)
                    { Add("namePos", "Namensposition", "select", osd.NamePos, options: Options(osd.PosOptions.ToArray()));
                        Add("timePos", "Zeitposition", "select", osd.TimePos, options: Options(osd.PosOptions.ToArray())); } } break;
            case "services":
                var ports = await QuietAsync(() => control.GetServicePortsAsync(ct), ct);
                if (ports != null)
                    foreach (var port in ports)
                    { Add(port.Service + "Port", port.Service + " Port", "number", port.Port, readOnly: true);
                        if (port.Enabled != null) Add(port.Service + "Enabled", port.Service + " aktiv", "boolean", port.Enabled, readOnly: true); }
                break;
            case "ai-diagnostic":
                var diagnostic = await QuietAsync(() => control.GetBaichuanAiDiagnosticAsync(type ?? "people", ct), ct);
                if (diagnostic != null)
                { Add("type", "KI-Typ", "select", diagnostic.Type, options: Options(diagnostic.Type), readOnly: true);
                    foreach (var field in diagnostic.NumericFields) state[field.Key] = field.Value;
                    state["reportedFieldNames"] = diagnostic.ReportedFieldNames; state["hasZone"] = diagnostic.HasZone;
                    state["areaMetadata"] = diagnostic.AreaMetadata; }
                break;
        }
        bool supported = fields.Count > 0;
        string? readError = Interlocked.Read(ref _readFailures) != failuresBefore ? _lastReadError : null;
        return new(new(resource, Label(resource), group, supported, supported && fields.Any(f => !f.ReadOnly), actuator,
            fields, supported ? resource == "privacy" ? "Der Privatsphärenmodus kann hier nur ausgelesen werden." : null
                : caps.Provisional ? "camera-not-ready" : readError != null ? "read-failed" : "not-reported-by-camera"), state, readError);
    }

    internal static JsonObject ParseWrite(string body)
    {
        using var doc = JsonDocument.Parse(body, new JsonDocumentOptions { MaxDepth = 8 });
        if (doc.RootElement.ValueKind != JsonValueKind.Object) throw new ArgumentException("Expected an object");
        var outer = doc.RootElement.EnumerateObject().ToArray();
        if (outer.Length != 1 || outer[0].Name != "values" || outer[0].Value.ValueKind != JsonValueKind.Object)
            throw new ArgumentException("Expected only a values object");
        var pairs = outer[0].Value.EnumerateObject().ToArray();
        if (pairs.Length == 0 || pairs.Length > 16 || pairs.Select(p => p.Name).Distinct().Count() != pairs.Length)
            throw new ArgumentException("Provide distinct control fields");
        return JsonNode.Parse(outer[0].Value.GetRawText())!.AsObject();
    }

    internal static void ValidateWrite(ProtectControlResource descriptor, JsonObject values)
    {
        if (!descriptor.Supported || !descriptor.Writable) throw new NotSupportedException("Control unavailable on this camera");
        foreach (var pair in values)
        {
            var field = descriptor.Fields.SingleOrDefault(f => f.Id == pair.Key);
            if (field == null || field.ReadOnly || pair.Value is not JsonValue value)
                throw new ArgumentException("Unknown, read-only or invalid control field");
            bool valid = field.Type switch
            {
                "boolean" => value.TryGetValue<bool>(out _),
                "number" => value.TryGetValue<double>(out var number) && double.IsFinite(number) && (field.Min == null || number >= field.Min)
                    && (field.Max == null || number <= field.Max) && (field.Step is < 1 || number == Math.Truncate(number)),
                "select" => field.Options?.Any(o => JsonSerializer.Serialize(o.Value) == value.ToJsonString()) == true,
                "mask" => value.TryGetValue<string>(out var mask) && mask.Length <= 16384 && mask.All(c => c is '0' or '1'),
                _ => false
            };
            if (!valid) throw new ArgumentException("Control value is outside the advertised schema");
        }
        if (descriptor.Id is "siren" or "privacy" or "volume" or "preset" or "autotrack" or "motion")
            Require(values, descriptor.Fields[0].Id);
        if (descriptor.Id == "ptz") Require(values, "command");
        if (descriptor.Id == "ai")
        { Require(values, "type"); if (values.Count == 1) throw new ArgumentException("Provide an AI setting"); }
        if (descriptor.Id == "zone") Require(values, "type", "table");
        if (descriptor.Id == "image" && values.Count == 1 && values.ContainsKey("hdr")) return;
    }

    private static void Require(JsonObject values, params string[] keys)
    { if (keys.Any(key => !values.ContainsKey(key))) throw new ArgumentException("Required control field is missing"); }
    private static IReadOnlyList<ProtectControlOption> Options(params string[] values) => values.Select(v => new ProtectControlOption(v, v)).ToArray();
    private static IReadOnlyList<ProtectControlOption> Options(params int[] values) => values.Select(v => new ProtectControlOption(v, v.ToString())).ToArray();
    private async Task<T?> QuietAsync<T>(Func<Task<T?>> read, CancellationToken ct)
    {
        try { return await read().ConfigureAwait(false); }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (Exception ex)
        {
            _lastReadError = ex is TimeoutException or OperationCanceledException ? "read-timeout"
                : ex is CameraOfflineException ? "camera-offline" : "camera-read-failed";
            Interlocked.Increment(ref _readFailures);
            return default; // Never reflect exception text containing camera addresses, tokens or raw replies.
        }
    }
    private static string Label(string id) => id switch
    { "led" => "IR und LED", "whiteled" => "Spotlight", "floodlight" => "Flutlicht", "siren" => "Sirene",
      "privacy" => "Privatsphäre", "image" => "Bild", "volume" => "Lautsprecher", "audio" => "Audio",
      "ptz" => "Schwenken und Neigen", "zoomfocus" => "Zoom und Fokus", "preset" => "Gespeicherte Positionen",
      "autotrack" => "Automatische Verfolgung", "motion" => "Bewegungserkennung", "ai" => "KI-Erkennung",
      "zone" => "Erkennungszonen", "osd" => "Bildeinblendungen", _ => id };
    private static string AiLabel(string id) => id switch
    { "sensitivity" => "KI-Empfindlichkeit", "stayTime" => "Alarmverzögerung (Sekunden)",
      "widthMin" => "Minimale Objektbreite", "widthMax" => "Maximale Objektbreite",
      "heightMin" => "Minimale Objekthöhe", "heightMax" => "Maximale Objekthöhe", _ => id };
}

public sealed class ProtectControlBusyException : Exception;
