// Actual camera PTZ exposed on the existing ONVIF identity. AGPL-3.0.
using System.Globalization;
using System.Xml;
using System.Xml.Linq;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    public const string NsPtz = "http://www.onvif.org/ver20/ptz/wsdl";
    private const string VelocitySpace = "http://www.onvif.org/ver10/tptz/PanTiltSpaces/VelocityGenericSpace";
    private const string RelativeSpace = "http://www.onvif.org/ver10/tptz/PanTiltSpaces/TranslationGenericSpace";
    private const string ZoomVelocitySpace = "http://www.onvif.org/ver10/tptz/ZoomSpaces/VelocityGenericSpace";
    private const string ZoomPositionSpace = "http://www.onvif.org/ver10/tptz/ZoomSpaces/PositionGenericSpace";
    private const string ZoomRelativeSpace = "http://www.onvif.org/ver10/tptz/ZoomSpaces/TranslationGenericSpace";
    private readonly OnvifPtzCamera? _ptzCamera;
    private readonly SemaphoreSlim _nativeRelativeMove = new(1, 1);
    private bool NativePtzAvailable => _controls?.KnownCapabilities is { Provisional: false } caps
        && (caps.Features.Ptz || caps.Features.Zoom);
    private bool NativePanTilt => _controls?.KnownCapabilities?.Features.Ptz == true;
    private bool NativeZoom => _controls?.KnownCapabilities?.Features.Zoom == true;

    private string NativePtzConfiguration(string element) => !NativePtzAvailable ? "" :
        $"<{element} token=\"ptz_main\"><tt:Name>{Esc(_config.Name)}</tt:Name><tt:UseCount>1</tt:UseCount><tt:NodeToken>node_main</tt:NodeToken>" +
        (NativePanTilt ? $"<tt:DefaultRelativePanTiltTranslationSpace>{RelativeSpace}</tt:DefaultRelativePanTiltTranslationSpace>" +
            $"<tt:DefaultContinuousPanTiltVelocitySpace>{VelocitySpace}</tt:DefaultContinuousPanTiltVelocitySpace>" : "") +
        (NativeZoom ? $"<tt:DefaultAbsoluteZoomPositionSpace>{ZoomPositionSpace}</tt:DefaultAbsoluteZoomPositionSpace>" +
            $"<tt:DefaultRelativeZoomTranslationSpace>{ZoomRelativeSpace}</tt:DefaultRelativeZoomTranslationSpace>" +
            $"<tt:DefaultContinuousZoomVelocitySpace>{ZoomVelocitySpace}</tt:DefaultContinuousZoomVelocitySpace>" : "") +
        "<tt:DefaultPTZTimeout>PT2S</tt:DefaultPTZTimeout>" + $"</{element}>";

    private string NativePtzSpaces()
    {
        static string Range(string tag, string uri, bool twoAxes, double min = -1, double max = 1) =>
            $"<tt:{tag}><tt:URI>{uri}</tt:URI><tt:XRange><tt:Min>{min.ToString(CultureInfo.InvariantCulture)}</tt:Min><tt:Max>{max.ToString(CultureInfo.InvariantCulture)}</tt:Max></tt:XRange>" +
            (twoAxes ? "<tt:YRange><tt:Min>-1</tt:Min><tt:Max>1</tt:Max></tt:YRange>" : "") + $"</tt:{tag}>";
        return (NativePanTilt ? Range("RelativePanTiltTranslationSpace", RelativeSpace, true) + Range("ContinuousPanTiltVelocitySpace", VelocitySpace, true) : "") +
            (NativeZoom ? Range("AbsoluteZoomPositionSpace", ZoomPositionSpace, false, 0, 1) + Range("RelativeZoomTranslationSpace", ZoomRelativeSpace, false) +
                Range("ContinuousZoomVelocitySpace", ZoomVelocitySpace, false) : "");
    }

    private async Task<(int Status, string Body)> HandleNativePtzAsync(XElement op, CancellationToken ct)
    {
        if (_controls == null || _ptzCamera == null) return Fault(400, "ter:ActionNotSupported", "Camera controls are disabled");
        if (_controls.IsStopping) return Fault(503, "ter:Action", "Camera controls are stopping");
        try
        {
            var caps = op.Name.LocalName == "Stop" && _controls.KnownCapabilities is { } known ? known
                : await _controls.CapabilitiesAsync(ct).ConfigureAwait(false);
            if (caps.Provisional || !NativePtzAvailable) return Fault(400, "ter:ActionNotSupported", "Camera did not report PTZ capabilities");
            _ptzCamera.ObserveCapabilities(caps);
            string action = op.Name.LocalName;
            string? token = Child(op, "ProfileToken");
            if (token != null && !_streams.Any(s => s.Token == token)) return Fault(400, "ter:NoProfile", "Unknown profile");
            string? configToken = Child(op, "ConfigurationToken") ?? Child(op, "PTZConfigurationToken");
            if (configToken != null && configToken != "ptz_main") return Fault(400, "ter:InvalidArgVal", "Unknown PTZ configuration");
            if (Child(op, "NodeToken") is { } nodeToken && nodeToken != "node_main") return Fault(400, "ter:InvalidArgVal", "Unknown PTZ node");
            if (action is "GetNodes" or "GetNode") await _ptzCamera.PresetsAsync(ct);
            var node = "<tptz:PTZNode token=\"node_main\" FixedHomePosition=\"false\"><tt:Name>Camera PTZ</tt:Name>" +
                $"<tt:SupportedPTZSpaces>{NativePtzSpaces()}</tt:SupportedPTZSpaces><tt:MaximumNumberOfPresets>{_ptzCamera.PresetSlots}</tt:MaximumNumberOfPresets><tt:HomeSupported>false</tt:HomeSupported></tptz:PTZNode>";
            switch (action)
            {
                case "GetServiceCapabilities": return Ok("<tptz:GetServiceCapabilitiesResponse><tptz:Capabilities EFlip=\"false\" Reverse=\"false\" GetCompatibleConfigurations=\"true\" MoveStatus=\"true\" StatusPosition=\"false\"/></tptz:GetServiceCapabilitiesResponse>");
                case "GetConfigurations": return Ok($"<tptz:GetConfigurationsResponse>{NativePtzConfiguration("tptz:PTZConfiguration")}</tptz:GetConfigurationsResponse>");
                case "GetCompatibleConfigurations": return Ok($"<tptz:GetCompatibleConfigurationsResponse>{NativePtzConfiguration("tptz:PTZConfiguration")}</tptz:GetCompatibleConfigurationsResponse>");
                case "GetConfiguration": return Ok($"<tptz:GetConfigurationResponse>{NativePtzConfiguration("tptz:PTZConfiguration")}</tptz:GetConfigurationResponse>");
                case "GetNodes": return Ok($"<tptz:GetNodesResponse>{node}</tptz:GetNodesResponse>");
                case "GetNode": return Ok($"<tptz:GetNodeResponse>{node}</tptz:GetNodeResponse>");
                case "GetConfigurationOptions": return Ok("<tptz:GetConfigurationOptionsResponse><tptz:PTZConfigurationOptions>" +
                    $"<tt:Spaces>{NativePtzSpaces()}</tt:Spaces><tt:PTZTimeout><tt:Min>PT0.1S</tt:Min><tt:Max>PT10S</tt:Max></tt:PTZTimeout></tptz:PTZConfigurationOptions></tptz:GetConfigurationOptionsResponse>");
                case "GetStatus": return Ok("<tptz:GetStatusResponse><tptz:PTZStatus><tt:MoveStatus>" +
                    (NativePanTilt ? $"<tt:PanTilt>{(_ptzCamera.Moving ? "MOVING" : "IDLE")}</tt:PanTilt>" : "") +
                    (NativeZoom ? $"<tt:Zoom>{(_ptzCamera.Zooming ? "MOVING" : "IDLE")}</tt:Zoom>" : "") +
                    $"</tt:MoveStatus><tt:UtcTime>{DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)}</tt:UtcTime></tptz:PTZStatus></tptz:GetStatusResponse>");
                case "GetPresets":
                    var presets = await _ptzCamera.PresetsAsync(ct);
                    return Ok("<tptz:GetPresetsResponse>" + string.Concat((presets ?? []).Where(p => p.Enabled).Select(p =>
                        $"<tptz:Preset token=\"{p.Id}\"><tt:Name>{Esc(p.Name)}</tt:Name></tptz:Preset>")) + "</tptz:GetPresetsResponse>");
                case "GotoPreset":
                    if (!int.TryParse(Child(op, "PresetToken"), NumberStyles.None, CultureInfo.InvariantCulture, out int preset)) throw new ArgumentException();
                    var actual = await _ptzCamera.PresetsAsync(ct);
                    if (actual?.Any(p => p.Enabled && p.Id == preset) != true) throw new ArgumentException();
                    await _ptzCamera.GotoPresetAsync(preset, ct); break;
                case "ContinuousMove": await NativeContinuousMoveAsync(op, ct); break;
                case "RelativeMove": await NativeRelativeMoveAsync(op, ct); break;
                case "AbsoluteMove": await NativeAbsoluteMoveAsync(op, ct); break;
                case "Stop":
                    if (Child(op, "Zoom") != "false") _ptzCamera.StopZoom();
                    if (NativePanTilt && Child(op, "PanTilt") != "false") await _ptzCamera.MoveAsync("stop", 32, TimeSpan.FromSeconds(2), ct); break;
                default: return Fault(400, "ter:ActionNotSupported", "Unsupported PTZ operation");
            }
            return Ok($"<tptz:{action}Response/>");
        }
        catch (ArgumentException) { return Fault(400, "ter:InvalidArgVal", "Invalid camera PTZ request"); }
        catch (NotSupportedException) { return Fault(400, "ter:ActionNotSupported", "Camera does not support the requested PTZ operation"); }
        catch (OperationCanceledException) { throw; }
        catch { return Fault(503, "ter:Action", "Camera PTZ request failed"); }
    }

    private static XElement? PtzChild(XElement parent, string name) => parent.Elements().FirstOrDefault(e => e.Name.LocalName == name);
    private static double Coordinate(XElement element, string key, double min = -1, double max = 1)
    { if (!double.TryParse(element.Attribute(key)?.Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
        || !double.IsFinite(value) || value < min || value > max) throw new ArgumentException(); return value; }
    private static TimeSpan PtzTimeout(XElement op)
    { var value = Child(op, "Timeout"); var timeout = value == null ? TimeSpan.FromSeconds(2) : XmlConvert.ToTimeSpan(value);
        if (timeout.TotalSeconds is < 0.1 or > 10) throw new ArgumentException(); return timeout; }
    private void ValidateAxes(XElement? pan, XElement? zoom)
    { if (pan == null && zoom == null) throw new ArgumentException(); if ((pan != null && !NativePanTilt) || (zoom != null && !NativeZoom)) throw new NotSupportedException(); }

    private async Task NativeContinuousMoveAsync(XElement op, CancellationToken ct)
    {
        var velocity = PtzChild(op, "Velocity") ?? throw new ArgumentException();
        var pan = PtzChild(velocity, "PanTilt"); var zoom = PtzChild(velocity, "Zoom"); ValidateAxes(pan, zoom);
        double x = pan == null ? 0 : Coordinate(pan, "x"), y = pan == null ? 0 : Coordinate(pan, "y"), z = zoom == null ? 0 : Coordinate(zoom, "x");
        var timeout = PtzTimeout(op);
        if (pan != null)
        { string command = Math.Max(Math.Abs(x), Math.Abs(y)) < 0.01 ? "stop" : Math.Abs(x) >= Math.Abs(y) ? x > 0 ? "right" : "left" : y > 0 ? "up" : "down";
            await _ptzCamera!.MoveAsync(command, (float)Math.Clamp(Math.Max(Math.Abs(x), Math.Abs(y)) * 64, 1, 64), timeout, ct); }
        if (zoom != null) { if (Math.Abs(z) < 0.01) _ptzCamera!.StopZoom(); else await _ptzCamera!.ZoomAsync(z, timeout, ct); }
    }

    /// <summary>Reolink's directional protocol has no position readback. Relative pan/tilt are bounded timed nudges,
    /// followed by an explicit Stop; no absolute pose is invented or reported.</summary>
    private async Task NativeRelativeMoveAsync(XElement op, CancellationToken ct)
    {
        var translation = PtzChild(op, "Translation") ?? throw new ArgumentException();
        var pan = PtzChild(translation, "PanTilt"); var zoom = PtzChild(translation, "Zoom"); ValidateAxes(pan, zoom);
        double x = pan == null ? 0 : Coordinate(pan, "x"), y = pan == null ? 0 : Coordinate(pan, "y"), z = zoom == null ? 0 : Coordinate(zoom, "x");
        var speed = PtzChild(op, "Speed"); var panSpeed = speed == null ? null : PtzChild(speed, "PanTilt");
        float sx = (float)((panSpeed == null ? 0.5 : Coordinate(panSpeed, "x", 0.01, 1)) * 64),
            sy = (float)((panSpeed == null ? 0.5 : Coordinate(panSpeed, "y", 0.01, 1)) * 64);
        await _nativeRelativeMove.WaitAsync(ct);
        try
        {
            long? owner = _ptzCamera!.OwnershipGeneration;
            foreach (var move in new[] { (x, x > 0 ? "right" : "left", sx), (y, y > 0 ? "up" : "down", sy) })
            {
                if (Math.Abs(move.Item1) < 0.001) continue;
                var duration = TimeSpan.FromMilliseconds(Math.Clamp(Math.Abs(move.Item1) * 1000, 50, 1000));
                owner = await _ptzCamera.NudgeAsync(move.Item2, move.Item3, duration, ct, owner).ConfigureAwait(false);
                if (owner == null) return; // a newer REST/ONVIF/external operator owns the head
            }
            if (zoom != null)
            { var current = CameraControl.ZoomPosition(await _controls!.Control.GetZoomFocusAsync(ct)) ?? throw new NotSupportedException();
                long next = Math.Clamp(current.Cur + (long)Math.Round(z * (current.Max - current.Min)), current.Min, current.Max);
                await _ptzCamera.SetLensPositionAsync("zoomPos", (uint)next, ct, owner); }
        }
        finally { _nativeRelativeMove.Release(); }
    }

    private async Task NativeAbsoluteMoveAsync(XElement op, CancellationToken ct)
    {
        var position = PtzChild(op, "Position") ?? throw new ArgumentException();
        // Without measured pan/tilt positions an absolute head move cannot be translated honestly.
        if (PtzChild(position, "PanTilt") != null) throw new NotSupportedException();
        var zoom = PtzChild(position, "Zoom") ?? throw new ArgumentException();
        if (!NativeZoom) throw new NotSupportedException();
        double value = Coordinate(zoom, "x", 0, 1);
        var current = CameraControl.ZoomPosition(await _controls!.Control.GetZoomFocusAsync(ct)) ?? throw new NotSupportedException();
        await _ptzCamera!.SetLensPositionAsync("zoomPos", (uint)Math.Round(current.Min + value * (current.Max - current.Min)), ct);
    }
}
