// Source-derived AI diagnostic on the current Baichuan connection. AGPL-3.0.
using System.Globalization;
using System.Text;
using System.Xml.Linq;
using Neolink.Protocol;

namespace Neolink.Streaming;

public sealed record BaichuanAiDiagnostic(string Type, IReadOnlyDictionary<string, string> NumericFields,
    IReadOnlyList<string> ReportedFieldNames, bool HasZone, object? AreaMetadata = null);

public sealed partial class CameraControl
{
    private static readonly IReadOnlyDictionary<string, string> BaichuanAiKeys = new Dictionary<string, string>
    {
        ["sensitivity"] = "sensitivity", ["stayTime"] = "stayTime", ["widthMin"] = "minTargetWidth",
        ["heightMin"] = "minTargetHeight", ["widthMax"] = "maxTargetWidth", ["heightMax"] = "maxTargetHeight"
    };
    private readonly HashSet<string> _baichuanAiTypes = new(StringComparer.Ordinal);

    private void NoteAiTransport(string type, bool baichuan)
    { lock (_baichuanAiTypes) { if (baichuan) _baichuanAiTypes.Add(type); else _baichuanAiTypes.Remove(type); } }
    private bool AiUsesBaichuan(string type)
    { lock (_baichuanAiTypes) return _baichuanAiTypes.Contains(type) || _httpApi == null || HttpPaused; }

    private async Task<XElement?> TryBaichuanAiConfigAsync(string type, CancellationToken ct, bool recordParameterTransport = true)
    {
        if (SleepingOnPurpose) return null;
        try
        {
            return await WithCameraAsync(async camera =>
            {
                var cfg = await camera.GetAiDetectConfigAsync(type, ProbeTimeout, ct).ConfigureAwait(false);
                if (cfg != null)
                {
                    if (recordParameterTransport) NoteAiTransport(type, true);
                    NoteAiZoneSupport(type, ParseBaichuanAiZone(type, cfg) != null, baichuan: true);
                }
                return cfg;
            }, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && ex is CameraCommandException or IOException or TimeoutException or OperationCanceledException)
        { return null; }
    }

    internal static AiAlarmParameters ParseBaichuanAiParameters(string type, XElement cfg)
    {
        var fields = new List<AiAlarmNumericField>();
        foreach (var pair in BaichuanAiKeys)
        {
            if (SingleElement(cfg, pair.Value) is not { } element || XmlNumber(element.Value) is not { } value) continue;
            // 342/343 SDK contract: sensitivity 0..100, stayTime seconds 0..8;
            // min/maxTarget* are image fractions, independent of the area grid's width/height.
            bool fraction = pair.Key is "widthMin" or "widthMax" or "heightMin" or "heightMax";
            double maximum = fraction ? 1 : pair.Key == "stayTime" ? 8 : 100;
            bool valid = value >= 0 && value <= maximum && (fraction || value == Math.Truncate(value));
            fields.Add(new(pair.Key, value, 0, maximum, valid, fraction ? 0.000001 : 1));
        }
        bool? enabled = SingleElement(cfg, "enable")?.Value switch { "0" => false, "1" => true, _ => null };
        return new(type, enabled, fields);
    }

    internal static XElement ApplyBaichuanAiParameters(string type, XElement source,
        IReadOnlyDictionary<string, double> changes, bool? enabled)
    {
        if (source.Element("type")?.Value != type) throw new ArgumentException("AI type differs from source configuration");
        var actual = ParseBaichuanAiParameters(type, source);
        if (changes.Count == 0 && enabled == null) throw new ArgumentException("No AI parameters supplied");
        foreach (var change in changes)
        {
            var field = actual.Fields.SingleOrDefault(f => f.Id == change.Key);
            if (field == null || !field.Writable) throw new NotSupportedException("Camera did not report a writable AI field");
            if (!double.IsFinite(change.Value) || change.Value < field.Min || change.Value > field.Max
                || field.Step >= 1 && change.Value != Math.Truncate(change.Value)) throw new ArgumentException("AI value exceeds camera bounds");
        }
        if (enabled != null && actual.Enabled == null) throw new NotSupportedException("Camera did not report an AI enable switch");
        double? Result(string id) => changes.TryGetValue(id, out double value) ? value : actual.Fields.FirstOrDefault(f => f.Id == id)?.Value;
        foreach (string axis in new[] { "width", "height" })
            if (Result(axis + "Max") is > 0 and var maximum && Result(axis + "Min") is { } minimum && minimum > maximum)
                throw new ArgumentException("Minimum AI target size exceeds maximum");
        var cfg = new XElement(source);
        foreach (var change in changes)
        {
            var field = actual.Fields.Single(f => f.Id == change.Key);
            // Captured camera-app wire format has exactly six decimals and a two-digit exponent.
            SingleElement(cfg, BaichuanAiKeys[change.Key])!.Value = field.Step < 1
                ? change.Value.ToString("0.000000e+00", CultureInfo.InvariantCulture)
                : ((int)change.Value).ToString(CultureInfo.InvariantCulture);
        }
        if (enabled is { } on) SingleElement(cfg, "enable")!.Value = on ? "1" : "0";
        return cfg;
    }

    private Task SetBaichuanAiParametersAsync(string type, IReadOnlyDictionary<string, double> changes, bool? enabled, CancellationToken ct) =>
        WithCameraAsync<object?>(async camera =>
        {
            var cfg = await camera.GetAiDetectConfigAsync(type, ProbeTimeout, ct).ConfigureAwait(false)
                ?? throw new NotSupportedException("Camera did not report this AI type");
            var updated = ApplyBaichuanAiParameters(type, cfg, changes, enabled);
            await camera.SetAiDetectConfigAsync(type, updated, ProbeTimeout, ct).ConfigureAwait(false);
            var readback = await camera.GetAiDetectConfigAsync(type, ProbeTimeout, ct).ConfigureAwait(false)
                ?? throw new IOException("Camera did not return AI configuration after write");
            var actual = ParseBaichuanAiParameters(type, readback);
            foreach (var change in changes)
                if (actual.Fields.SingleOrDefault(f => f.Id == change.Key) is not { } field || Math.Abs(field.Value - change.Value) > (field.Step < 1 ? 0.000001 : 0))
                    throw new IOException("Camera did not apply the requested AI value");
            if (enabled != null && actual.Enabled != enabled) throw new IOException("Camera did not apply the requested AI enable state");
            NoteAiTransport(type, true);
            return null;
        }, ct);

    internal static DetectionZone? ParseBaichuanAiZone(string type, XElement cfg)
    {
        if (!TryBaichuanAiArea(cfg, out int cols, out int rows, out var bytes)) return null;
        var bits = new StringBuilder(cols * rows);
        // Source SDK motionZone helpers and captured AI fixture use MSB-first, row-major; 1 is monitored.
        for (int i = 0; i < cols * rows; i++) bits.Append((bytes[i / 8] & (1 << (7 - i % 8))) != 0 ? '1' : '0');
        return new(type, cols, rows, bits.ToString());
    }

    internal static XElement ApplyBaichuanAiZone(string type, XElement source, string table)
    {
        if (source.Element("type")?.Value != type) throw new ArgumentException("AI type differs from source configuration");
        if (!TryBaichuanAiArea(source, out int cols, out int rows, out var bytes)) throw new NotSupportedException("Camera did not report a valid AI area");
        if (table.Length != cols * rows || table.Any(ch => ch is not ('0' or '1'))) throw new ArgumentException("AI area dimensions changed or contain invalid bits");
        // Keep trailing camera-owned bytes and unused low bits in the final byte exactly as read.
        for (int i = 0; i < table.Length; i++)
        { byte bit = (byte)(1 << (7 - i % 8)); bytes[i / 8] = table[i] == '1' ? (byte)(bytes[i / 8] | bit) : (byte)(bytes[i / 8] & ~bit); }
        var cfg = new XElement(source); SingleElement(cfg, "area")!.Value = Convert.ToBase64String(bytes); return cfg;
    }

    private Task SetBaichuanAiZoneAsync(string type, string table, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        var cfg = await camera.GetAiDetectConfigAsync(type, ProbeTimeout, ct).ConfigureAwait(false)
            ?? throw new NotSupportedException("Camera did not report this AI type");
        await camera.SetAiDetectConfigAsync(type, ApplyBaichuanAiZone(type, cfg, table), ProbeTimeout, ct).ConfigureAwait(false);
        var readback = await camera.GetAiDetectConfigAsync(type, ProbeTimeout, ct).ConfigureAwait(false);
        if (readback == null || ParseBaichuanAiZone(type, readback)?.Table != table) throw new IOException("Camera did not apply the requested AI area");
        NoteAiZoneSupport(type, true, baichuan: true);
        lock (_zoneCache) _zoneCache.Remove(type); return null;
    }, ct);

    private static bool TryBaichuanAiArea(XElement cfg, out int cols, out int rows, out byte[] bytes)
    {
        cols = rows = 0; bytes = [];
        if (!int.TryParse(SingleElement(cfg, "width")?.Value, out cols) || !int.TryParse(SingleElement(cfg, "height")?.Value, out rows)
            || cols <= 0 || rows <= 0 || cols > 512 || rows > 512 || cols * rows > 65536
            || SingleElement(cfg, "area") is not { } area || area.Value.Length > 32768) return false;
        try { bytes = Convert.FromBase64String(area.Value); } catch (FormatException) { return false; }
        return bytes.Length >= (cols * rows + 7) / 8;
    }
    private static XElement? SingleElement(XElement cfg, string name) => cfg.Elements(name).Take(2).ToArray() is [var one] && !one.HasElements ? one : null;
    private static double? XmlNumber(string value) => double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double result) && double.IsFinite(result) ? result : null;

    public Task<BaichuanAiDiagnostic?> GetBaichuanAiDiagnosticAsync(string aiType, CancellationToken ct) =>
        WithCameraAsync(async camera =>
        {
            var cfg = await camera.GetAiDetectConfigAsync(aiType, ProbeTimeout, ct).ConfigureAwait(false);
            if (cfg == null) return null;
            string[] safe = ["sensitivity", "stayTime", "enable", "minWidth", "minHeight", "maxWidth", "maxHeight",
                "min_target_width", "min_target_height", "max_target_width", "max_target_height", "widthMin", "heightMin", "widthMax", "heightMax",
                "minTargetWidth", "minTargetHeight", "maxTargetWidth", "maxTargetHeight", "rows", "cols", "width", "height", "type", "chn"];
            var fields = cfg.Descendants().Where(e => !e.HasElements && safe.Contains(e.Name.LocalName) && e.Value.Length <= 32)
                .GroupBy(e => e.Name.LocalName).ToDictionary(g => g.Key, g => g.First().Value);
            return new BaichuanAiDiagnostic(aiType, fields,
                cfg.Descendants().Select(e => e.Name.LocalName).Distinct().Where(k => !k.Contains("password", StringComparison.OrdinalIgnoreCase)
                    && !k.Contains("token", StringComparison.OrdinalIgnoreCase) && !k.Contains("nonce", StringComparison.OrdinalIgnoreCase)).ToArray(),
                ParseBaichuanAiZone(aiType, cfg) != null,
                TryBaichuanAiArea(cfg, out int cols, out int rows, out var bytes)
                    ? new { cols, rows, byteLength = bytes.Length, packing = "msb-first-row-major", watchedBit = 1 } : null);
        }, ct);
}
