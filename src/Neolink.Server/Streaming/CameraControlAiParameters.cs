// Firmware-reported AI alarm parameters; preserve all non-edited source fields. AGPL-3.0.
using System.Text.Json.Nodes;

namespace Neolink.Streaming;

public sealed partial class CameraControl
{
    private static readonly IReadOnlyDictionary<string, string[]> AiParameterKeys = new Dictionary<string, string[]>
    {
        ["sensitivity"] = ["sensitivity"], ["stayTime"] = ["stay_time"], ["widthMin"] = ["min_target_width", "widthMin"],
        ["widthMax"] = ["max_target_width", "widthMax"], ["heightMin"] = ["min_target_height", "heightMin"],
        ["heightMax"] = ["max_target_height", "heightMax"]
    };

    public async Task<AiAlarmParameters?> GetAiAlarmParametersAsync(string aiType, CancellationToken ct)
    {
        if (!AiAlarmTypes.Contains(aiType)) throw new ArgumentException("Unknown AI type");
        await EnsureAiTypeSupportedAsync(aiType, ct).ConfigureAwait(false);
        var parameters = await HttpTryAsync<AiAlarmParameters?>(async c =>
        {
            var (cfg, range) = await _httpApi!.GetAiAlarmWithRangeAsync(aiType, c).ConfigureAwait(false);
            return ParseAiParameters(aiType, cfg, range);
        }, ct).ConfigureAwait(false);
        if (parameters != null) { NoteAiTransport(aiType, false); return parameters; }
        var cfg = await TryBaichuanAiConfigAsync(aiType, ct).ConfigureAwait(false);
        return cfg == null ? null : ParseBaichuanAiParameters(aiType, cfg);
    }

    public async Task SetAiAlarmParametersAsync(string aiType, IReadOnlyDictionary<string, double> parameters,
        bool? enabled, CancellationToken ct)
    {
        if (!AiAlarmTypes.Contains(aiType)) throw new ArgumentException("Unknown AI type");
        await EnsureAiTypeSupportedAsync(aiType, ct).ConfigureAwait(false);
        if (AiUsesBaichuan(aiType))
        { await SetBaichuanAiParametersAsync(aiType, parameters, enabled, ct).ConfigureAwait(false); return; }
        var (cfg, range) = await _httpApi!.GetAiAlarmWithRangeAsync(aiType, ct).ConfigureAwait(false);
        ApplyAiParameters(aiType, cfg, range, parameters, enabled);
        await _httpApi.SetAiAlarmAsync(cfg, ct).ConfigureAwait(false);
    }

    internal static AiAlarmParameters ParseAiParameters(string type, JsonObject cfg, JsonObject? range)
    {
        var fields = new List<AiAlarmNumericField>();
        foreach (var pair in AiParameterKeys)
        {
            string? wire = pair.Value.FirstOrDefault(key => cfg.ContainsKey(key));
            if (wire == null || AiNumber(cfg[wire]) is not { } value) continue;
            var bounds = range?[wire] as JsonObject;
            double? min = AiNumber(bounds?["min"]), max = AiNumber(bounds?["max"]);
            bool normalized = wire.EndsWith("_target_width", StringComparison.Ordinal) || wire.EndsWith("_target_height", StringComparison.Ordinal);
            if (normalized && bounds == null)
            {
                // Actual firmware range format: min_target_width=0.0, max_target_width=1.0.
                string axis = wire.EndsWith("width", StringComparison.Ordinal) ? "width" : "height";
                min = AiNumber(range?["min_target_" + axis]); max = AiNumber(range?["max_target_" + axis]);
            }
            // Sensitivity's normalized 0-100 contract is already established in CameraControl.
            if (pair.Key == "sensitivity") { min ??= 0; max ??= 100; }
            bool writable = min is >= 0 && max >= min && max <= 65535 && value >= min && value <= max;
            fields.Add(new(pair.Key, value, min, max, writable, normalized ? 0.000001 : 1));
        }
        bool? enabled = cfg["enable"] is JsonValue enable
            ? enable.TryGetValue<bool>(out var boolean) ? boolean
                : enable.TryGetValue<int>(out var number) && number is 0 or 1 ? number == 1 : null
            : null;
        return new(type, enabled, fields);
    }

    internal static void ApplyAiParameters(string type, JsonObject cfg, JsonObject? range,
        IReadOnlyDictionary<string, double> changes, bool? enabled)
    {
        var actual = ParseAiParameters(type, cfg, range);
        if (changes.Count == 0 && enabled == null) throw new ArgumentException("No AI parameters supplied");
        foreach (var change in changes)
        {
            var field = actual.Fields.SingleOrDefault(f => f.Id == change.Key);
            if (field == null || !field.Writable) throw new NotSupportedException("Camera did not report a writable AI field");
            if (!double.IsFinite(change.Value) || change.Value < field.Min || change.Value > field.Max
                || field.Step >= 1 && change.Value != Math.Truncate(change.Value)) throw new ArgumentException("AI value exceeds camera bounds");
        }
        if (enabled != null && actual.Enabled == null) throw new NotSupportedException("Camera did not report an AI enable switch");
        // Validate the resulting dimensions before touching the source object.
        double? Result(string id) => changes.TryGetValue(id, out double value) ? value : actual.Fields.FirstOrDefault(f => f.Id == id)?.Value;
        foreach (string dimension in new[] { "width", "height" })
            if (Result(dimension + "Max") is > 0 and var maximum && Result(dimension + "Min") is { } minimum && minimum > maximum)
                throw new ArgumentException("Minimum AI target size exceeds maximum");
        foreach (var change in changes)
        {
            string wire = AiParameterKeys[change.Key].First(key => cfg.ContainsKey(key));
            var field = actual.Fields.Single(f => f.Id == change.Key);
            cfg[wire] = field.Step >= 1 ? JsonValue.Create((int)change.Value) : JsonValue.Create(change.Value);
        }
        if (enabled is { } on)
            cfg["enable"] = cfg["enable"] is JsonValue v && v.TryGetValue<bool>(out _) ? JsonValue.Create(on) : JsonValue.Create(on ? 1 : 0);
    }

    private static double? AiNumber(JsonNode? node)
    {
        if (node is not JsonValue value) return null;
        double? number = value.TryGetValue<double>(out var real) ? real : value.TryGetValue<int>(out var integer) ? integer : null;
        return number is { } n && double.IsFinite(n) ? n : null;
    }
}
