using System.Text.Json.Nodes;

namespace Neolink.Streaming;

public sealed partial class CameraControl
{
    private IReadOnlySet<string>? _httpAiSupportedTypes;
    private async Task<IReadOnlySet<string>?> HttpAiSupportedTypesAsync(CancellationToken ct)
    {
        if (_httpAiSupportedTypes != null) return _httpAiSupportedTypes;
        var state = await HttpTryAsync<JsonObject?>(async c => await _httpApi!.GetAiStateAsync(c).ConfigureAwait(false), ct).ConfigureAwait(false);
        var ability = await HttpTryAsync<JsonObject?>(async c => await AbilityAsync(c).ConfigureAwait(false), ct).ConfigureAwait(false);
        var supported = ParseHttpAiSupportedTypes(state, ability, _httpApi?.ChannelId ?? 0);
        if (state != null || ability != null) _httpAiSupportedTypes = supported;
        return state == null && ability == null ? null : supported;
    }

    internal static IReadOnlySet<string> ParseHttpAiSupportedTypes(JsonObject? state, JsonObject? ability, int channel)
    {
        if (state?["channel"] is { } responseChannel && responseChannel.GetValue<int>() != channel)
            throw new InvalidDataException("AI support state belongs to another camera channel");
        var result = new HashSet<string>(StringComparer.Ordinal);
        var entry = ability == null ? null : AbilityChannel(ability, channel);
        foreach (string type in AiAlarmTypes)
        {
            string[] keys = type switch
            { "people" => ["supportAiPeople"], "vehicle" => ["supportAiVehicle"], "dog_cat" => ["supportAiDogCat", "supportAiAnimal"],
                "face" => ["supportAiFace"], "package" => ["supportAiPackage"], _ => [] };
            bool? hardware = entry == null ? null : keys.Any(key => entry.Any(pair => pair.Key.Equals(key, StringComparison.OrdinalIgnoreCase)
                && (int?)pair.Value?["ver"] is > 0));
            bool? explicitState = state?[type] is JsonObject status && (int?)status["support"] is { } flag ? flag == 1 : null;
            // Modern per-class support outranks a firmware's generic config shape. Legacy integer state
            // values prove only core classes; face/package need a positive hardware declaration.
            bool legacy = state?[type] is JsonValue number && number.TryGetValue<int>(out int value) && value is 0 or 1;
            if (explicitState == true || explicitState == null && (hardware == true || legacy && hardware != false && type is "people" or "vehicle" or "dog_cat"))
                result.Add(type);
        }
        return result;
    }

    private async Task EnsureAiTypeSupportedAsync(string type, CancellationToken ct)
    {
        if (!AiAlarmTypes.Contains(type)) throw new ArgumentException("Unknown AI type");
        var supported = await HttpAiSupportedTypesAsync(ct).ConfigureAwait(false);
        if (supported != null && !supported.Contains(type) || supported == null && type is "face" or "package")
            throw new NotSupportedException("Camera did not report support for this AI class");
    }
}
