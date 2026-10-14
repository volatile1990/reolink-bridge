using System.Globalization;
using System.Xml.Linq;
using Neolink.Bc.Xml;
using Neolink.Protocol;

namespace Neolink.Streaming;

public sealed partial class CameraControl
{
    private bool BcFallbackImage, BcFallbackVolume, BcFallbackAudioVolumes, BcFallbackRecordAudio,
        BcFallbackMd, BcFallbackMdZone, BcFallbackOsd;
    private static readonly TimeSpan BcFallbackReadTimeout = TimeSpan.FromMilliseconds(900);

    private static BcXmlBody BcFallbackClone(BcXmlBody original)
    {
        var copy = new BcXmlBody();
        copy.Raw.AddRange(original.Raw.Select(e => new XElement(e)));
        return copy;
    }

    private static XElement? BcFallbackOne(IEnumerable<XElement> values) => values.Take(2).ToArray() is { Length: 1 } one ? one[0] : null;
    private static XElement? BcFallbackBlock(BcXmlBody body, string name) => BcFallbackOne(body.Raw.Where(e => e.Name.LocalName == name));
    private static XElement? BcFallbackChild(XElement? parent, string name) => parent == null ? null : BcFallbackOne(parent.Elements(name));
    private static int? BcFallbackInt(XElement? parent, string name, int min, int max) =>
        int.TryParse(BcFallbackChild(parent, name)?.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n) && n >= min && n <= max ? n : null;
    private static bool? BcFallbackFlag(XElement? parent, string name) => BcFallbackInt(parent, name, 0, 1) is { } n ? n != 0 : null;
    private static XElement[] BcFallbackEncAudio(BcXmlBody body)
    {
        // Native cmd56 wraps stream settings in Compression; Enc is the CGI
        // vocabulary and only accepted when the camera actually reports it.
        var enc = BcFallbackOne(body.Raw.Where(e => e.Name.LocalName is "Compression" or "Enc"));
        if (enc == null) return [];
        var parents = new[] { enc }.Concat(enc.Elements().Where(e => e.Name.LocalName is "mainStream" or "subStream" or "thirdStream" or "extStream"));
        if (parents.Any(e => e.Elements("audio").Count() > 1)) throw new InvalidDataException("Ambiguous native audio setting");
        return parents.SelectMany(e => e.Elements("audio")).ToArray();
    }

    private static void BcFallbackSet(XElement? parent, string name, object value)
    {
        var field = BcFallbackChild(parent, name) ?? throw new NotSupportedException("Camera did not report the requested field");
        field.Value = Convert.ToString(value, CultureInfo.InvariantCulture)!;
    }
    private static void BcFallbackRange(int value, int min, int max)
    { if (value < min || value > max) throw new ArgumentOutOfRangeException(nameof(value)); }

    private static async Task<BcXmlBody> BcFallbackReadBody(IBcCamera camera, uint id, CancellationToken ct)
    {
        var response = await camera.SendCommandAsync(id, extension: new ExtensionXml { ChannelId = camera.ChannelId },
            replyTimeout: BcFallbackReadTimeout, ct: ct).ConfigureAwait(false);
        if (response?.Meta.ResponseCode != 200 || response.Xml is not { Raw.Count: > 0 } body)
            throw new IOException("Camera did not acknowledge the configuration read");
        // A body naming a different channel must never be written back.
        foreach (var root in body.Raw)
            foreach (var channel in root.Elements().Where(e => e.Name.LocalName is "channelId" or "channel" or "chn"))
                if (!byte.TryParse(channel.Value, out byte idValue) || idValue != camera.ChannelId)
                    throw new InvalidDataException("Configuration response names another camera channel");
        return BcFallbackClone(body);
    }
    private static async Task BcFallbackWriteBody(IBcCamera camera, uint id, BcXmlBody body, CancellationToken ct)
    {
        var response = await camera.SendCommandAsync(id, body, new ExtensionXml { ChannelId = camera.ChannelId },
            replyTimeout: TimeSpan.FromSeconds(3), ct: ct).ConfigureAwait(false);
        if (response?.Meta.ResponseCode != 200) throw new IOException("Camera did not acknowledge the configuration write");
        uint readId = id switch { 25 => 26, 265 => 264, 57 => 56, 47 => 46, 45 => 44, _ => throw new ArgumentException("Unknown native configuration writer") };
        var confirmed = await BcFallbackReadBody(camera, readId, ct).ConfigureAwait(false);
        static bool Fields(XElement? expected, XElement? actual, params string[] names) => names.All(name =>
            BcFallbackChild(expected, name) is not { } field || BcFallbackChild(actual, name)?.Value == field.Value);
        bool matches = id switch
        {
            25 => Fields(BcFallbackBlock(body, "VideoInput"), BcFallbackBlock(confirmed, "VideoInput"), "bright", "contrast", "saturation", "hue", "sharpen")
                && Fields(BcFallbackChild(BcFallbackBlock(body, "InputAdvanceCfg"), "DayNight"), BcFallbackChild(BcFallbackBlock(confirmed, "InputAdvanceCfg"), "DayNight"), "mode")
                && Fields(BcFallbackChild(BcFallbackBlock(body, "InputAdvanceCfg"), "PowerLineFrequency"), BcFallbackChild(BcFallbackBlock(confirmed, "InputAdvanceCfg"), "PowerLineFrequency"), "mode", "enable"),
            265 => Fields(BcFallbackBlock(body, "audioCfg"), BcFallbackBlock(confirmed, "audioCfg"), "volume", "talkAndReplyVolume", "visitorVolume"),
            57 => BcFallbackEncAudio(body).Select(e => e.Value).SequenceEqual(BcFallbackEncAudio(confirmed).Select(e => e.Value)),
            47 => Fields(BcFallbackChild(BcFallbackBlock(body, "MD"), "sensInfoNew"), BcFallbackChild(BcFallbackBlock(confirmed, "MD"), "sensInfoNew"), "sensitivityDefault")
                && Fields(BcFallbackChild(BcFallbackBlock(body, "MD"), "scope"), BcFallbackChild(BcFallbackBlock(confirmed, "MD"), "scope"), "columns", "rows", "valueTable"),
            45 => Fields(BcFallbackBlock(body, "OsdDatetime"), BcFallbackBlock(confirmed, "OsdDatetime"), "enable", "topLeftX", "topLeftY")
                && Fields(BcFallbackBlock(body, "OsdChannelName"), BcFallbackBlock(confirmed, "OsdChannelName"), "enable", "topLeftX", "topLeftY", "enWatermark"),
            _ => false
        };
        if (!matches) throw new IOException("Camera did not confirm the native configuration change");
    }
    private async Task<T?> BcFallbackTry<T>(uint id, Func<BcXmlBody, T?> parse, CancellationToken ct)
    {
        if (SleepingOnPurpose) return default;
        try { return await WithCameraAsync(async camera => parse(await BcFallbackReadBody(camera, id, ct).ConfigureAwait(false)), ct).ConfigureAwait(false); }
        catch (Exception error) when (error is IOException or InvalidDataException or TimeoutException or CameraCommandException or CameraOfflineException) { return default; }
    }

    internal static ImageSettings? ParseBcFallbackImage(BcXmlBody body)
    {
        var image = BcFallbackBlock(body, "VideoInput");
        var advanced = BcFallbackBlock(body, "InputAdvanceCfg");
        int? Number(string key) => BcFallbackInt(image, key, 0, 255);
        string? day = BcFallbackChild(BcFallbackChild(advanced, "DayNight"), "mode")?.Value switch
        { "auto" => "Auto", "color" => "Color", "blackAndWhite" => "Black&White", _ => null };
        var line = BcFallbackChild(advanced, "PowerLineFrequency");
        string? mode = BcFallbackChild(line, "mode")?.Value;
        string? flicker = mode is not ("off" or "outdoor" or "50hz" or "60hz") ? null : BcFallbackFlag(line, "enable") switch
        { false => "Off", true => BcFallbackChild(line, "mode")?.Value switch { "outdoor" => "Outdoor", "50hz" => "50HZ", "60hz" => "60HZ", _ => null }, _ => null };
        var result = new ImageSettings(Number("bright"), Number("contrast"), Number("saturation"), Number("hue"), Number("sharpen"), day, flicker, null, null);
        return result.Bright == null && result.Contrast == null && result.Saturation == null && result.Hue == null && result.Sharpen == null && day == null && flicker == null ? null : result;
    }
    private async Task<ImageSettings?> GetBcFallbackImageAsync(CancellationToken ct)
    {
        var value = await BcFallbackTry(26, ParseBcFallbackImage, ct).ConfigureAwait(false);
        if (value != null) BcFallbackImage = true;
        return value;
    }
    private Task SetBcFallbackImageAsync(int? bright, int? contrast, int? saturation, int? hue, int? sharpen,
        string? dayNight, string? antiFlicker, bool? flip, bool? mirror, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        if (flip != null || mirror != null) throw new NotSupportedException("Camera did not report native flip/mirror capabilities");
        var body = await BcFallbackReadBody(camera, 26, ct).ConfigureAwait(false);
        var image = BcFallbackBlock(body, "VideoInput"); var advanced = BcFallbackBlock(body, "InputAdvanceCfg");
        foreach (var (key, value) in new[] { ("bright", bright), ("contrast", contrast), ("saturation", saturation), ("hue", hue), ("sharpen", sharpen) })
            if (value is { } v) { BcFallbackRange(v, 0, 255); if (BcFallbackInt(image, key, 0, 255) == null) throw new NotSupportedException(); BcFallbackSet(image, key, v); }
        if (dayNight != null)
        {
            string raw = dayNight switch { "Auto" => "auto", "Color" => "color", "Black&White" => "blackAndWhite", _ => throw new ArgumentException("Unknown day/night mode") };
            if (ParseBcFallbackImage(body)?.DayNight == null) throw new NotSupportedException();
            BcFallbackSet(BcFallbackChild(advanced, "DayNight"), "mode", raw);
        }
        if (antiFlicker != null)
        {
            string raw = antiFlicker switch { "Off" => "off", "Outdoor" => "outdoor", "50HZ" => "50hz", "60HZ" => "60hz", _ => throw new ArgumentException("Unknown anti-flicker mode") };
            var line = BcFallbackChild(advanced, "PowerLineFrequency");
            if (ParseBcFallbackImage(body)?.AntiFlicker == null) throw new NotSupportedException();
            BcFallbackSet(line, "enable", raw == "off" ? 0 : 1);
            if (raw != "off") BcFallbackSet(line, "mode", raw);
        }
        await BcFallbackWriteBody(camera, 25, body, ct).ConfigureAwait(false); return null;
    }, ct);

    private async Task<int?> GetBcFallbackVolumeAsync(CancellationToken ct)
    {
        var value = await BcFallbackTry<int?>(264, body => BcFallbackInt(BcFallbackBlock(body, "audioCfg"), "volume", 0, 100), ct).ConfigureAwait(false);
        if (value != null) BcFallbackVolume = true; return value;
    }
    private async Task<AudioState?> GetBcFallbackAudioAsync(bool record, bool volumes, CancellationToken ct)
    {
        bool? rec = null; int? talk = null, visitor = null;
        if (record)
        {
            rec = await BcFallbackTry<bool?>(56, body =>
            {
                var flags = BcFallbackEncAudio(body);
                if (flags.Length == 0 || flags.Any(e => e.Value is not ("0" or "1"))) return null;
                return flags.Any(e => e.Value == "1");
            }, ct).ConfigureAwait(false);
            if (rec != null) BcFallbackRecordAudio = true;
        }
        if (volumes)
        {
            var audio = await BcFallbackTry(264, body =>
            {
                var root = BcFallbackBlock(body, "audioCfg");
                int? t = BcFallbackInt(root, "talkAndReplyVolume", 0, 100), v = BcFallbackInt(root, "visitorVolume", 0, 100);
                return t == null && v == null ? null : new AudioState(null, t, v);
            }, ct).ConfigureAwait(false);
            talk = audio?.TalkVolume; visitor = audio?.VisitorVolume;
            if (audio != null) BcFallbackAudioVolumes = true;
        }
        return rec == null && talk == null && visitor == null ? null : new AudioState(rec, talk, visitor);
    }
    private Task SetBcFallbackAudioVolumesAsync(int? volume, int? talk, int? visitor, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        var body = await BcFallbackReadBody(camera, 264, ct).ConfigureAwait(false); var root = BcFallbackBlock(body, "audioCfg");
        foreach (var (key, value) in new[] { ("volume", volume), ("talkAndReplyVolume", talk), ("visitorVolume", visitor) })
            if (value is { } v) { BcFallbackRange(v, 0, 100); if (BcFallbackInt(root, key, 0, 100) == null) throw new NotSupportedException(); BcFallbackSet(root, key, v); }
        await BcFallbackWriteBody(camera, 265, body, ct).ConfigureAwait(false); return null;
    }, ct);
    private Task SetBcFallbackRecordAudioAsync(bool on, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        var body = await BcFallbackReadBody(camera, 56, ct).ConfigureAwait(false);
        var flags = BcFallbackEncAudio(body);
        if (flags.Length == 0 || flags.Any(e => e.Value is not ("0" or "1"))) throw new NotSupportedException();
        foreach (var flag in flags) flag.Value = on ? "1" : "0";
        await BcFallbackWriteBody(camera, 57, body, ct).ConfigureAwait(false); return null;
    }, ct);

    private async Task<int?> GetBcFallbackMdAsync(CancellationToken ct)
    {
        var value = await BcFallbackTry<int?>(46, body => BcFallbackInt(BcFallbackChild(BcFallbackBlock(body, "MD"), "sensInfoNew"), "sensitivityDefault", 1, 50), ct).ConfigureAwait(false);
        if (value != null) BcFallbackMd = true; return value;
    }
    private Task SetBcFallbackMdAsync(int value, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        BcFallbackRange(value, 1, 50);
        var body = await BcFallbackReadBody(camera, 46, ct).ConfigureAwait(false);
        var modern = BcFallbackChild(BcFallbackBlock(body, "MD"), "sensInfoNew");
        if (BcFallbackInt(modern, "sensitivityDefault", 1, 50) == null) throw new NotSupportedException();
        // The modern dialect carries the user-facing value; legacy inverse tables
        // are preserved, including their time bands, rather than guessed/rewritten.
        BcFallbackSet(modern, "sensitivityDefault", value);
        await BcFallbackWriteBody(camera, 47, body, ct).ConfigureAwait(false); return null;
    }, ct);

    private static (XElement Scope, byte[] Bytes, int Columns, int Rows, int Width, int Height)? BcFallbackMdGrid(BcXmlBody body)
    {
        var md = BcFallbackBlock(body, "MD"); var scope = BcFallbackChild(md, "scope");
        int? columns = BcFallbackInt(scope, "columns", 1, 512), rows = BcFallbackInt(scope, "rows", 1, 512);
        if (scope == null || columns == null || rows == null) return null;
        int? activeWidth = BcFallbackInt(md, "width", 1, columns.Value), activeHeight = BcFallbackInt(md, "height", 1, rows.Value);
        if (md?.Element("width") != null && activeWidth == null || md?.Element("height") != null && activeHeight == null) return null;
        int width = activeWidth ?? columns.Value, height = activeHeight ?? rows.Value;
        try
        {
            string? table = BcFallbackChild(scope, "valueTable")?.Value;
            if (table == null || table.Length > 65536) return null;
            byte[] bytes = Convert.FromBase64String(table);
            return bytes.Length * 8 < columns * rows ? null : (scope, bytes, columns.Value, rows.Value, width, height);
        }
        catch (FormatException) { return null; }
    }
    internal static DetectionZone? ParseBcFallbackMdZone(BcXmlBody body)
    {
        if (BcFallbackMdGrid(body) is not { } grid) return null;
        var cells = new char[grid.Width * grid.Height];
        for (int r = 0; r < grid.Height; r++) for (int c = 0; c < grid.Width; c++)
        { int bit = r * grid.Columns + c; cells[r * grid.Width + c] = (grid.Bytes[bit / 8] & (1 << (7 - bit % 8))) != 0 ? '1' : '0'; }
        return new DetectionZone("md", grid.Width, grid.Height, new string(cells));
    }
    private async Task<DetectionZone?> GetBcFallbackMdZoneAsync(CancellationToken ct)
    {
        var zone = await BcFallbackTry(46, ParseBcFallbackMdZone, ct).ConfigureAwait(false);
        if (zone != null) BcFallbackMdZone = true; return zone;
    }
    internal static BcXmlBody ApplyBcFallbackMdZone(BcXmlBody original, string table)
    {
        var copy = BcFallbackClone(original);
        var grid = BcFallbackMdGrid(copy) ?? throw new NotSupportedException();
        if (table.Length != grid.Width * grid.Height || table.Any(c => c is not ('0' or '1'))) throw new ArgumentException("Zone must match camera dimensions");
        for (int r = 0; r < grid.Height; r++) for (int c = 0; c < grid.Width; c++)
        {
            int bit = r * grid.Columns + c, mask = 1 << (7 - bit % 8);
            grid.Bytes[bit / 8] = (byte)(table[r * grid.Width + c] == '1' ? grid.Bytes[bit / 8] | mask : grid.Bytes[bit / 8] & ~mask);
        }
        BcFallbackSet(grid.Scope, "valueTable", Convert.ToBase64String(grid.Bytes)); return copy;
    }
    private Task SetBcFallbackMdZoneAsync(string table, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        var original = await BcFallbackReadBody(camera, 46, ct).ConfigureAwait(false);
        await BcFallbackWriteBody(camera, 47, ApplyBcFallbackMdZone(original, table), ct).ConfigureAwait(false); return null;
    }, ct);

    private static readonly string[] BcFallbackPositions = ["UpperLeft", "UpperMid", "UpperRight", "LowerLeft", "LowerMid", "LowerRight"];
    private static string? BcFallbackPosition(XElement? overlay)
    {
        int? x = BcFallbackInt(overlay, "topLeftX", 0, 65537), y = BcFallbackInt(overlay, "topLeftY", 0, 65536);
        int horizontal = x switch { 0 or 1 => 0, 65537 => 1, 65536 => 2, _ => -1 };
        int vertical = y switch { 0 or 1 => 0, 65536 => 1, _ => -1 };
        return horizontal < 0 || vertical < 0 ? null : BcFallbackPositions[vertical * 3 + horizontal];
    }
    internal static OsdSettings? ParseBcFallbackOsd(BcXmlBody body)
    {
        var name = BcFallbackBlock(body, "OsdChannelName"); var time = BcFallbackBlock(body, "OsdDatetime");
        bool? showName = BcFallbackFlag(name, "enable"), showTime = BcFallbackFlag(time, "enable"), watermark = BcFallbackFlag(name, "enWatermark");
        if (showName == null && showTime == null && watermark == null) return null;
        return new OsdSettings(showName ?? false, BcFallbackChild(name, "name")?.Value, BcFallbackPosition(name), showTime ?? false,
            BcFallbackPosition(time), watermark, BcFallbackPositions, showName != null, showTime != null);
    }
    private async Task<OsdSettings?> GetBcFallbackOsdAsync(CancellationToken ct)
    {
        var value = await BcFallbackTry(44, ParseBcFallbackOsd, ct).ConfigureAwait(false);
        if (value != null) BcFallbackOsd = true; return value;
    }
    private Task SetBcFallbackOsdAsync(bool? showName, string? namePos, bool? showTime, string? timePos, bool? watermark, CancellationToken ct) => WithCameraAsync<object?>(async camera =>
    {
        var body = await BcFallbackReadBody(camera, 44, ct).ConfigureAwait(false);
        var name = BcFallbackBlock(body, "OsdChannelName"); var time = BcFallbackBlock(body, "OsdDatetime");
        foreach (var (root, enabled, pos) in new[] { (name, showName, namePos), (time, showTime, timePos) })
        {
            if (enabled is { } on) { if (BcFallbackFlag(root, "enable") == null) throw new NotSupportedException(); BcFallbackSet(root, "enable", on ? 1 : 0); }
            if (pos != null)
            {
                int index = Array.IndexOf(BcFallbackPositions, pos); if (index < 0) throw new ArgumentException("Unknown OSD position");
                if (BcFallbackInt(root, "topLeftX", 0, 65537) == null || BcFallbackInt(root, "topLeftY", 0, 65536) == null) throw new NotSupportedException();
                BcFallbackSet(root, "topLeftX", (index % 3) switch { 0 => 1, 1 => 65537, _ => 65536 });
                BcFallbackSet(root, "topLeftY", index < 3 ? 1 : 65536);
            }
        }
        if (watermark is { } mark) { if (BcFallbackFlag(name, "enWatermark") == null) throw new NotSupportedException(); BcFallbackSet(name, "enWatermark", mark ? 1 : 0); }
        await BcFallbackWriteBody(camera, 45, body, ct).ConfigureAwait(false); return null;
    }, ct);

    private async Task<HttpFeatures> BcFallbackCompleteFeaturesAsync(HttpFeatures partial, CancellationToken ct)
    {
        var image = partial.Image ?? await GetBcFallbackImageAsync(ct).ConfigureAwait(false);
        var volume = partial.Volume ?? await GetBcFallbackVolumeAsync(ct).ConfigureAwait(false);
        var md = partial.MdSensitivity ?? await GetBcFallbackMdAsync(ct).ConfigureAwait(false);
        var osd = partial.Osd ?? await GetBcFallbackOsdAsync(ct).ConfigureAwait(false);
        var nativeAudio = await GetBcFallbackAudioAsync(partial.Audio?.RecordAudio == null,
            partial.Audio?.TalkVolume == null && partial.Audio?.VisitorVolume == null, ct).ConfigureAwait(false);
        AudioState? audio = partial.Audio == null ? nativeAudio : new(partial.Audio.RecordAudio ?? nativeAudio?.RecordAudio,
            partial.Audio.TalkVolume ?? nativeAudio?.TalkVolume, partial.Audio.VisitorVolume ?? nativeAudio?.VisitorVolume);
        return partial with { Image = image, Volume = volume, MdSensitivity = md, Osd = osd, Audio = audio };
    }
}
