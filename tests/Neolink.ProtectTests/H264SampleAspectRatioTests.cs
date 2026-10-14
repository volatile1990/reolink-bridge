using System.Globalization;
using System.Text;
using Neolink.Media;

internal static class H264SampleAspectRatioTests
{
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    internal static byte[] SyntheticSps(bool withVui = true) => Fixture(66, 1, 0, true, withVui ? 1 : -1, crop: false).Nal;

    public static Task SyntaxPreservation()
    {
        // Independent bit writer builds full SPS syntax, including optional
        // scaling lists, frame crop, POC cycles, timing and both HRD structures.
        foreach (int profile in new[] { 66, 77, 88, 100, 110, 122, 244 })
        foreach (int poc in new[] { 0, 1, 2 })
        foreach (int aspect in new[] { -1, 0, 1, 255 })
        {
            int chroma = profile == 244 ? 3 : 1;
            var source = Fixture(profile, chroma, poc, poc != 1, aspect);
            byte[] original = source.Nal.ToArray();
            byte[] rewritten = H264SampleAspectRatio.RewriteSps(source.Nal, 4, 3);
            var expected = new Bits();
            expected.Add(source.Prefix);
            expected.Bit(true); // VUI is now present
            expected.Bit(true); expected.UInt(255, 8); expected.UInt(4, 16); expected.UInt(3, 16);
            if (aspect == -1) expected.UInt(0, 8);
            else expected.Add(source.Suffix);
            expected.Stop();
            Check(rewritten.SequenceEqual(Nal(expected)), "SPS crop, POC, scaling, timing or HRD bits changed during SAR rewrite");
            Check(source.Nal.SequenceEqual(original), "SPS input storage was modified");
            Check(H264SampleAspectRatio.RewriteSps(rewritten, 4, 3).SequenceEqual(rewritten), "same SAR rewrite is not idempotent");
            Check(H26x.TryGetDimensions(VideoCodec.H264, rewritten, out uint width, out uint height)
                && H26x.TryGetDimensions(VideoCodec.H264, original, out uint oldWidth, out uint oldHeight)
                && width == oldWidth && height == oldHeight, "SAR rewrite changed coded dimensions or frame crop");
            Check(ValidEscapes(rewritten.AsSpan(1)), "rewritten SPS contains an Annex-B start-code emulation");
        }
        foreach (var (num, den) in new[] { (1, 1), (65535, 1), (1, 65535), (65535, 65535), (256, 256) })
        {
            byte[] result = H264SampleAspectRatio.RewriteSps(SyntheticSps(), num, den);
            Check(H264SampleAspectRatio.RewriteSps(result, num, den).SequenceEqual(result), "Extended_SAR boundary ratio was damaged");
            Check(ValidEscapes(result.AsSpan(1)), "SAR boundary produced invalid emulation prevention");
        }
        return Task.CompletedTask;
    }

    public static Task MalformedInputs()
    {
        byte[] good = SyntheticSps();
        foreach (var (num, den) in new[] { (0, 3), (-1, 3), (65536, 3), (4, 0), (4, -1), (4, 65536) })
        {
            bool rejected = false;
            try { H264SampleAspectRatio.RewriteSps(good, num, den); }
            catch (ArgumentOutOfRangeException) { rejected = true; }
            Check(rejected, "invalid SAR ratio was accepted");
        }
        foreach (byte[] bad in new byte[][]
        {
            [], [0x67], [0x68, 0x80], [0xe7, 0x80], [0x07, 0x80],
            [0x67, 0, 0, 3], [0x67, 0, 0, 3, 4], [0x67, 0, 0, 1, 0x80],
            [0x67, 0, 0, 2, 0x80], [0x67, 0, 0, 0, 0x80],
            good.Concat(new byte[] { 0 }).ToArray(), new byte[65537],
            Fixture(99, 1, 0, true, 1).Nal,
            Fixture(66, 1, 0, true, 19).Nal,
            Fixture(66, 1, 0, true, 255, zeroSar: true).Nal,
            Fixture(66, 1, 0, true, 1, zeroTiming: true).Nal,
            Fixture(66, 1, 3, true, 1).Nal,
            Fixture(100, 4, 0, true, 1).Nal
        }) ExpectInvalid(bad);
        for (int length = 0; length < good.Length; length++) ExpectInvalid(good[..length]);
        var overlong = new Bits(); overlong.UInt(66, 8); overlong.UInt(0, 8); overlong.UInt(30, 8);
        overlong.UInt(0, 32); overlong.Bit(true); overlong.Stop();
        ExpectInvalid(Nal(overlong));

        // Bounded adversarial inputs must either parse or fail with the fixed
        // InvalidDataException, never leak offsets/data or throw index errors.
        var random = new Random(1701);
        for (int sample = 0; sample < 1000; sample++)
        {
            var data = new byte[random.Next(2, 300)]; random.NextBytes(data); data[0] = 0x67;
            try { H264SampleAspectRatio.RewriteSps(data, 4, 3); }
            catch (InvalidDataException error) { Check(error.Message == "Invalid H.264 SPS.", "malformed SPS diagnostic includes source data"); }
        }
        return Task.CompletedTask;
    }

    public static async Task RealFfmpegPixelPreservation()
    {
        string ffmpeg = Ffmpeg.ExePath ?? throw new InvalidOperationException("SAR pixel-preservation regression requires ffmpeg");
        const int width = 160, height = 120, frames = 75;
        var (source, encodingErrors) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=160x120:rate=15",
                "-frames:v", "75", "-vf", "setsar=1", "-pix_fmt", "yuv420p", "-c:v", "libx264", "-threads", "1",
                "-preset", "veryfast", "-profile:v", "high", "-x264-params", "repeat-headers=1:aud=1:bframes=0:keyint=15:min-keyint=15:scenecut=0",
                "-f", "h264", "pipe:1"], [], TimeSpan.FromSeconds(25), CancellationToken.None);
        Check(source.Length > 0 && encodingErrors.Length == 0, "synthetic H.264 encoder failed");
        var originalNals = H26x.SplitNals(source);
        var modifiedNals = originalNals.Select(nal => H26x.H264NalType(nal.Span) == H26x.H264Sps
            ? H264SampleAspectRatio.RewriteSps(nal.Span, 4, 3) : nal.ToArray()).ToArray();
        Check(originalNals.Count(n => H26x.H264NalType(n.Span) == H26x.H264Sps) >= 5, "repeated synthetic SPS headers were missing");
        for (int i = 0; i < originalNals.Count; i++)
            if (H26x.H264NalType(originalNals[i].Span) != H26x.H264Sps)
                Check(originalNals[i].Span.SequenceEqual(modifiedNals[i]), "encoded picture/PPS/SEI/AUD changed during SAR rewrite");
        byte[] modified = modifiedNals.SelectMany(n => new byte[] { 0, 0, 0, 1 }.Concat(n)).ToArray();
        string[] decoderArgs = ["-hide_banner", "-loglevel", "error", "-f", "h264", "-i", "pipe:0", "-map", "0:v:0",
            "-an", "-fps_mode", "passthrough", "-pix_fmt", "yuv420p", "-f", "rawvideo", "pipe:1"];
        var (baselinePixels, baselineErrors) = await Ffmpeg.RunAsync(ffmpeg, decoderArgs, [source], TimeSpan.FromSeconds(25), CancellationToken.None);
        var (rewrittenPixels, rewrittenErrors) = await Ffmpeg.RunAsync(ffmpeg, decoderArgs, [modified], TimeSpan.FromSeconds(25), CancellationToken.None);
        Check(baselineErrors.Length == 0 && rewrittenErrors.Length == 0, "SAR stream generated decoder errors");
        Check(baselinePixels.Length == width * height * 3 / 2 * frames, "baseline decoder did not produce 75 complete pictures");
        Check(rewrittenPixels.SequenceEqual(baselinePixels), "SAR metadata rewrite changed decoded picture samples or frame count");
        var (headerTrace, traceErrors) = await Ffmpeg.RunAsync(ffmpeg,
            ["-hide_banner", "-loglevel", "info", "-f", "h264", "-i", "pipe:0", "-map", "0:v:0", "-c:v", "copy",
                "-frames:v", "1", "-f", "null", "pipe:1"], [modified], TimeSpan.FromSeconds(20), CancellationToken.None);
        _ = headerTrace;
        Check(traceErrors.Contains("160x120", StringComparison.Ordinal) && traceErrors.Contains("SAR 4:3 DAR 16:9", StringComparison.Ordinal),
            "independent FFmpeg parser did not recognize the rewritten 4:3 SAR and 16:9 display ratio");
        Console.WriteLine("H.264 SAR rewrite: all 75 decoded 160x120 pictures are byte-identical; FFmpeg reports SAR 4:3 / DAR 16:9.");
    }

    private static void ExpectInvalid(byte[] nal)
    {
        bool rejected = false;
        try { H264SampleAspectRatio.RewriteSps(nal, 4, 3); }
        catch (InvalidDataException error) { rejected = error.Message == "Invalid H.264 SPS."; }
        Check(rejected, "malformed SPS was accepted or produced a non-fixed diagnostic");
    }

    private sealed record SpsFixture(byte[] Nal, string Prefix, string Suffix);

    private static SpsFixture Fixture(int profile, int chroma, int poc, bool frameOnly, int aspect,
        bool zeroSar = false, bool zeroTiming = false, bool crop = true)
    {
        var b = new Bits();
        b.UInt((uint)profile, 8); b.UInt(0, 8); b.UInt(30, 8); b.Ue(0);
        if (profile is 100 or 110 or 122 or 244)
        {
            b.Ue((uint)chroma); if (chroma == 3) b.Bit(true);
            b.Ue(0); b.Ue(0); b.Bit(false); b.Bit(true);
            for (int list = 0; list < (chroma == 3 ? 12 : 8); list++)
            {
                b.Bit(list is 0 or 6);
                if (list == 0) { b.Se(-8); } // nextScale=0: remaining list is inferred
                else if (list == 6) for (int i = 0; i < 64; i++) b.Se(i % 2 == 0 ? 1 : -1);
            }
        }
        b.Ue(0); b.Ue((uint)poc);
        if (poc == 0) b.Ue(2);
        else if (poc == 1) { b.Bit(false); b.Se(-1); b.Se(1); b.Ue(2); b.Se(-2); b.Se(2); }
        b.Ue(2); b.Bit(false); b.Ue(39); b.Ue(frameOnly ? 29u : 14u); b.Bit(frameOnly);
        if (!frameOnly) b.Bit(true);
        b.Bit(true); b.Bit(crop);
        if (crop) { b.Ue(0); b.Ue(2); b.Ue(0); b.Ue(1); }
        string prefix = b.Text;
        b.Bit(aspect != -1);
        if (aspect != -1)
        {
            b.Bit(aspect != 0);
            if (aspect != 0)
            {
                b.UInt((uint)aspect, 8);
                if (aspect == 255) { b.UInt(zeroSar ? 0u : 12u, 16); b.UInt(11, 16); }
            }
        }
        int suffixStart = b.Length;
        if (aspect != -1)
        {
            b.Bit(true); b.Bit(false); // overscan
            b.Bit(true); b.UInt(5, 3); b.Bit(true); b.Bit(true); b.UInt(1, 8); b.UInt(1, 8); b.UInt(1, 8);
            b.Bit(true); b.Ue(2); b.Ue(3);
            b.Bit(true); b.UInt(zeroTiming ? 0u : 1u, 32); b.UInt(30, 32); b.Bit(true);
            b.Bit(true); Hrd(b); b.Bit(true); Hrd(b); b.Bit(false);
            b.Bit(true); b.Bit(true); b.Bit(true); b.Ue(2); b.Ue(1); b.Ue(10); b.Ue(9); b.Ue(1); b.Ue(2);
        }
        string suffix = b.Text[suffixStart..];
        b.Stop();
        return new SpsFixture(Nal(b), prefix, suffix);
    }

    private static void Hrd(Bits b)
    {
        b.Ue(1); b.UInt(0, 4); b.UInt(0, 4);
        b.Ue(255); b.Ue(1023); b.Bit(true); b.Ue(127); b.Ue(511); b.Bit(false);
        b.UInt(23, 5); b.UInt(23, 5); b.UInt(23, 5); b.UInt(24, 5);
    }

    private static byte[] Nal(Bits bits)
    {
        byte[] raw = bits.Bytes();
        var result = new List<byte> { 0x67 };
        int zeros = 0;
        foreach (byte value in raw)
        {
            if (zeros == 2 && value <= 3) { result.Add(3); zeros = 0; }
            result.Add(value); zeros = value == 0 ? zeros + 1 : 0;
        }
        return result.ToArray();
    }

    private static bool ValidEscapes(ReadOnlySpan<byte> data)
    {
        int zeros = 0;
        for (int i = 0; i < data.Length; i++)
        {
            if (zeros == 2)
            {
                if (data[i] <= 2) return false;
                if (data[i] == 3)
                {
                    if (i + 1 >= data.Length || data[i + 1] > 3) return false;
                    zeros = 0; continue;
                }
            }
            zeros = data[i] == 0 ? zeros + 1 : 0;
        }
        return true;
    }

    private sealed class Bits
    {
        private readonly StringBuilder _bits = new();
        public string Text => _bits.ToString();
        public int Length => _bits.Length;
        public void Add(string text) => _bits.Append(text);
        public void Bit(bool bit) => _bits.Append(bit ? '1' : '0');
        public void UInt(uint value, int count) { for (int i = count - 1; i >= 0; i--) Bit(((value >> i) & 1) != 0); }
        public void Ue(uint value)
        {
            string code = Convert.ToString((long)value + 1, 2);
            _bits.Append('0', code.Length - 1); _bits.Append(code);
        }
        public void Se(int value) => Ue((uint)(value <= 0 ? -2 * value : 2 * value - 1));
        public void Stop() { Bit(true); while ((_bits.Length & 7) != 0) Bit(false); }
        public byte[] Bytes()
        {
            var result = new byte[(Length + 7) / 8];
            for (int i = 0; i < result.Length; i++) result[i] = byte.Parse(Text.AsSpan(i * 8, 8), NumberStyles.BinaryNumber, CultureInfo.InvariantCulture);
            return result;
        }
    }
}
