// Copyright (c) 2026 Oluwabori Olaleye
// Licensed under the GNU Affero General Public License v3.0; see the LICENSE file
// in the repository root.
using System.Numerics;

namespace Neolink.Media;

/// <summary>
/// Changes only the VUI sample aspect ratio of an H.264 SPS NAL. The coded
/// dimensions, crop, timing, HRD and all remaining SPS bits are preserved.
/// This changes display geometry, not encoded picture samples. It does not
/// imply that a particular receiver will honor non-square pixels.
/// </summary>
public static class H264SampleAspectRatio
{
    private const int MaxNalBytes = 65536;
    private const string InvalidSps = "Invalid H.264 SPS.";

    /// <summary>
    /// Rewrites one SPS NAL without its Annex-B start code. Other NAL types
    /// must be left untouched by the caller. An absent VUI is created with
    /// only aspect_ratio_info_present_flag enabled. Ratios use Extended_SAR.
    /// </summary>
    public static byte[] RewriteSps(ReadOnlySpan<byte> nal, int numerator, int denominator)
    {
        if (numerator is < 1 or > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(numerator), "Sample aspect ratio must be between 1 and 65535.");
        if (denominator is < 1 or > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(denominator), "Sample aspect ratio must be between 1 and 65535.");
        if (nal.Length is < 2 or > MaxNalBytes || (nal[0] & 0x80) != 0
            || (nal[0] & 0x1f) != H26x.H264Sps || (nal[0] & 0x60) == 0)
            throw Malformed();

        byte[] rbsp = Unescape(nal[1..]);
        if (rbsp.Length == 0 || rbsp[^1] == 0) throw Malformed();
        int dataBits = (rbsp.Length - 1) * 8 + 7 - BitOperations.TrailingZeroCount((uint)rbsp[^1]);
        var reader = new Reader(rbsp, dataBits);
        ParseSpsBeforeVui(ref reader);
        int vuiFlag = reader.Position;
        bool hasVui = reader.Flag();
        int aspectStart = reader.Position;
        int aspectEnd = aspectStart;
        if (hasVui)
        {
            if (reader.Flag())
            {
                uint aspectIdc = reader.Bits(8);
                if (aspectIdc == 255)
                {
                    if (reader.Bits(16) == 0 || reader.Bits(16) == 0) throw Malformed();
                }
                else if (aspectIdc > 16) throw Malformed();
            }
            aspectEnd = reader.Position;
            ParseVuiAfterAspect(ref reader);
        }
        // SPS has no extension after VUI: exactly rbsp_trailing_bits remains.
        if (reader.Position != dataBits) throw Malformed();

        var writer = new Writer();
        writer.Copy(rbsp, 0, hasVui ? aspectStart : vuiFlag);
        if (!hasVui) writer.Bit(true); // vui_parameters_present_flag
        writer.Bit(true);             // aspect_ratio_info_present_flag
        writer.Bits(255, 8);          // Extended_SAR
        writer.Bits((uint)numerator, 16);
        writer.Bits((uint)denominator, 16);
        if (hasVui) writer.Copy(rbsp, aspectEnd, dataBits - aspectEnd);
        else writer.Bits(0, 8); // overscan, signal, chroma, timing, both HRD, pic_struct, restriction
        writer.Bit(true); // rbsp_stop_one_bit; padding is regenerated at the new alignment
        return Escape(nal[0], writer.Finish());
    }

    private static void ParseSpsBeforeVui(ref Reader r)
    {
        uint profile = r.Bits(8);
        if ((r.Bits(8) & 3) != 0) throw Malformed(); // reserved_zero_2bits
        r.Bits(8); // level_idc
        r.Ue(31); // seq_parameter_set_id
        uint chroma = 1;
        bool separateColourPlane = false;
        if (profile is 100 or 110 or 122 or 244 or 44 or 83 or 86 or 118 or 128 or 138 or 139 or 134 or 135 or 144)
        {
            chroma = r.Ue(3);
            if (chroma == 3) separateColourPlane = r.Flag();
            r.Ue(6); r.Ue(6); // bit depths minus 8
            r.Flag(); // qpprime_y_zero_transform_bypass_flag
            if (r.Flag())
            {
                int listCount = chroma == 3 ? 12 : 8;
                for (int i = 0; i < listCount; i++)
                {
                    if (!r.Flag()) continue;
                    int lastScale = 8, nextScale = 8;
                    for (int j = 0; j < (i < 6 ? 16 : 64); j++)
                    {
                        if (nextScale != 0)
                            nextScale = (int)((lastScale + r.Se() + 256) & 255);
                        lastScale = nextScale == 0 ? lastScale : nextScale;
                    }
                }
            }
        }
        else if (profile is not (66 or 77 or 88)) throw Malformed();

        r.Ue(12); // log2_max_frame_num_minus4
        uint poc = r.Ue(2);
        if (poc == 0) r.Ue(12);
        else if (poc == 1)
        {
            r.Flag(); r.Se(); r.Se();
            uint count = r.Ue(255);
            for (uint i = 0; i < count; i++) r.Se();
        }
        r.Ue(16); // max_num_ref_frames
        r.Flag(); // gaps_in_frame_num_value_allowed_flag
        ulong width = ((ulong)r.Ue() + 1) * 16;
        ulong height = ((ulong)r.Ue() + 1) * 16;
        bool frameOnly = r.Flag();
        if (!frameOnly) { height *= 2; r.Flag(); }
        r.Flag(); // direct_8x8_inference_flag
        if (r.Flag())
        {
            ulong left = r.Ue(), right = r.Ue(), top = r.Ue(), bottom = r.Ue();
            uint chromaArrayType = separateColourPlane ? 0 : chroma;
            uint unitX = chromaArrayType is 1 or 2 ? 2u : 1u;
            uint unitY = (chromaArrayType == 1 ? 2u : 1u) * (frameOnly ? 1u : 2u);
            if ((left + right) * unitX >= width || (top + bottom) * unitY >= height) throw Malformed();
        }
    }

    private static void ParseVuiAfterAspect(ref Reader r)
    {
        if (r.Flag()) r.Flag(); // overscan
        if (r.Flag())
        {
            if (r.Bits(3) > 5) throw Malformed(); // video_format
            r.Flag(); // video_full_range_flag
            if (r.Flag()) { r.Bits(8); r.Bits(8); r.Bits(8); }
        }
        if (r.Flag()) { r.Ue(5); r.Ue(5); } // chroma sample location
        if (r.Flag())
        {
            uint numUnitsInTick = r.Bits(32), timeScale = r.Bits(32);
            if (numUnitsInTick == 0 || timeScale == 0) throw Malformed();
            r.Flag(); // fixed_frame_rate_flag
        }
        bool nalHrd = r.Flag();
        if (nalHrd) ParseHrd(ref r);
        bool vclHrd = r.Flag();
        if (vclHrd) ParseHrd(ref r);
        if (nalHrd || vclHrd) r.Flag(); // low_delay_hrd_flag
        r.Flag(); // pic_struct_present_flag
        if (r.Flag())
        {
            r.Flag();
            r.Ue(16); r.Ue(16); r.Ue(16); r.Ue(16);
            uint reorder = r.Ue(16), buffering = r.Ue(16);
            if (reorder > buffering) throw Malformed();
        }
    }

    private static void ParseHrd(ref Reader r)
    {
        uint countMinusOne = r.Ue(31);
        r.Bits(4); r.Bits(4);
        for (uint i = 0; i <= countMinusOne; i++) { r.Ue(); r.Ue(); r.Flag(); }
        r.Bits(5); r.Bits(5); r.Bits(5); r.Bits(5);
    }

    private static byte[] Unescape(ReadOnlySpan<byte> ebsp)
    {
        var bytes = new List<byte>(ebsp.Length);
        int zeros = 0;
        for (int i = 0; i < ebsp.Length; i++)
        {
            byte b = ebsp[i];
            if (zeros >= 2)
            {
                if (b == 3)
                {
                    if (i + 1 == ebsp.Length || ebsp[i + 1] > 3) throw Malformed();
                    zeros = 0;
                    continue;
                }
                if (b < 3) throw Malformed();
            }
            bytes.Add(b);
            zeros = b == 0 ? zeros + 1 : 0;
        }
        return bytes.ToArray();
    }

    private static byte[] Escape(byte header, byte[] rbsp)
    {
        var bytes = new List<byte>(rbsp.Length + 8) { header };
        int zeros = 0;
        foreach (byte b in rbsp)
        {
            if (zeros >= 2 && b <= 3) { bytes.Add(3); zeros = 0; }
            bytes.Add(b);
            zeros = b == 0 ? zeros + 1 : 0;
        }
        if (bytes.Count > MaxNalBytes) throw Malformed();
        return bytes.ToArray();
    }

    private static InvalidDataException Malformed() => new(InvalidSps);

    private ref struct Reader(ReadOnlySpan<byte> data, int bitLimit)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private readonly int _limit = bitLimit;
        public int Position { get; private set; }
        public bool Flag() => Bits(1) != 0;
        public uint Bits(int count)
        {
            if (count > _limit - Position) throw Malformed();
            uint value = 0;
            for (int i = 0; i < count; i++, Position++)
                value = (value << 1) | (uint)((_data[Position >> 3] >> (7 - (Position & 7))) & 1);
            return value;
        }
        public uint Ue(uint maximum = uint.MaxValue)
        {
            int leading = 0;
            while (!Flag()) if (++leading > 31) throw Malformed();
            uint value = (uint)((1UL << leading) - 1 + Bits(leading));
            if (value > maximum) throw Malformed();
            return value;
        }
        public long Se()
        {
            uint value = Ue();
            return (value & 1) == 0 ? -(long)(value / 2) : ((long)value + 1) / 2;
        }
    }

    private sealed class Writer
    {
        private readonly List<byte> _data = [];
        private int _bits;
        public void Bit(bool value)
        {
            if ((_bits & 7) == 0) _data.Add(0);
            if (value) _data[^1] |= (byte)(1 << (7 - (_bits & 7)));
            _bits++;
        }
        public void Bits(uint value, int count)
        {
            for (int i = count - 1; i >= 0; i--) Bit(((value >> i) & 1) != 0);
        }
        public void Copy(ReadOnlySpan<byte> source, int start, int count)
        {
            for (int i = start; i < start + count; i++) Bit((source[i >> 3] & (1 << (7 - (i & 7)))) != 0);
        }
        public byte[] Finish() => _data.ToArray();
    }
}
