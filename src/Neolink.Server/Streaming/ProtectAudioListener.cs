// Independent audio-only consumption of the already connected source; AGPL-3.0.
using System.Diagnostics;
using Neolink.Media;

namespace Neolink.Streaming;

public sealed record ProtectListenMetadata(bool Available, string? Reason, string? Profile,
    string? SourceCodec, int SourceSampleRate, int SourceChannels, string Mime,
    int OutputSampleRate, int OutputChannels, bool Transcode);

internal sealed class ProtectAudioListener(StreamHub hub, AudioTrackInfo audio)
{
    private readonly long _epoch = hub.SourceEpoch;
    private static readonly Lazy<bool> AacEncoder = new(ProbeAacEncoder);
    internal static bool CanEncodeAac => AacEncoder.Value;

    internal static bool CanFrameAac(AudioTrackInfo audio)
    {
        if (!audio.IsAac || audio.AudioSpecificConfig is not { Length: 2 } asc) return false;
        int type = asc[0] >> 3, frequency = ((asc[0] & 7) << 1) | (asc[1] >> 7), channels = (asc[1] >> 3) & 15;
        int[] rates = [96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350];
        return type is >= 1 and <= 4 && frequency < rates.Length && channels is >= 1 and <= 7
            && rates[frequency] == audio.SampleRate && channels == audio.Channels && (asc[1] & 7) == 0;
    }

    internal static byte[] FrameAdts(AudioTrackInfo audio, byte[] au)
    {
        if (!CanFrameAac(audio) || au.Length is < 1 or > 8184) throw new ArgumentException("Unsupported AAC framing");
        var asc = audio.AudioSpecificConfig!;
        int profile = (asc[0] >> 3) - 1, frequency = ((asc[0] & 7) << 1) | (asc[1] >> 7);
        int channels = (asc[1] >> 3) & 15, length = au.Length + 7;
        byte[] packet = new byte[length];
        packet[0] = 0xFF; packet[1] = 0xF1;
        packet[2] = (byte)((profile << 6) | (frequency << 2) | (channels >> 2));
        packet[3] = (byte)(((channels & 3) << 6) | (length >> 11));
        packet[4] = (byte)(length >> 3); packet[5] = (byte)((length << 5) | 0x1F); packet[6] = 0xFC;
        au.CopyTo(packet, 7);
        return packet;
    }

    internal bool Fresh => hub.SourceEpoch == _epoch && hub.LiveVideo && !hub.AuthenticationFailed
        && hub.AudioMetadataSourceEpoch == _epoch && hub.LastAudioAgeMs is >= 0 and <= 5000
        && hub.Audio is { } current && current.IsAac == audio.IsAac && current.SampleRate == audio.SampleRate
        && current.Channels == audio.Channels && (current.AudioSpecificConfig == audio.AudioSpecificConfig
            || current.AudioSpecificConfig != null && audio.AudioSpecificConfig != null
            && current.AudioSpecificConfig.SequenceEqual(audio.AudioSpecificConfig));

    internal async Task PumpAsync(Stream output, CancellationToken ct)
    {
        var subscription = hub.SubscribeAudio();
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var freshness = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
            while (await timer.WaitForNextTickAsync(lifetime.Token).ConfigureAwait(false))
                if (!Fresh) { lifetime.Cancel(); return; }
        }, CancellationToken.None);
        try
        {
            if (audio.IsAac)
                await foreach (var packet in subscription.Reader.ReadAllAsync(lifetime.Token).ConfigureAwait(false))
                {
                    if (!Fresh) return;
                    if (packet is HubAudioAac aac) await WriteLiveAsync(output, FrameAdts(audio, aac.Au), lifetime.Token).ConfigureAwait(false);
                }
            else await EncodePcmAsync(output, subscription.Reader, lifetime.Token).ConfigureAwait(false);
        }
        finally
        {
            lifetime.Cancel(); hub.UnsubscribeAudio(subscription.Id);
            try { await freshness.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
    }

    private static async Task WriteLiveAsync(Stream output, byte[] packet, CancellationToken ct)
    {
        using var write = CancellationTokenSource.CreateLinkedTokenSource(ct);
        write.CancelAfter(TimeSpan.FromSeconds(2));
        await output.WriteAsync(packet, write.Token).ConfigureAwait(false);
        await output.FlushAsync(write.Token).ConfigureAwait(false);
    }

    private async Task EncodePcmAsync(Stream output, System.Threading.Channels.ChannelReader<HubPacket> reader, CancellationToken ct)
    {
        var info = new ProcessStartInfo(Ffmpeg.ExePath ?? throw new NotSupportedException("AAC encoder unavailable"))
        { UseShellExecute = false, CreateNoWindow = true, RedirectStandardInput = true,
            RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string arg in new[] { "-hide_banner", "-loglevel", "error", "-nostats", "-probesize", "32", "-analyzeduration", "0",
            "-f", "s16le", "-ar", audio.SampleRate.ToString(System.Globalization.CultureInfo.InvariantCulture),
            "-ac", audio.Channels.ToString(System.Globalization.CultureInfo.InvariantCulture), "-i", "pipe:0", "-vn",
            "-c:a", "aac", "-profile:a", "aac_low", "-ar", "22050", "-ac", "1", "-b:a", "32k",
            "-threads", "1", "-flush_packets", "1", "-f", "adts", "pipe:1" }) info.ArgumentList.Add(arg);
        using var process = Process.Start(info) ?? throw new InvalidOperationException("Audio encoder did not start");
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var stderr = process.StandardError.ReadToEndAsync(lifetime.Token);
        var feed = Task.Run(async () =>
        {
            try
            {
                await foreach (var packet in reader.ReadAllAsync(lifetime.Token).ConfigureAwait(false))
                {
                    if (!Fresh) return;
                    if (packet is HubAudioPcm pcm)
                    {
                        await process.StandardInput.BaseStream.WriteAsync(pcm.Pcm, lifetime.Token).ConfigureAwait(false);
                        await process.StandardInput.BaseStream.FlushAsync(lifetime.Token).ConfigureAwait(false);
                    }
                }
            }
            finally { process.StandardInput.Close(); }
        }, CancellationToken.None);
        try
        {
            byte[] buffer = new byte[4096];
            int count;
            while ((count = await process.StandardOutput.BaseStream.ReadAsync(buffer, lifetime.Token).ConfigureAwait(false)) > 0)
                await WriteLiveAsync(output, buffer.AsSpan(0, count).ToArray(), lifetime.Token).ConfigureAwait(false);
        }
        finally
        {
            lifetime.Cancel();
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch { }
            try { await feed.ConfigureAwait(false); } catch (Exception) { }
            try { await stderr.ConfigureAwait(false); } catch (Exception) { }
        }
    }

    private static bool ProbeAacEncoder()
    {
        if (Ffmpeg.ExePath is not { } exe) return false;
        try
        {
            var info = new ProcessStartInfo(exe)
            { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (string arg in new[] { "-hide_banner", "-h", "encoder=aac" }) info.ArgumentList.Add(arg);
            using var process = Process.Start(info)!;
            var stdout = process.StandardOutput.ReadToEndAsync(); var stderr = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit(5000)) { process.Kill(); return false; }
            Task.WaitAll([stdout, stderr], 2000);
            return stdout.IsCompletedSuccessfully && stdout.Result.Contains("Encoder aac", StringComparison.OrdinalIgnoreCase);
        }
        catch { return false; }
    }
}
