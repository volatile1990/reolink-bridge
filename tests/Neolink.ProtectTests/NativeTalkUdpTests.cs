using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text.Json;
using Neolink.Media;
using Neolink.Rtsp;
using Neolink.Streaming;

internal static class NativeTalkUdpTests
{
    private static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }
    private static JsonElement Diagnostics(NativeTalkUdpSink sink) => JsonSerializer.SerializeToElement(sink.Diagnostics());
    private static byte[] Rtp() => [0x80, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0xFF, 0x7F];

    public static async Task FilterAndLazyHandshake()
    {
        var control = DispatchProxy.Create<ICameraControl, AudioControlProxy>();
        var probe = (AudioControlProxy)(object)control;
        int port;
        using (var reservation = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0)))
            port = ((IPEndPoint)reservation.Client.LocalEndPoint!).Port;
        var sink = new NativeTalkUdpSink(control, IPAddress.Loopback, port, [IPAddress.Loopback]);
        using var stop = new CancellationTokenSource();
        var serve = sink.RunAsync(stop.Token);
        try
        {
            Check(Diagnostics(sink).GetProperty("ready").GetBoolean(), "native UDP socket did not bind");
            await Task.Delay(50);
            Check(probe.Starts == 0, "opening the native talk listener opened the camera speaker");
            sink.Accept(Rtp(), IPAddress.Parse("192.0.2.1"), stop.Token);
            sink.Accept([0x80, 97, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0], IPAddress.Loopback, stop.Token);
            sink.Accept([0xFF, 0xF1, 0, 0, 0, 0, 0, 0], IPAddress.Loopback, stop.Token);
            await Task.Delay(50);
            Check(probe.Starts == 0 && Diagnostics(sink).GetProperty("rejected").GetInt64() == 3,
                "foreign source, wrong RTP codec or malformed AAC opened a speaker");
            using var client = new UdpClient(AddressFamily.InterNetwork);
            await client.SendAsync(Rtp(), new IPEndPoint(IPAddress.Loopback, port));
            var pcm = await probe.Pcm.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(3));
            Check(probe.Starts == 1 && pcm.SequenceEqual(new byte[4]), "native UDP did not carry G.711 speech to its real PCM sink");
            await probe.Ended.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(4));
            Check(Diagnostics(sink).GetProperty("input").ValueKind == JsonValueKind.Null,
                "an idle native talk stream kept its speaker session open");
        }
        finally
        {
            stop.Cancel(); await serve.WaitAsync(TimeSpan.FromSeconds(3));
        }
        Check(!Diagnostics(sink).GetProperty("ready").GetBoolean(), "stopped native listener still claims readiness");
    }

    internal static async Task<byte[]> MakeAac()
    {
        string ffmpeg = Ffmpeg.ExePath ?? throw new Exception("No ffmpeg for native AAC talk regression");
        var info = new ProcessStartInfo(ffmpeg)
        { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string arg in new[] { "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
            "sine=frequency=440:sample_rate=22050:duration=1.2", "-c:a", "aac", "-profile:a", "aac_low",
            "-ar", "22050", "-ac", "1", "-b:a", "48k", "-f", "adts", "pipe:1" }) info.ArgumentList.Add(arg);
        using var process = Process.Start(info)!;
        using var bytes = new MemoryStream();
        var copy = process.StandardOutput.BaseStream.CopyToAsync(bytes);
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        await copy; await stderr;
        Check(process.ExitCode == 0 && bytes.Length > 0, "synthetic AAC encoding failed");
        return bytes.ToArray();
    }

    public static async Task LiveAacDecodeAndStop()
    {
        byte[] aac = await MakeAac();
        Check(NativeTalkUdpSink.IsCompleteAdts(aac), "native parser rejects real AAC/ADTS");
        Check(!NativeTalkUdpSink.IsCompleteAdts(aac.AsSpan(0, aac.Length - 1)), "native parser accepts truncated AAC");
        var blocks = new List<byte[]>();
        for (int offset = 0; offset < aac.Length;)
        {
            int length = ((aac[offset + 3] & 3) << 11) | (aac[offset + 4] << 3) | (aac[offset + 5] >> 5);
            blocks.Add(aac.AsSpan(offset, length).ToArray()); offset += length;
        }
        var firstPcm = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long totalBytes = 0; double sumSquares = 0; int errors = 0;
        using var lifetime = new CancellationTokenSource();
        var decoder = new NativeAacTalkDecoder(pcm =>
        {
            Interlocked.Add(ref totalBytes, pcm.Length);
            for (int i = 0; i < pcm.Length; i += 2)
            { short sample = (short)(pcm[i] | (pcm[i + 1] << 8)); sumSquares += (double)sample * sample; }
            firstPcm.TrySetResult();
        }, () => Interlocked.Increment(ref errors), lifetime.Token);
        try
        {
            foreach (byte[] block in blocks)
            {
                Check(NativeTalkUdpSink.IsCompleteAdts(block), "one native WebSocket frame is not complete ADTS");
                decoder.Feed(block);
                await Task.Delay(15); // paced source, bounded queue cannot drop all test audio
            }
            // A real talk session has no EOF until the user releases the button.
            // PCM must arrive before closing stdin, proving interactive decoding.
            await firstPcm.Task.WaitAsync(TimeSpan.FromSeconds(3));
            decoder.CompleteInput();
            await decoder.Completion.WaitAsync(TimeSpan.FromSeconds(3));
            Check(totalBytes >= 16000 && errors == 0, "AAC decoder lost most speech or reported an error");
            double rms = Math.Sqrt(sumSquares / (totalBytes / 2));
            Check(rms > 1000 && rms < 5000, "decoded synthetic tone is silent or corrupt");
        }
        finally
        {
            decoder.Stop(); await decoder.Completion.WaitAsync(TimeSpan.FromSeconds(3));
        }
        Console.WriteLine($"Native AAC talk: real 22050-Hz ADTS became {totalBytes / 2} PCM samples at 8000 Hz before EOF.");
    }
}
