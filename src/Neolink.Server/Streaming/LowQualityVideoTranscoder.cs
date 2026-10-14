// Reolink Bridge: optional low-quality HEVC video within one camera identity; AGPL-3.0.
using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;
using System.Text.RegularExpressions;
using Neolink.Media;
using Neolink.Rtsp;

namespace Neolink.Streaming;

/// <summary>
/// Transcodes the existing camera substream, never the main stream. Camera credentials
/// never enter ffmpeg: encoded video is fed over stdin. Audio belongs to the source
/// sink and is not decoded, duplicated or fed to ffmpeg here. Failures clear only
/// the low-quality output and retry with backoff until bridge shutdown.
/// </summary>
public sealed class LowQualityVideoTranscoder
{
    private readonly string _name;
    private readonly IStreamHub _source;
    private readonly StreamHub _output;
    private readonly CancellationToken _lifetime;
    private readonly int _frameRate, _bitrateKbps, _sourceFrameRate;
    private readonly int _outputWidth, _outputHeight;
    private readonly string _encoder, _device;
    private string _state = "waiting-for-source";
    private string? _lastFailure;
    private string[] _encoderErrorCategories = [];
    private long _encodedFrames;
    private int _failures;
    private static readonly TimeSpan SourceTimeout = TimeSpan.FromSeconds(5);

    public LowQualityVideoTranscoder(string name, IStreamHub source, StreamHub output,
        CancellationToken lifetime, int frameRate = 10, int bitrateKbps = 512, int sourceFrameRate = 20,
        int outputWidth = 0, int outputHeight = 0, string encoder = "software", string device = "/dev/dri/renderD128")
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(output);
        if (frameRate is < 1 or > 30 || sourceFrameRate is < 1 or > 60 || bitrateKbps is < 64 or > 8192)
            throw new ArgumentOutOfRangeException(nameof(frameRate), "Invalid low-quality encoder rates");
        ValidateOutput(outputWidth, outputHeight, encoder, device);
        _name = name; _source = source; _output = output; _lifetime = lifetime;
        _frameRate = frameRate; _bitrateKbps = bitrateKbps; _sourceFrameRate = sourceFrameRate;
        _outputWidth = outputWidth; _outputHeight = outputHeight; _encoder = encoder; _device = device;
    }

    public object Diagnostics() => new
    {
        backend = _encoder,
        encoder = _encoder == "vaapi" ? "hevc_vaapi" : "libx265",
        state = Volatile.Read(ref _state),
        outputWidth = _outputWidth, outputHeight = _outputHeight,
        outputFps = _frameRate, sourceFps = _sourceFrameRate,
        videoTimeBasis = _frameRate == _sourceFrameRate ? "bounded-source-arrival-phase" : "configured-output-rate",
        captureSynchronizationVerified = false,
        encodedFrames = Interlocked.Read(ref _encodedFrames),
        failures = Volatile.Read(ref _failures),
        lastFailure = Volatile.Read(ref _lastFailure),
        encoderErrorCategories = Volatile.Read(ref _encoderErrorCategories).ToArray(),
        softwareFallback = false
    };

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(_lifetime, cancellationToken);
        var ct = shutdown.Token;
        int failures = 0;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    bool produced = await RunOnceAsync(ct).ConfigureAwait(false);
                    failures = produced ? 0 : Math.Min(5, failures + 1);
                    Volatile.Write(ref _state, "waiting-for-source");
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
                catch (Exception ex)
                {
                    failures = Math.Min(5, failures + 1);
                    Interlocked.Increment(ref _failures);
                    Volatile.Write(ref _state, "unavailable");
                    Volatile.Write(ref _lastFailure, ex.GetType().Name);
                    // Inputs are pipe-only, and no camera configuration is included in diagnostics.
                    Log.Warn($"{_name}: low-quality {_encoder} encoder unavailable ({ex.GetType().Name}); retrying without backend fallback");
                }
                _output.SourceStopped();
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(30, 1 << failures)), ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally { Volatile.Write(ref _state, "stopped"); _output.SourceStopped(); }
    }

    private async Task<bool> RunOnceAsync(CancellationToken ct)
    {
        Volatile.Write(ref _encoderErrorCategories, []);
        // Do not start an encoder against stale SPS/GOP metadata left by a disconnected publisher.
        Volatile.Write(ref _state, "waiting-for-source");
        while (!_source.VideoReady || !_source.LiveVideo || _source.Codec == null)
            await Task.Delay(250, ct).ConfigureAwait(false);
        if (Ffmpeg.ExePath is not { } exe) throw new InvalidOperationException("ffmpeg is required for HEVC low quality");

        long epoch = _source.SourceEpoch;
        var codec = _source.Codec.Value;
        var (id, reader) = _source.Subscribe(); // internal consumer; does not count as another viewer
        using var session = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = session.Token;
        Process? process = null;
        var tasks = new List<Task>();
        long framesOut = 0;
        try
        {
            var psi = StartInfo(exe, codec, _frameRate, _bitrateKbps, _sourceFrameRate,
                _outputWidth, _outputHeight, _encoder, _device);
            Volatile.Write(ref _state, "starting");
            Log.Info($"{_name}: starting low-quality {_encoder} encoder at {_frameRate} fps; backend fallback disabled");
            process = Process.Start(psi) ?? throw new InvalidOperationException("ffmpeg would not start");
            var proc = process;
            // Cancellation closes pipes too: older platform pipe implementations can ignore ReadAsync cancellation.
            using var terminate = token.Register(() => Kill(proc));
            var firstTimestamp = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
            // Equal rates retain every source picture. Its actual wall duration may
            // differ from the configured ceiling (e.g. 12.5 fps during night exposure).
            var pictureTimes = _frameRate == _sourceFrameRate ? Channel.CreateBounded<uint>(new BoundedChannelOptions(2048)
                { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true }) : null;
            var feed = FeedAsync(proc.StandardInput.BaseStream, reader, epoch, firstTimestamp, pictureTimes?.Writer, token);
            long lastOutput = Stopwatch.GetTimestamp();
            var watch = WatchSourceAsync(epoch, () => Interlocked.Read(ref lastOutput), token);
            var stderr = DrainErrorAsync(proc, token);
            var output = ReadOutputAsync(proc.StandardOutput.BaseStream, epoch, firstTimestamp.Task, pictureTimes?.Reader,
                () =>
                {
                    if (Interlocked.Increment(ref framesOut) == 1)
                    {
                        Volatile.Write(ref _state, "active");
                        Volatile.Write(ref _lastFailure, null);
                        Log.Info($"{_name}: low-quality {_encoder} encoder produced valid HEVC at {_output.Width}x{_output.Height}/{_frameRate} fps");
                    }
                    Interlocked.Increment(ref _encodedFrames);
                    Interlocked.Exchange(ref lastOutput, Stopwatch.GetTimestamp());
                }, token);
            var exited = proc.WaitForExitAsync(token);
            tasks.AddRange([feed, watch, stderr, output, exited]);
            var completed = await Task.WhenAny(feed, watch, output, exited).ConfigureAwait(false);
            await completed.ConfigureAwait(false);
            if (!ct.IsCancellationRequested && proc.HasExited)
            {
                Log.Warn($"{_name}: low-quality video encoder exited (code {proc.ExitCode}, {framesOut} frames)");
                if (proc.ExitCode != 0) throw new InvalidOperationException("Low-quality encoder process failed");
            }
            return framesOut > 0;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested) { return framesOut > 0; }
        finally
        {
            session.Cancel();
            _source.Unsubscribe(id);
            if (process != null) Kill(process);
            // Kill plus cancellation makes all pump tasks finish; keep shutdown bounded if a platform pipe wedges.
            try { await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(3)).ConfigureAwait(false); }
            catch { foreach (var task in tasks) Observe(task); }
            process?.Dispose();
            _output.SourceStopped();
        }
    }

    internal static ProcessStartInfo StartInfo(string exe, VideoCodec inputCodec,
        int frameRate, int bitrateKbps, int sourceFrameRate, int outputWidth = 0, int outputHeight = 0,
        string encoder = "software", string device = "/dev/dri/renderD128")
    {
        ValidateOutput(outputWidth, outputHeight, encoder, device);
        var psi = new ProcessStartInfo(exe)
        {
            UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true
        };
        string Number(int value) => value.ToString(CultureInfo.InvariantCulture);
        void Add(params string[] values) { foreach (var value in values) psi.ArgumentList.Add(value); }
        Add("-hide_banner", "-loglevel", "error", "-nostats", "-nostdin");
        if (encoder == "vaapi")
        {
            psi.Environment["LIBVA_DRIVER_NAME"] = "iHD";
            Add("-init_hw_device", "vaapi=low:" + device, "-filter_hw_device", "low");
        }
        Add(
            "-probesize", "4096", "-analyzeduration", "0", "-threads", "1",
            "-f", inputCodec == VideoCodec.H265 ? "hevc" : "h264", "-framerate", Number(sourceFrameRate), "-i", "pipe:0",
            "-map", "0:v:0", "-an", "-sn", "-dn", "-threads", "1", "-filter_threads", "1");
        string filter = "fps=" + Number(frameRate);
        if (outputWidth > 0) filter += ",scale=" + Number(outputWidth) + ":" + Number(outputHeight) + ":flags=fast_bilinear,setsar=1";
        if (encoder == "vaapi")
        {
            // Software decode permits the fps filter before upload, so only selected pictures enter the GPU.
            // Explicit encoder/device selection cannot silently revert to libx265. GLK uses EncSlice, not low_power.
            Add("-vf", filter + ",format=nv12,hwupload", "-c:v", "hevc_vaapi", "-profile:v", "main",
                "-rc_mode", "CBR", "-bf", "0", "-g", Number(frameRate), "-idr_interval", "0",
                // FFmpeg 6.1's VAAPI HEVC AUD packing can swallow P-picture boundaries on GLK.
                // first_slice_segment_in_pic_flag provides the same framing without those broken AUDs.
                "-aud", "0", "-async_depth", "1", "-flags:v", "-global_header",
                "-bsf:v", "dump_extra=freq=keyframe");
        }
        else
        {
            Add("-vf", filter, "-c:v", "libx265", "-preset", "ultrafast", "-tune", "zerolatency",
                "-pix_fmt", "yuv420p", "-x265-params",
                "pools=none:frame-threads=1:wpp=0:repeat-headers=1:aud=1:bframes=0:rc-lookahead=0:scenecut=0:keyint="
                    + Number(frameRate) + ":min-keyint=" + Number(frameRate) + ":log-level=error");
        }
        Add("-b:v", Number(bitrateKbps) + "k", "-maxrate", Number(bitrateKbps) + "k",
            "-bufsize", Number(bitrateKbps * 2) + "k",
            "-fps_mode", "passthrough", "-f", "hevc", "-flush_packets", "1", "pipe:1");
        return psi;
    }

    private static void ValidateOutput(int width, int height, string encoder, string device)
    {
        if (encoder is not ("software" or "vaapi") || !Regex.IsMatch(device, "^/dev/dri/renderD[0-9]{1,4}$"))
            throw new ArgumentException("Invalid low-quality encoder backend or DRM device");
        if ((width != 0 || height != 0) && (width is < 2 or > 16384 || height is < 2 or > 16384
            || width % 2 != 0 || height % 2 != 0))
            throw new ArgumentOutOfRangeException(nameof(width), "Output resolution must have two even dimensions");
    }

    private async Task FeedAsync(Stream stdin, ChannelReader<HubPacket> reader, long epoch,
        TaskCompletionSource<uint> firstTimestamp, ChannelWriter<uint>? pictureTimes, CancellationToken ct)
    {
        bool started = false;
        long? lastIndex = null;
        var timeline = pictureTimes == null ? null : new LowQualitySourceTimeline(_sourceFrameRate);
        await foreach (var packet in reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            if (_source.SourceEpoch != epoch) return;
            // A bounded hub can evict old packets under load. Never feed a decoder a broken predictive GOP.
            if (lastIndex.HasValue && packet.Index != lastIndex.Value + 1)
                throw new InvalidDataException("Low-quality source subscriber lost packets");
            lastIndex = packet.Index;
            if (packet is not HubVideo video) continue;
            if (video.SourceEpoch != epoch) continue;
            if (!started)
            {
                if (!video.Keyframe) continue;
                started = true;
                firstTimestamp.TrySetResult(video.CameraMicroseconds ?? (uint)((ulong)video.RtpTs * 1000 / 90));
                // Most Baichuan keyframes already contain SPS/PPS. Some sources expose them only via DESCRIBE.
                if (!video.HasSps)
                {
                    if (_source.Codec == VideoCodec.H265) await WriteNalAsync(stdin, _source.Vps, ct).ConfigureAwait(false);
                    await WriteNalAsync(stdin, _source.Sps, ct).ConfigureAwait(false);
                    await WriteNalAsync(stdin, _source.Pps, ct).ConfigureAwait(false);
                }
            }
            if (timeline != null)
            {
                int pictures = RtpAccessUnitSplitter.Split(_source.Codec!.Value, video.AnnexB).Count(unit => unit.HasVcl);
                if (pictures > 0)
                    foreach (uint timestamp in timeline.Map(video, pictures))
                        await pictureTimes!.WriteAsync(timestamp, ct).ConfigureAwait(false);
            }
            await stdin.WriteAsync(video.AnnexB, ct).ConfigureAwait(false);
            await stdin.FlushAsync(ct).ConfigureAwait(false);
        }
    }

    private static async Task WriteNalAsync(Stream output, byte[]? nal, CancellationToken ct)
    {
        if (nal == null) return;
        await output.WriteAsync(new byte[] { 0, 0, 0, 1 }, ct).ConfigureAwait(false);
        await output.WriteAsync(nal, ct).ConfigureAwait(false);
    }

    private async Task WatchSourceAsync(long epoch, Func<long> lastOutput, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            if (_source.SourceEpoch != epoch || !_source.LiveVideo
                || _source.GetVideoDiagnostics().LastVideoAgeMs > SourceTimeout.TotalMilliseconds
                || (Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(10)
                    && Stopwatch.GetElapsedTime(lastOutput()) > SourceTimeout))
                return;
    }

    private async Task ReadOutputAsync(Stream stdout, long epoch, Task<uint> firstTimestamp,
        ChannelReader<uint>? pictureTimes, Action onFrame, CancellationToken ct)
    {
        var assembler = new HevcAccessUnitReader();
        var buffer = new byte[16 * 1024];
        ulong frame = 0;
        int read;
        while ((read = await stdout.ReadAsync(buffer, ct).ConfigureAwait(false)) > 0)
        {
            foreach (var unit in assembler.Feed(buffer.AsSpan(0, read)))
            {
                // SourceStopped is synchronous upstream. Reject any buffered output from its old generation.
                if (_source.SourceEpoch != epoch || !_source.LiveVideo || ct.IsCancellationRequested) return;
                uint timestamp;
                if (pictureTimes != null) timestamp = await pictureTimes.ReadAsync(ct).ConfigureAwait(false);
                else
                {
                    uint origin = await firstTimestamp.WaitAsync(ct).ConfigureAwait(false);
                    timestamp = unchecked(origin + (uint)(frame * 1_000_000 / (ulong)_frameRate));
                }
                if (_source.SourceEpoch != epoch || !_source.LiveVideo || ct.IsCancellationRequested) return;
                frame++;
                uint width = _outputWidth > 0 ? (uint)_outputWidth : _source.Width;
                uint height = _outputHeight > 0 ? (uint)_outputHeight : _source.Height;
                if (frame == 1)
                {
                    var parameters = H26x.SplitNals(unit.Data);
                    CheckFirstOutput(parameters, width, height, unit.Keyframe);
                }
                if (width > 0 && height > 0 && (frame == 1 || _output.Width != width || _output.Height != height))
                    _output.PublishInfo(new MediaInfo(width, height, (byte)_frameRate));
                _output.PublishVideo(new VideoFrame(VideoCodec.H265, unit.Keyframe, timestamp, null, unit.Data));
                onFrame();
            }
        }
        // Do not publish EOF residue: a failed encoder/source must not prime a viewer with a stale final frame.
    }

    internal static void CheckFirstOutput(IReadOnlyList<ReadOnlyMemory<byte>> parameters, uint width, uint height, bool keyframe)
    {
        var sps = parameters.FirstOrDefault(n => H26x.H265NalType(n.Span) == 33);
        if (!keyframe || sps.IsEmpty || !parameters.Any(n => H26x.H265NalType(n.Span) == 32)
            || !parameters.Any(n => H26x.H265NalType(n.Span) == 34)
            || !H26x.TryGetDimensions(VideoCodec.H265, sps.Span, out var actualWidth, out var actualHeight)
            || actualWidth != width || actualHeight != height)
            throw new InvalidDataException("Low-quality encoder output lacks matching HEVC dimensions/decoder parameters");
    }

    internal static string[] ClassifyEncoderError(string text)
    {
        string lower = text.ToLowerInvariant();
        var result = new List<string>();
        if (lower.Contains("permission denied") || lower.Contains("no va display") || lower.Contains("failed to initialise vaapi")) result.Add("device-inaccessible");
        if (lower.Contains("unknown encoder") || lower.Contains("encoder not found")) result.Add("encoder-unavailable");
        if (lower.Contains("error initializing") || lower.Contains("failed to initialize") || lower.Contains("failed to initialise")) result.Add("initialization-failed");
        if (lower.Contains("invalid data") || lower.Contains("invalid nal") || lower.Contains("error while decoding")) result.Add("invalid-bitstream");
        if (lower.Contains("broken pipe") || lower.Contains("input/output error")) result.Add("input-output-failed");
        return result.Count > 0 ? result.ToArray() : ["encoder-reported-error"];
    }

    private async Task DrainErrorAsync(Process proc, CancellationToken ct)
    {
        // Fixed categories retain the diagnostic meaning without camera data or arbitrary process text.
        var buffer = new char[1024];
        string tail = "";
        int count;
        while ((count = await proc.StandardError.ReadAsync(buffer, ct).ConfigureAwait(false)) > 0)
        {
            tail += new string(buffer, 0, count);
            if (tail.Length > 8192) tail = tail[^8192..];
            Volatile.Write(ref _encoderErrorCategories, ClassifyEncoderError(tail));
        }
    }

    private static void Kill(Process proc)
    {
        try { if (!proc.HasExited) proc.Kill(entireProcessTree: true); }
        catch (InvalidOperationException) { }
        catch (System.ComponentModel.Win32Exception) { }
    }

    private static void Observe(Task task) => _ = task.ContinueWith(static t => _ = t.Exception,
        CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
        TaskScheduler.Default);
}

internal sealed record HevcAccessUnit(byte[] Data, bool Keyframe);

/// <summary>Incremental Annex-B HEVC reader. Both start codes and NALs may span arbitrary pipe reads.</summary>
internal sealed class HevcAccessUnitReader
{
    private const int MaxBytes = 2 * 1024 * 1024;
    private byte[] _pending = new byte[16 * 1024];
    private int _length;
    private readonly MemoryStream _unit = new();
    private bool _hasVcl, _keyframe;

    public List<HevcAccessUnit> Feed(ReadOnlySpan<byte> data, bool endOfStream = false)
    {
        if (_length + data.Length > MaxBytes) throw new InvalidDataException("HEVC NAL exceeds low-quality buffer bound");
        if (_pending.Length < _length + data.Length)
            Array.Resize(ref _pending, Math.Min(MaxBytes, Math.Max(_pending.Length * 2, _length + data.Length)));
        data.CopyTo(_pending.AsSpan(_length)); _length += data.Length;
        var result = new List<HevcAccessUnit>();
        int first = FindStartCode(_pending.AsSpan(0, _length), 0, out int startLength);
        if (first < 0)
        {
            if (_length > 4) throw new InvalidDataException("HEVC output is not Annex-B");
            return result;
        }
        int nal = first + startLength;
        int keep = first;
        while (true)
        {
            int next = FindStartCode(_pending.AsSpan(0, _length), nal, out int nextLength);
            if (next < 0)
            {
                if (endOfStream) { AddNal(_pending.AsSpan(nal, _length - nal), result); keep = _length; }
                break;
            }
            AddNal(_pending.AsSpan(nal, next - nal), result);
            keep = next; nal = next + nextLength;
        }
        if (keep > 0)
        {
            _pending.AsSpan(keep, _length - keep).CopyTo(_pending);
            _length -= keep;
        }
        if (endOfStream) Close(result);
        return result;
    }

    private void AddNal(ReadOnlySpan<byte> nal, List<HevcAccessUnit> frames)
    {
        if (nal.Length < 2) return;
        int type = H26x.H265NalType(nal);
        bool vcl = type <= 31;
        bool firstSlice = vcl && nal.Length >= 3 && (nal[2] & 0x80) != 0;
        bool prefix = type is 32 or 33 or 34 or 35 or 39;
        if (_hasVcl && (firstSlice || prefix)) Close(frames);
        if (_unit.Length + 4 + nal.Length > MaxBytes) throw new InvalidDataException("HEVC access unit exceeds low-quality buffer bound");
        _unit.Write(new byte[] { 0, 0, 0, 1 }); _unit.Write(nal);
        _hasVcl |= vcl;
        _keyframe |= vcl && type is >= 16 and <= 23;
    }

    private void Close(List<HevcAccessUnit> frames)
    {
        if (_hasVcl) frames.Add(new HevcAccessUnit(_unit.ToArray(), _keyframe));
        _unit.SetLength(0); _hasVcl = _keyframe = false;
    }

    private static int FindStartCode(ReadOnlySpan<byte> bytes, int from, out int length)
    {
        for (int i = from; i + 2 < bytes.Length; i++)
        {
            if (bytes[i] != 0 || bytes[i + 1] != 0) continue;
            if (bytes[i + 2] == 1) { length = 3; return i; }
            if (i + 3 < bytes.Length && bytes[i + 2] == 0 && bytes[i + 3] == 1) { length = 4; return i; }
        }
        length = 0; return -1;
    }
}
