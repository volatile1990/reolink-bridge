// UniFi Protect native WebSocket-to-UDP talkback receiver; AGPL-3.0.
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Neolink.Media;
using Neolink.Streaming;

namespace Neolink.Rtsp;

/// <summary>Protect 7.3.70 forwards camera talkback WebSocket binary messages
/// unchanged to camera.host:talkbackSettings.bindPort (vendor module 897293).
/// This opt-in sink accepts complete AAC/ADTS or G.711 RTP datagrams only from
/// explicitly configured console addresses. Only decoded PCM starts TalkAsync;
/// binding, WebSocket opening and invalid datagrams send no camera command.</summary>
public sealed class NativeTalkUdpSink
{
    private readonly ICameraControl _control;
    private readonly IPAddress _bind;
    private readonly int _port;
    private readonly HashSet<IPAddress> _senders;
    private readonly object _gate = new();
    private Session? _session;
    private int _ready;
    private long _acceptedAac, _acceptedRtp, _rejected, _pcmBytes, _decoderErrors;
    private Task _retiring = Task.CompletedTask;
    private long _retryDecoderAt, _decoderRecoveries;
    private int _decoderFailures;
    internal TimeSpan DecoderRecoveryDelay { get; init; } = TimeSpan.FromMilliseconds(500);
    internal Func<Action<byte[]>, Action, CancellationToken, NativeAacTalkDecoder>? DecoderFactory { get; init; }
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(2);

    public NativeTalkUdpSink(ICameraControl control, IPAddress bindAddress, int port,
        IEnumerable<IPAddress> allowedSenders)
    {
        if (bindAddress.AddressFamily != AddressFamily.InterNetwork || port is < 1 or > 65535)
            throw new ArgumentException("Native talk needs an IPv4 listener and a valid UDP port");
        _control = control; _bind = bindAddress; _port = port;
        _senders = allowedSenders.ToHashSet();
        if (_senders.Count == 0 || _senders.Any(ip => ip.AddressFamily != AddressFamily.InterNetwork
            || ip.Equals(IPAddress.Any) || ip.Equals(IPAddress.Broadcast)))
            throw new ArgumentException("Native talk needs an explicit console IPv4 allowlist");
    }

    public object Diagnostics()
    {
        lock (_gate)
            return new
            {
                ready = Volatile.Read(ref _ready) == 1, port = _port,
                allowedSenders = _senders.Count, input = _session?.Format,
                acceptedAac = Interlocked.Read(ref _acceptedAac), acceptedRtp = Interlocked.Read(ref _acceptedRtp),
                rejected = Interlocked.Read(ref _rejected), decodedPcmBytes = Interlocked.Read(ref _pcmBytes),
                decoderErrors = Interlocked.Read(ref _decoderErrors),
                decoderRecoveries = Interlocked.Read(ref _decoderRecoveries),
                idleTimeoutMs = (int)IdleTimeout.TotalMilliseconds,
            };
    }

    public async Task RunAsync(CancellationToken ct)
    {
        using var socket = new UdpClient(new IPEndPoint(_bind, _port));
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Volatile.Write(ref _ready, 1);
        var idle = ExpireIdleAsync(lifetime.Token);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var datagram = await socket.ReceiveAsync(ct).ConfigureAwait(false);
                Accept(datagram.Buffer, datagram.RemoteEndPoint.Address, lifetime.Token);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally
        {
            Volatile.Write(ref _ready, 0);
            lifetime.Cancel();
            Task retiring;
            lock (_gate) { RetireSession(); retiring = _retiring; }
            try { await idle.ConfigureAwait(false); } catch (OperationCanceledException) { }
            await retiring.ConfigureAwait(false);
        }
    }

    internal void Accept(byte[] packet, IPAddress sender, CancellationToken ct)
    {
        if (!_senders.Contains(sender) || ct.IsCancellationRequested || packet.Length > 32768)
        { Interlocked.Increment(ref _rejected); return; }
        byte[]? pcm = null;
        string? format = IsCompleteAdts(packet) ? "aac-adts" :
            (pcm = BackchannelReceiver.Depacketize(packet)) != null ? "g711-rtp" : null;
        if (format == null || (format == "aac-adts" && Ffmpeg.ExePath == null))
        { Interlocked.Increment(ref _rejected); return; }
        lock (_gate)
        {
            if (_session?.DecoderEnded == true)
            {
                RetireSession();
                _decoderFailures = Math.Min(4, _decoderFailures + 1);
                double seconds = Math.Min(5, DecoderRecoveryDelay.TotalSeconds * (1 << (_decoderFailures - 1)));
                _retryDecoderAt = Stopwatch.GetTimestamp() + (long)(seconds * Stopwatch.Frequency);
                Interlocked.Increment(ref _decoderRecoveries);
            }
            if (!_retiring.IsCompleted || Stopwatch.GetTimestamp() < _retryDecoderAt)
            { Interlocked.Increment(ref _rejected); return; }
            if (_session?.Format != format)
            {
                if (_session != null)
                {
                    RetireSession();
                    if (!_retiring.IsCompleted) { Interlocked.Increment(ref _rejected); return; }
                }
                _session = new Session(_control, format, ct,
                    bytes => Interlocked.Add(ref _pcmBytes, bytes),
                    () => Interlocked.Increment(ref _decoderErrors), DecoderFactory);
            }
            _session.LastPacket = Stopwatch.GetTimestamp();
            if (format == "aac-adts") Interlocked.Increment(ref _acceptedAac);
            else Interlocked.Increment(ref _acceptedRtp);
            _session.Feed(packet, pcm);
        }
    }

    private void RetireSession()
    {
        if (_session == null) return;
        _retiring = _session.StopAsync();
        _session = null;
    }

    internal static bool IsCompleteAdts(ReadOnlySpan<byte> packet)
    {
        int offset = 0;
        if (packet.Length < 8) return false;
        while (offset < packet.Length)
        {
            var frame = packet[offset..];
            if (frame.Length < 7 || frame[0] != 0xFF || (frame[1] & 0xF6) != 0xF0) return false;
            int frequency = (frame[2] >> 2) & 15;
            if (frequency > 12) return false;
            int header = (frame[1] & 1) != 0 ? 7 : 9;
            int length = ((frame[3] & 3) << 11) | (frame[4] << 3) | (frame[5] >> 5);
            if (length <= header || length > frame.Length) return false;
            offset += length;
        }
        return offset == packet.Length;
    }

    private async Task ExpireIdleAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            lock (_gate)
                if (_session is { } session && Stopwatch.GetElapsedTime(session.LastPacket) >= IdleTimeout)
                { RetireSession(); _decoderFailures = 0; _retryDecoderAt = 0; }
    }

    private sealed class Session
    {
        public string Format { get; }
        public long LastPacket { get; set; } = Stopwatch.GetTimestamp();
        private readonly BackchannelReceiver _speaker;
        private readonly NativeAacTalkDecoder? _decoder;
        public bool DecoderEnded => _decoder?.Completion.IsCompleted == true;
        public Session(ICameraControl control, string format, CancellationToken ct,
            Action<int> onPcmBytes, Action onDecoderError,
            Func<Action<byte[]>, Action, CancellationToken, NativeAacTalkDecoder>? decoderFactory)
        {
            Format = format;
            _speaker = new(control); _speaker.Start(ct);
            if (format == "aac-adts")
            {
                void Output(byte[] pcm) { onPcmBytes(pcm.Length); _speaker.OnPcm(pcm); }
                _decoder = decoderFactory == null ? new(Output, onDecoderError, ct) : decoderFactory(Output, onDecoderError, ct);
            }
        }
        public void Feed(byte[] packet, byte[]? pcm)
        {
            if (_decoder != null) _decoder.Feed(packet);
            else if (pcm != null) _speaker.OnPcm(pcm);
        }
        public async Task StopAsync()
        {
            var speaker = _speaker.StopAsync();
            _decoder?.Stop();
            if (_decoder != null) await _decoder.Completion.ConfigureAwait(false);
            await speaker.ConfigureAwait(false);
        }
    }
}

/// <summary>One bounded AAC/ADTS decoder per active native talk session. Its
/// process and PCM queue are separate from every recording/video pipeline.</summary>
internal sealed class NativeAacTalkDecoder
{
    private readonly Channel<byte[]> _input = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(32)
    { SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.DropOldest });
    private readonly CancellationTokenSource _lifetime;
    private readonly Action<byte[]> _onPcm;
    private readonly Action _onError;
    private readonly Func<ProcessStartInfo, Process?> _start;
    private Process? _process;
    private readonly Task _completion;
    internal Task Completion => _completion;

    public NativeAacTalkDecoder(Action<byte[]> onPcm, Action onError, CancellationToken ct,
        Func<ProcessStartInfo, Process?>? start = null)
    {
        _onPcm = onPcm; _onError = onError;
        _start = start ?? Process.Start;
        _lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _completion = Task.Run(RunAsync, CancellationToken.None);
    }
    public void Feed(byte[] packet) => _input.Writer.TryWrite(packet);
    internal void CompleteInput() => _input.Writer.TryComplete();
    public void Stop()
    {
        _input.Writer.TryComplete();
        try { _lifetime.Cancel(); } catch (ObjectDisposedException) { }
        try { if (_process is { HasExited: false } process) process.Kill(entireProcessTree: true); } catch { }
    }

    private async Task RunAsync()
    {
        var ct = _lifetime.Token;
        try
        {
            string exe = Ffmpeg.ExePath ?? throw new InvalidOperationException("Native AAC talk requires ffmpeg");
            var info = new ProcessStartInfo(exe)
            {
                UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
            };
            foreach (string arg in new[] { "-hide_banner", "-loglevel", "error", "-nostats", "-probesize", "32",
                "-analyzeduration", "0", "-f", "aac", "-i", "pipe:0", "-vn", "-ac", "1", "-ar", "8000",
                "-c:a", "pcm_s16le", "-flush_packets", "1", "-f", "s16le", "pipe:1" })
                info.ArgumentList.Add(arg);
            using var process = _start(info) ?? throw new InvalidOperationException("AAC decoder did not start");
            _process = process;
            using var terminate = ct.Register(() => { try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch { } });
            var stderr = DrainErrorAsync(process, ct);
            var write = Task.Run(async () =>
            {
                try
                {
                    await foreach (byte[] block in _input.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    {
                        await process.StandardInput.BaseStream.WriteAsync(block, ct).ConfigureAwait(false);
                        await process.StandardInput.BaseStream.FlushAsync(ct).ConfigureAwait(false);
                    }
                }
                finally { process.StandardInput.Close(); }
            }, CancellationToken.None);
            try
            {
                byte[] buffer = new byte[4096];
                int pending = 0;
                while (true)
                {
                    int count = await process.StandardOutput.BaseStream.ReadAsync(buffer.AsMemory(pending), ct).ConfigureAwait(false);
                    if (count == 0) break;
                    int total = count + pending, even = total & ~1;
                    if (even > 0) _onPcm(buffer.AsSpan(0, even).ToArray());
                    pending = total - even;
                    if (pending != 0) buffer[0] = buffer[even];
                }
                await process.WaitForExitAsync(ct).ConfigureAwait(false);
                if (process.ExitCode != 0 && !ct.IsCancellationRequested) _onError();
            }
            finally
            {
                _input.Writer.TryComplete();
                _lifetime.Cancel();
                try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch { }
                try { await write.ConfigureAwait(false); } catch (Exception) { }
                try { await stderr.ConfigureAwait(false); } catch (Exception) { }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception) { if (!ct.IsCancellationRequested) _onError(); }
        finally
        {
            _input.Writer.TryComplete();
            try { if (_process is { HasExited: false } process) process.Kill(entireProcessTree: true); } catch { }
            _process = null;
            _lifetime.Dispose();
        }
    }

    private static async Task DrainErrorAsync(Process process, CancellationToken ct)
    {
        var buffer = new char[1024];
        while (await process.StandardError.ReadAsync(buffer, ct).ConfigureAwait(false) > 0) { }
    }
}
