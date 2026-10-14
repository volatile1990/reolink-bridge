// Protected audio-only HTTP stream; no camera login or speaker command. AGPL-3.0.
using System.Net.Sockets;
using System.Text;
using Neolink.Streaming;

namespace Neolink.Onvif;

public sealed partial class ProtectOnvifServer
{
    private readonly SemaphoreSlim _listenSlots = new(2, 2);

    private (ProtectOnvifStream Stream, StreamHub Hub, AudioTrackInfo Audio)? ListenSource()
    {
        if (!_config.Controls) return null;
        // Prefer passthrough AAC. Reading the main hub's microphone is independent
        // of the main RTSP mount's video-only GOP policy and changes no SDP.
        foreach (var source in _streams.OrderBy(s => s.Hub.Audio?.IsAac != true).ThenBy(s => Index(s)))
            if (source.Hub is StreamHub hub && hub.LiveVideo && !hub.AuthenticationFailed
                && hub.AudioMetadataSourceEpoch == hub.SourceEpoch && hub.LastAudioAgeMs is >= 0 and <= 5000
                && hub.Audio is { } audio && (ProtectAudioListener.CanFrameAac(audio)
                    || !audio.IsAac && audio.SampleRate > 0 && audio.Channels == 1 && ProtectAudioListener.CanEncodeAac))
                return (source, hub, audio);
        return null;
    }

    public object ListenMetadata()
    {
        var source = ListenSource();
        if (source == null) return new ProtectListenMetadata(false, _config.Controls ? "no-live-compatible-audio" : "controls-disabled",
            null, null, 0, 0, "audio/aac", 0, 0, false);
        var (stream, _, audio) = source.Value;
        return new ProtectListenMetadata(true, null, stream.Token, audio.IsAac ? "AAC" : "PCM16LE", audio.SampleRate,
            audio.Channels, "audio/aac", audio.IsAac ? audio.SampleRate : 22050, audio.IsAac ? audio.Channels : 1, !audio.IsAac);
    }

    private async Task HandleListenAsync(NetworkStream output, IReadOnlyDictionary<string, string> headers, CancellationToken ct)
    {
        if (!AuthenticatedBasic(headers.GetValueOrDefault("Authorization")))
        { await RespondAsync(output, 401, "{\"error\":\"authentication-required\"}", true, ct, "application/json"); return; }
        if (!ControlUserAllowed(headers.GetValueOrDefault("Authorization")))
        { await RespondAsync(output, 403, "{\"error\":\"camera-control-permission-required\"}", true, ct, "application/json"); return; }
        if (!_config.Controls)
        { await RespondAsync(output, 404, "{\"error\":\"controls-disabled\"}", true, ct, "application/json"); return; }
        var source = ListenSource();
        if (source == null)
        { await RespondAsync(output, 503, "{\"error\":\"audio-unavailable\"}", true, ct, "application/json"); return; }
        if (!await _listenSlots.WaitAsync(0, ct).ConfigureAwait(false))
        { await RespondAsync(output, 429, "{\"error\":\"too-many-audio-listeners\"}", true, ct, "application/json"); return; }
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var disconnect = Task.Run(async () =>
        {
            try { await output.ReadAsync(new byte[1], lifetime.Token).ConfigureAwait(false); }
            catch (Exception) { }
            finally { lifetime.Cancel(); }
        }, CancellationToken.None);
        try
        {
            string head = "HTTP/1.1 200 OK\r\nContent-Type: audio/aac\r\nCache-Control: no-store\r\n" +
                "X-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n";
            await output.WriteAsync(Encoding.ASCII.GetBytes(head), lifetime.Token).ConfigureAwait(false);
            await new ProtectAudioListener(source.Value.Hub, source.Value.Audio).PumpAsync(output, lifetime.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { }
        finally
        {
            lifetime.Cancel(); _listenSlots.Release();
            try { await disconnect.ConfigureAwait(false); } catch (Exception) { }
        }
    }
}
