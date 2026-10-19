// Reolink Bridge headless lifecycle; AGPL-3.0.
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Protocol;
using Neolink.Rtsp;
using System.Net;
using System.Runtime.InteropServices;

namespace Neolink.Streaming;

/// <summary>Minimal Protect bridge: one camera identity, main and optional substream, local RTSP and ONVIF only.
/// Optional controls share the live session; UI, recording, MQTT and notifications remain absent.</summary>
public static class ProtectBridgeHost
{
    public static async Task<int> RunAsync(NeolinkConfig config)
    {
        var onvif = config.Onvif ?? throw new ArgumentException("ONVIF configuration missing");
        var camera = config.Cameras.Single();
        using var shutdown = new CancellationTokenSource();
        // Listener shutdown first retires PTZ owners; keep the source session
        // alive until their independent stop budget has completed.
        using var sourceLifetime = new CancellationTokenSource();
        ConsoleCancelEventHandler cancel = (_, e) => { e.Cancel = true; shutdown.Cancel(); };
        EventHandler exit = (_, _) => { try { shutdown.Cancel(); } catch (ObjectDisposedException) { } };
        Console.CancelKeyPress += cancel;
        AppDomain.CurrentDomain.ProcessExit += exit;
        using var terminate = OperatingSystem.IsLinux()
            ? PosixSignalRegistration.Create(PosixSignal.SIGTERM, context => { context.Cancel = true; shutdown.Cancel(); }) : null;
        var users = config.Users.ToDictionary(u => u.Name, u => u.Pass, StringComparer.Ordinal);
        var permitted = users.Keys.ToHashSet(StringComparer.Ordinal);
        var pipeline = CreateStreams(camera, onvif, sourceLifetime.Token);
        var hub = pipeline.MainHub;
        var source = pipeline.MainSource;
        using var httpApi = onvif.Controls ? new ReolinkHttpApi(camera.HttpAddress ?? camera.Host,
            camera.Username, camera.Password, camera.ChannelId, onvif.CameraHttpsCertificateSha256) : null;
        var control = onvif.Controls ? new CameraControl(source, httpApi,
            onvif: new OnvifClient(camera.OnvifAddress ?? camera.Host, camera.Username, camera.Password, camera.Name)) : null;
        var rtsp = new RtspServer(users) { TcpOnly = true };
        AddRtspMounts(rtsp, camera.Name, hub, permitted, onvif.GopPlayout, onvif.PlayoutDelayMs, pipeline.LowHub,
            onvif.Talk ? control : null, onvif.GopAudio, onvif.ControlUsers?.ToHashSet(StringComparer.Ordinal),
            onvif.LowQualityMatchHqDelay);
        var snapshots = new NativeProtectSnapshotProvider(() => source.LiveCamera,
            () => !hub.AuthenticationFailed && hub.LiveVideo && hub.VideoReady
                && hub.GetVideoDiagnostics().LastVideoAgeMs is >= 0 and <= 5000,
            () => hub.SourceEpoch, lifetime: shutdown.Token);
        var eventBroker = onvif.Events ? new ProtectEventBroker(onvif.EventStaleSeconds, channel: camera.ChannelId,
            motionEventPolicy: onvif.MotionEventPolicy, replayPath: onvif.EventReplayPath,
            replayRetentionSeconds: onvif.EventReplayRetentionSeconds, replayMaxBytes: onvif.EventReplayMaxBytes) : null;
        var cameraControls = control == null ? null : new ProtectCameraControls(camera.Name, control, () => source.SirenOn,
            onvif.Talk, () => hub.SourceEpoch, sourceLifetime.Token);
        var endpoint = new ProtectOnvifServer(onvif, users, pipeline.OnvifStreams, config.BindPort, snapshots, events: eventBroker,
            controls: cameraControls);
        if (cameraControls != null) cameraControls.ListenMetadata = endpoint.ListenMetadata;
        endpoint.SourceDiagnostics = source.SourceDiagnostics;
        if (onvif.GopAudio) endpoint.GopAudioDiagnostics = rtsp.GopAudioDiagnostics;
        endpoint.LowQualityTimingDiagnostics = rtsp.LowQualityTimingDiagnostics;
        if (pipeline.Transcoder is { } lowEncoder)
        {
            endpoint.LowQualityEncoderDiagnostics = lowEncoder.Diagnostics;
            endpoint.LowQualityDecoderDiagnostics = lowEncoder.DecoderDiagnostics;
        }
        if (endpoint.Events is { } events) source.MotionSink = events.Publish;
        var tasks = new List<Task>
        {
            RunCameraAsync(source, sourceLifetime.Token),
            rtsp.RunAsync(config.BindAddr, config.BindPort, shutdown.Token),
            endpoint.RunAsync(shutdown.Token),
            WatchEventsAsync(endpoint.Events, hub, shutdown.Token)
        };
        if (cameraControls != null) tasks.Add(WarmControlsAsync(cameraControls, hub, shutdown.Token));
        if (onvif.NativeTalkPort > 0 && control != null)
        {
            var nativeTalk = new NativeTalkUdpSink(control, IPAddress.Parse(onvif.Bind), onvif.NativeTalkPort,
                onvif.NativeTalkSenders.Select(IPAddress.Parse));
            endpoint.NativeTalkDiagnostics = nativeTalk.Diagnostics;
            cameraControls!.NativeTalkDiagnostics = nativeTalk.Diagnostics;
            tasks.Add(nativeTalk.RunAsync(shutdown.Token));
        }
        if (pipeline.Transcoder is { } transcoder) tasks.Add(RunLowQualityAsync(transcoder, sourceLifetime.Token));
        Log.Info($"Reolink Bridge starting: one camera identity, {pipeline.OnvifStreams.Count} video profile(s), TCP RTSP, read-only ONVIF and camera alarm events; camera settings are unchanged");
        int result = 0;
        try
        {
            // A listener failure must stop the whole instance rather than leave an apparently healthy partial service.
            var completed = await Task.WhenAny(tasks).ConfigureAwait(false);
            await completed.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
        catch
        {
            Log.Error("Bridge stopped because a listener failed");
            result = 1;
        }
        finally
        {
            shutdown.Cancel();
            await StopControlsBeforeSourceAsync(endpoint, sourceLifetime).ConfigureAwait(false);
            foreach (var task in tasks)
                try { await task.ConfigureAwait(false); } catch (OperationCanceledException) { } catch { }
            Console.CancelKeyPress -= cancel;
            AppDomain.CurrentDomain.ProcessExit -= exit;
        }
        return result;
    }
    internal static async Task StopControlsBeforeSourceAsync(ProtectOnvifServer endpoint, CancellationTokenSource sourceLifetime)
    {
        try { await endpoint.StopControlsAsync().ConfigureAwait(false); }
        finally { sourceLifetime.Cancel(); }
    }

    private static async Task WarmControlsAsync(ProtectCameraControls controls, IStreamHub hub, CancellationToken ct)
    {
        long warmedEpoch = long.MinValue;
        while (!ct.IsCancellationRequested)
        {
            if (hub.LiveVideo && warmedEpoch != hub.SourceEpoch)
            {
                long epoch = hub.SourceEpoch;
                try
                {
                    var caps = await controls.CapabilitiesAsync(ct).ConfigureAwait(false);
                    if (!caps.Provisional && epoch == hub.SourceEpoch) warmedEpoch = epoch;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
                catch { await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); }
            }
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct).ConfigureAwait(false);
        }
    }

    internal sealed record BridgeStreams(CameraService MainSource, StreamHub MainHub,
        StreamHub? SubSourceHub, StreamHub? LowHub, LowQualityVideoTranscoder? Transcoder,
        IReadOnlyList<ProtectOnvifStream> OnvifStreams);

    /// <summary>Both qualities share one login and ONVIF device. Main alone owns alarms and snapshots.</summary>
    internal static BridgeStreams CreateStreams(CameraConfig camera, BridgeOnvifConfig onvif, CancellationToken lifetime)
    {
        var mainHub = new StreamHub(camera.Name + " mainStream");
        var streams = new List<ProtectOnvifStream> { new("main", "/" + camera.Name + "/mainStream", mainHub) };
        if (onvif.Profiles.Count == 1)
            return new(new CameraService(camera, StreamKind.Main, mainHub, TimeSpan.Zero) { RelayOnly = true },
                mainHub, null, null, null, streams);

        var profile = onvif.Profiles[1];
        var nativeSub = new StreamHub(camera.Name + " nativeSubStream");
        var lowHub = onvif.LowQualityTranscode == "H265" ? new StreamHub(camera.Name + " subStream") : nativeSub;
        IMediaSink sink = ReferenceEquals(nativeSub, lowHub) ? nativeSub : new LowQualitySourceSink(nativeSub, lowHub);
        if (onvif.LowQualitySampleAspectRatio == "4:3")
            sink = new LowQualityDisplayMetadataSink(sink, 4, 3);
        var mainSource = new CameraService(camera, StreamKind.Main, mainHub, TimeSpan.Zero)
            { RelayOnly = true, SecondaryVideoSink = sink };
        var transcoder = onvif.LowQualityTranscode == "H265"
            ? new LowQualityVideoTranscoder(camera.Name, nativeSub, lowHub, lifetime, profile.Fps, profile.Bitrate,
                onvif.LowQualitySourceFps, profile.Width, profile.Height, onvif.LowQualityEncoder,
                onvif.LowQualityDevice, onvif.LowQualityDecoder) : null;
        streams.Add(new("sub", "/" + camera.Name + "/subStream", lowHub, RequiredForHealth: false));
        return new(mainSource, mainHub, nativeSub, lowHub, transcoder, streams);
    }

    /// <summary>Video enters the converter once; original substream audio is retained on the low output.</summary>
    internal sealed class LowQualitySourceSink(IMediaSink source, IMediaSink output) : IMediaSink
    {
        // The encoder owns output dimensions/rate; native input metadata may differ after scaling.
        public void PublishInfo(MediaInfo info) => source.PublishInfo(info);
        public void PublishVideo(VideoFrame frame) => source.PublishVideo(frame);
        public void PublishAac(AacFrame frame) { source.PublishAac(frame); output.PublishAac(frame); }
        public void PublishAdpcm(AdpcmFrame frame) { source.PublishAdpcm(frame); output.PublishAdpcm(frame); }
        public void SourceStopped() { source.SourceStopped(); output.SourceStopped(); }
        public void SourceAuthenticationFailed() { source.SourceAuthenticationFailed(); output.SourceAuthenticationFailed(); }
    }

    private static async Task WatchEventsAsync(ProtectEventBroker? events, IStreamHub hub, CancellationToken ct)
    {
        if (events == null) { await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false); return; }
        long epoch = hub.SourceEpoch;
        bool live = hub.LiveVideo;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                long currentEpoch = hub.SourceEpoch;
                bool currentLive = hub.LiveVideo;
                if (currentEpoch != epoch || (live && !currentLive)) events.ResetActive();
                events.SetSourceConnected(currentLive);
                epoch = currentEpoch; live = currentLive;
                events.Tick();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally { events.ResetActive(); }
    }

    /// <summary>Main aliases retain their source and playout policy. Optional low output has its own media and audio.</summary>
    internal static void AddRtspMounts(RtspServer rtsp, string cameraName, IStreamHub hub,
        HashSet<string> permitted, bool gopPlayout, int playoutDelayMs, IStreamHub? lowHub = null, ICameraControl? talk = null,
        bool gopAudio = false, HashSet<string>? talkPermittedUsers = null, bool lowQualityMatchHqDelay = false)
    {
        var timing = lowQualityMatchHqDelay && gopPlayout && lowHub != null
            ? new HqPlayoutReference(hub, rtsp.PlayoutTimeProvider) : null;
        rtsp.AddMount(new RtspMount { Path = "/" + cameraName + "/mainStream", Hub = hub,
            PermittedUsers = permitted, GopPlayout = gopPlayout, GopAudio = gopAudio, PlayoutDelayMs = playoutDelayMs,
            PublishHqTiming = timing,
            Talk = gopPlayout ? null : talk, TalkPermittedUsers = talkPermittedUsers });
        rtsp.AddMount(new RtspMount { Path = "/" + cameraName, Hub = hub,
            PermittedUsers = permitted, GopPlayout = gopPlayout, GopAudio = gopAudio, PlayoutDelayMs = playoutDelayMs,
            PublishHqTiming = timing,
            Talk = gopPlayout ? null : talk, TalkPermittedUsers = talkPermittedUsers });
        if (lowHub != null)
        {
            rtsp.AddMount(new RtspMount { Path = "/" + cameraName + "/subStream", Hub = lowHub,
                PermittedUsers = permitted, GopPlayout = false, PlayoutDelayMs = 0, MatchHqTiming = timing,
                Talk = talk, TalkPermittedUsers = talkPermittedUsers });
            // Separate consumer policy, same source and authentication. Frigate can
            // retain its original latency without creating another camera pull/encoder.
            rtsp.AddMount(new RtspMount { Path = "/" + cameraName + "/subStream/direct", Hub = lowHub,
                PermittedUsers = permitted, GopPlayout = false, PlayoutDelayMs = 0,
                Talk = talk, TalkPermittedUsers = talkPermittedUsers });
        }
    }

    private static async Task RunLowQualityAsync(LowQualityVideoTranscoder transcoder, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await transcoder.RunAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch { Log.Warn("Low-quality conversion failed; retrying while the main stream continues"); }
            try { await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
        }
    }

    private static async Task RunCameraAsync(CameraService source, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await source.RunAsync(ct).ConfigureAwait(false); }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch { Log.Warn("Camera stream failed; reconnecting without changing camera settings"); }
            try { await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }
}
