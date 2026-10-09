// Reolink Bridge headless lifecycle; AGPL-3.0.
using Neolink.Config;
using Neolink.Media;
using Neolink.Onvif;
using Neolink.Protocol;
using Neolink.Rtsp;

namespace Neolink.Streaming;

/// <summary>Minimal Protect bridge: one camera login/stream, local RTSP and ONVIF only.
/// Does not initialize camera controls, UI, state, recording, MQTT, notifications or discovery probes.</summary>
public static class ProtectBridgeHost
{
    public static async Task<int> RunAsync(NeolinkConfig config)
    {
        var onvif = config.Onvif ?? throw new ArgumentException("ONVIF configuration missing");
        var camera = config.Cameras.Single();
        using var shutdown = new CancellationTokenSource();
        ConsoleCancelEventHandler cancel = (_, e) => { e.Cancel = true; shutdown.Cancel(); };
        EventHandler exit = (_, _) => { try { shutdown.Cancel(); } catch (ObjectDisposedException) { } };
        Console.CancelKeyPress += cancel;
        AppDomain.CurrentDomain.ProcessExit += exit;
        var users = config.Users.ToDictionary(u => u.Name, u => u.Pass, StringComparer.Ordinal);
        var permitted = users.Keys.ToHashSet(StringComparer.Ordinal);
        var hub = new StreamHub(camera.Name);
        var source = new CameraService(camera, StreamKind.Main, hub, TimeSpan.Zero) { RelayOnly = true };
        var path = "/" + camera.Name + "/mainStream";
        var rtsp = new RtspServer(users) { TcpOnly = true };
        rtsp.AddMount(new RtspMount { Path = path, Hub = hub, PermittedUsers = permitted });
        rtsp.AddMount(new RtspMount { Path = "/" + camera.Name, Hub = hub, PermittedUsers = permitted });
        var endpoint = new ProtectOnvifServer(onvif, users, [new ProtectOnvifStream("main", path, hub)], config.BindPort);
        var tasks = new[]
        {
            RunCameraAsync(source, shutdown.Token),
            rtsp.RunAsync(config.BindAddr, config.BindPort, shutdown.Token),
            endpoint.RunAsync(shutdown.Token)
        };
        Log.Info("Reolink Bridge starting: one upstream stream, TCP RTSP, read-only ONVIF; camera settings are unchanged");
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
            foreach (var task in tasks)
                try { await task.ConfigureAwait(false); } catch (OperationCanceledException) { } catch { }
            Console.CancelKeyPress -= cancel;
            AppDomain.CurrentDomain.ProcessExit -= exit;
        }
        return result;
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
