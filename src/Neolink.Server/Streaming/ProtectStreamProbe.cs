// Bounded read-only substream metadata probe; AGPL-3.0.
using System.Text.Json;
using Neolink.Config;
using Neolink.Protocol;

namespace Neolink.Streaming;

/// <summary>Reads actual substream headers without serving another ONVIF device or changing camera settings.</summary>
public static class ProtectStreamProbe
{
    public static async Task<int> RunAsync(NeolinkConfig config)
    {
        var camera = config.Cameras.Single();
        var hub = new StreamHub(camera.Name + "/sub-probe");
        var source = new CameraService(camera, StreamKind.Sub, hub, TimeSpan.Zero) { RelayOnly = true };
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(35));
        Log.Level = LogLevel.Error;
        var task = source.RunAsync(stop.Token);
        bool ready = false;
        object? metadata = null;
        try
        {
            while (!stop.IsCancellationRequested && !task.IsCompleted)
            {
                if (hub.VideoReady && hub.LiveVideo && hub.GetVideoDiagnostics().IncomingFrames >= 5)
                {
                    ready = true;
                    long framesBefore = hub.GetVideoDiagnostics().IncomingFrames;
                    long sampleAt = System.Diagnostics.Stopwatch.GetTimestamp();
                    await Task.Delay(2500, stop.Token).ConfigureAwait(false);
                    double sampleSeconds = System.Diagnostics.Stopwatch.GetElapsedTime(sampleAt).TotalSeconds;
                    double measuredFps = (hub.GetVideoDiagnostics().IncomingFrames - framesBefore) / sampleSeconds;
                    metadata = new { ok = true, stream = "subStream", codec = hub.Codec?.ToString(),
                        width = hub.Width, height = hub.Height, incomingFrames = hub.GetVideoDiagnostics().IncomingFrames,
                        measuredFps = Math.Round(measuredFps, 2),
                        authenticationFailed = hub.AuthenticationFailed };
                    break;
                }
                await Task.Delay(100, stop.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            stop.Cancel();
            try { await task.ConfigureAwait(false); } catch { }
        }
        Console.WriteLine(JsonSerializer.Serialize(metadata ?? new
        {
            ok = ready,
            stream = "subStream",
            codec = hub.Codec?.ToString(),
            width = hub.Width,
            height = hub.Height,
            incomingFrames = hub.GetVideoDiagnostics().IncomingFrames,
            authenticationFailed = hub.AuthenticationFailed
        }));
        return ready ? 0 : 1;
    }
}
