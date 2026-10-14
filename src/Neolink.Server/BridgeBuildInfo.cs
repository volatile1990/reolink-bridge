using System.Reflection;

namespace Neolink;

/// <summary>Runtime provenance, without paths, configuration or camera identities.</summary>
public static class BridgeBuildInfo
{
    public static string Version { get; } = typeof(BridgeBuildInfo).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
    public static object Diagnostics() => new { version = Version,
        runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription };
}
