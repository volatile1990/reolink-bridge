using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Neolink.Onvif;

/// <summary>Opt-in, bounded, private camera-push journal. It stores no credentials, images or synthetic events.</summary>
internal sealed class ProtectReplayJournal : IDisposable
{
    private sealed record Header(int Schema, string InstanceId, long Sequence, long Epoch);
    private readonly string _path;
    private readonly int _maxBytes;
    private readonly TimeSpan _retention;
    private readonly Action<string> _syncDirectory;
    private FileStream? _stream;
    private readonly FileStream _lease;
    private Header _header;
    private readonly Queue<(ProtectAlarmPush Push, int Bytes)> _entries = new();
    private long _bytes;
    private bool _disposed;
    public string InstanceId => _header.InstanceId;
    public long Sequence => _header.Sequence;
    public long Epoch => _header.Epoch;
    public string State { get; private set; } = "ready";
    public long Failures { get; private set; }

    public ProtectReplayJournal(string path, int maxBytes, TimeSpan retention, DateTimeOffset now,
        Action<string>? syncDirectory = null)
    {
        if (!Path.IsPathFullyQualified(path) || maxBytes is < 65536 or > 67108864 || retention.TotalSeconds is < 60 or > 604800)
            throw new ArgumentException("Invalid persistent replay limits");
        _path = Path.GetFullPath(path); _maxBytes = maxBytes; _retention = retention;
        _syncDirectory = syncDirectory ?? SyncDirectory;
        RejectLinks(_path);
        string directory = Path.GetDirectoryName(_path)!;
        if (!Directory.Exists(directory))
        {
            if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
            else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
        CheckPrivate(directory);
        string lockPath = _path + ".lock";
        RejectLinks(lockPath);
        if (File.Exists(lockPath)) CheckPrivate(lockPath);
        var leaseOptions = new FileStreamOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None };
        if (!OperatingSystem.IsWindows()) leaseOptions.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        _lease = new FileStream(lockPath, leaseOptions);
        _header = new(1, Guid.NewGuid().ToString("N"), 0, 0);
        try
        {
            if (File.Exists(_path)) Load();
            // A process restart ends the prior publishing session even when no new camera push follows.
            _header = _header with { Epoch = checked(_header.Epoch + 1) };
            Trim(now); Compact();
        }
        catch { _stream?.Dispose(); _lease.Dispose(); throw; }
    }

    private static void RejectLinks(string path)
    {
        for (string? current = path; current != null; current = Path.GetDirectoryName(current))
            if ((File.Exists(current) || Directory.Exists(current)) && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                throw new IOException("Persistent replay path contains a link");
    }

    private static void CheckPrivate(string path)
    {
        if (!OperatingSystem.IsWindows() && (File.GetUnixFileMode(path) &
            (UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
             UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute)) != 0)
            throw new IOException("Persistent replay file and parent directory must be private");
    }

    private void Load()
    {
        CheckPrivate(_path);
        if (new FileInfo(_path).Length > _maxBytes + 65536L) throw new IOException("Persistent replay exceeds its byte budget");
        byte[] content = File.ReadAllBytes(_path);
        int lineStart = 0, newline = Array.IndexOf(content, (byte)'\n');
        if (newline < 0) throw new InvalidDataException("Persistent replay header is incomplete");
        var header = JsonSerializer.Deserialize<Header>(content.AsSpan(0, newline));
        if (header == null || header.Schema != 1 || !System.Text.RegularExpressions.Regex.IsMatch(header.InstanceId, "^[a-f0-9]{32}$")
            || header.Sequence < 0 || header.Epoch < 0) throw new InvalidDataException("Persistent replay header is invalid");
        _header = header;
        lineStart = newline + 1;
        long previous = 0;
        while ((newline = Array.IndexOf(content, (byte)'\n', lineStart)) >= 0)
        {
            int length = newline - lineStart;
            if (length is < 1 or > 16384) throw new InvalidDataException("Persistent replay entry is invalid");
            var push = JsonSerializer.Deserialize<ProtectAlarmPush>(content.AsSpan(lineStart, length));
            if (push == null || push.Sequence <= previous || push.Sequence < 1 || push.ConnectionEpoch < 0
                || push.Classes == null || push.RawAiTypes == null || push.Classes.Length > 3 || push.RawAiTypes.Length > 16
                || push.Classes.Any(c => c is not ("person" or "vehicle" or "animal"))
                || push.RawAiTypes.Any(c => c == null || c.Length > 64 || c.Any(char.IsControl))
                || push.SourceStatus == null || push.SourceStatus.Length > 64 || push.SourceStatus.Any(char.IsControl))
                throw new InvalidDataException("Persistent replay entry violates the camera-push contract");
            _entries.Enqueue((push, length + 1)); _bytes += length + 1;
            previous = push.Sequence;
            _header = _header with { Sequence = Math.Max(_header.Sequence, push.Sequence), Epoch = Math.Max(_header.Epoch, push.ConnectionEpoch) };
            lineStart = newline + 1;
        }
        // An incomplete last append is discarded; every complete previous record remains available.
    }

    private FileStream Open(string path, FileMode mode)
    {
        var options = new FileStreamOptions { Mode = mode, Access = FileAccess.Write, Share = FileShare.Read,
            Options = FileOptions.WriteThrough };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private void Compact()
    {
        string temporary = _path + ".new";
        RejectLinks(_path); RejectLinks(temporary);
        if (File.Exists(temporary)) CheckPrivate(temporary);
        using (var output = Open(temporary, FileMode.Create))
        {
            WriteLine(output, JsonSerializer.SerializeToUtf8Bytes(_header));
            foreach (var entry in _entries) WriteLine(output, JsonSerializer.SerializeToUtf8Bytes(entry.Push));
            output.Flush(flushToDisk: true);
        }
        _stream?.Dispose(); _stream = null;
        File.Move(temporary, _path, overwrite: true);
        // File flush protects contents, not the new directory entry. Only
        // resume appends once the rename itself is durable on the NAS.
        _syncDirectory(Path.GetDirectoryName(_path)!);
        _stream = Open(_path, FileMode.Append);
    }

    private static void SyncDirectory(string directory)
    {
        // Production is Linux. Windows FlushFileBuffers does not support an
        // ordinary directory handle; retain its existing file-flush behavior.
        if (!OperatingSystem.IsLinux()) return;
        const int oDirectory = 0x10000, oCloexec = 0x80000;
        int descriptor = NativeOpen(directory, oDirectory | oCloexec);
        if (descriptor < 0)
            throw new IOException("Persistent replay directory could not be opened for sync",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        try
        {
            if (NativeFsync(descriptor) != 0)
                throw new IOException("Persistent replay directory sync failed",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        finally { NativeClose(descriptor); }
    }

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int NativeOpen([MarshalAs(UnmanagedType.LPUTF8Str)] string path, int flags);
    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int NativeFsync(int descriptor);
    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int NativeClose(int descriptor);

    private static void WriteLine(Stream stream, byte[] data) { stream.Write(data); stream.WriteByte((byte)'\n'); }

    private bool Trim(DateTimeOffset now)
    {
        bool changed = false;
        while (_entries.TryPeek(out var first) && (_bytes > _maxBytes - 1024 || first.Push.At < now - _retention))
        { _entries.Dequeue(); _bytes -= first.Bytes; changed = true; }
        return changed;
    }

    public void Append(ProtectAlarmPush push, DateTimeOffset now)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _header = _header with { Sequence = push.Sequence, Epoch = push.ConnectionEpoch };
        byte[] data = JsonSerializer.SerializeToUtf8Bytes(push);
        _entries.Enqueue((push, data.Length + 1)); _bytes += data.Length + 1;
        try
        {
            if (Trim(now) || _stream == null) Compact();
            else { WriteLine(_stream, data); _stream.Flush(flushToDisk: true); }
            State = "ready";
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        { State = "write-failed"; Failures++; _stream?.Dispose(); _stream = null; }
    }

    public ProtectAlarmPush[] Entries(DateTimeOffset now)
    {
        if (Trim(now) && !_disposed)
        {
            try { Compact(); State = "ready"; }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            { State = "write-failed"; Failures++; _stream?.Dispose(); _stream = null; }
        }
        return _entries.Select(e => e.Push).ToArray();
    }

    public object Diagnostics() => new { enabled = true, state = State, failures = Failures,
        retainedPushes = _entries.Count, retainedBytes = _bytes, maxBytes = _maxBytes,
        retentionSeconds = _retention.TotalSeconds };
    public void SetEpoch(long epoch)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _header = _header with { Epoch = epoch };
        try { Compact(); State = "ready"; }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        { State = "write-failed"; Failures++; _stream?.Dispose(); _stream = null; }
    }
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true; State = "stopped"; _stream?.Dispose(); _stream = null; _lease.Dispose();
    }
}
