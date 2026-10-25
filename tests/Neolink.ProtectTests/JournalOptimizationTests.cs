using System.Reflection;
using System.Text;
using System.Text.Json;
using Neolink.Onvif;

internal static class JournalOptimizationTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 25, 12, 0, 0, TimeSpan.Zero);
    private const string Instance = "00112233445566778899aabbccddeeff";
    private static void Check(bool condition, string message) { if (!condition) throw new Exception(message); }

    public static Task CanonicalRestartAndMutationParity()
    {
        using var pair = new Pair();
        var first = new ProtectAlarmPush(1, Now, true, ["person"], ["people"], "MD\u4e2d<>&", 1);
        pair.Append(first, Now);
        pair.Append(new(2, Now.AddSeconds(1), false, ["vehicle"], ["car"], "none", 1), Now.AddSeconds(1));
        pair.SetEpoch(2);
        // Existing internal callers may mutate an array returned by Entries.
        // Cached UTF8 must not freeze older values after such a mutation.
        first.Classes[0] = "animal"; first.RawAiTypes[0] = "dog_cat";
        pair.SetEpoch(3);
        Check(Encoding.UTF8.GetString(ReadBytes(pair.CurrentPath)).Contains("dog_cat", StringComparison.Ordinal), "mutated raw type stayed stale in cache");
        first.Classes[0] = "person"; first.RawAiTypes[0] = "people";
        pair.SetEpoch(4);
        pair.Restart(partialTail: true);
        pair.Append(new(3, Now.AddSeconds(2), true, ["animal"], ["dog_cat"], "MD", pair.Current.Epoch), Now.AddSeconds(2));
        pair.SetEpoch(5);
        Check(pair.Current.InstanceId == Instance && pair.Current.Sequence == 3 && pair.Current.Epoch == 5,
            "cache changed durable identity, sequence or epoch");
        return Task.CompletedTask;
    }

    public static Task PhysicalRetentionByteAccountingAndCacheCap()
    {
        string seed = Header() + string.Concat(Enumerable.Range(1, 700).Select(i =>
            "{\"Sequence\":" + i + ",\"At\":\"2026-10-25T12:00:00+00:00\",\"Classes\":[],\"RawAiTypes\":[],\"SourceStatus\":\"\"}\n"));
        using var pair = new Pair(seed, 65536);
        var retained = pair.Current.Entries(Now);
        long canonicalBytes = retained.Sum(p => JsonSerializer.SerializeToUtf8Bytes(p).LongLength + 1);
        long counted = JsonSerializer.SerializeToElement(pair.Current.Diagnostics()).GetProperty("retainedBytes").GetInt64();
        Check(canonicalBytes != counted, "noncanonical fixture did not exercise original file-byte accounting");
        Check(pair.Current.SerializationCacheCharge <= 65536 && pair.Current.SerializedCacheBytes <= 65536,
            "canonical cache exceeds its separate byte cap");
        Check(pair.Current.SerializedCacheBytes > 0 && pair.Current.SerializedCacheBytes < canonicalBytes,
            "fixture did not exercise both cached and uncached entries");
        pair.SetEpoch(2);
        pair.Append(new(701, Now, true, ["person"], ["people"], "MD", 2), Now);
        pair.Entries(Now.AddMinutes(1)); // equality at the retention edge must remain retained
        Check(pair.Current.Entries(Now.AddMinutes(1)).Length > 0, "strict retention boundary became inclusive");
        pair.Entries(Now.AddMinutes(1).AddTicks(1));
        Check(pair.Current.Entries(Now.AddMinutes(1).AddTicks(1)).Length == 0
            && pair.Current.Sequence == 701 && pair.Current.SerializedCacheBytes == 0 && pair.Current.SerializationCacheCharge == 0,
            "physical retention changed the cursor or retained expired serialized arrays");
        Check(ReadLines(pair.CurrentPath).Length == 1, "expired records remained physically in the journal");
        return Task.CompletedTask;
    }

    public static Task DurableFailureAndRecoveryParity()
    {
        using var pair = new Pair(syncProbes: true);
        pair.Append(new(1, Now, true, ["person"], [], "MD", 1), Now);
        pair.SetEpoch(2);
        pair.FailDirectorySync = true;
        pair.SetEpoch(3);
        Check(pair.Current.State == "write-failed" && pair.Current.Failures == 1,
            "failed directory sync was reported as durable");
        pair.FailDirectorySync = false;
        pair.Append(new(2, Now, false, [], [], "none", 3), Now);
        Check(pair.Current.State == "ready" && pair.SyncsOld == pair.SyncsCurrent,
            "retry after rename/sync failure lost the append or changed sync ordering");
        byte[] before = ReadBytes(pair.CurrentPath);
        PrivateDirectory(pair.OldPath + ".new"); PrivateDirectory(pair.CurrentPath + ".new");
        pair.SetEpoch(4); // a directory blocks the replacement file before rename
        Check(pair.Current.State == "write-failed" && pair.Current.Failures == 2
            && ReadBytes(pair.CurrentPath).SequenceEqual(before), "failed temporary write changed the published journal");
        Directory.Delete(pair.OldPath + ".new"); Directory.Delete(pair.CurrentPath + ".new");
        pair.Append(new(3, Now, true, ["vehicle"], ["car"], "MD", 4), Now);
        pair.Restart();
        Check(pair.Current.State == "ready" && pair.Current.Sequence == 3,
            "replacement failure changed restart/lease recovery semantics");
        return Task.CompletedTask;
    }

    public static Task NativeFileFlagsAndOwnedCache()
    {
        using var pair = new Pair();
        var open = typeof(ProtectReplayJournal).GetMethod("Open", BindingFlags.Instance | BindingFlags.NonPublic)!;
        using var temporary = (FileStream)open.Invoke(pair.Current, [pair.CurrentPath + ".probe", FileMode.Create, true])!;
        using var append = (FileStream)open.Invoke(pair.Current, [pair.CurrentPath + ".append-probe", FileMode.Append, false])!;
        if (OperatingSystem.IsLinux())
        {
            long Flags(FileStream f)
            {
                string value = File.ReadAllLines("/proc/self/fdinfo/" + f.SafeFileHandle.DangerousGetHandle().ToInt64())
                    .Single(line => line.StartsWith("flags:", StringComparison.Ordinal)).Split(':')[1].Trim();
                return Convert.ToInt64(value, 8);
            }
            const long oSync = 0x101000;
            Check((Flags(temporary) & oSync) == 0 && (Flags(append) & oSync) == oSync,
                "temporary and append native sync flags do not preserve their distinct contracts");
        }
        pair.Append(new(1, Now, true, ["person"], ["people"], "MD", 1), Now);
        var entries = typeof(ProtectReplayJournal).GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(pair.Current)!;
        object entry = ((System.Collections.IEnumerable)entries).Cast<object>().Single();
        var dataField = entry.GetType().GetField("Utf8")!;
        var cached = (byte[])dataField.GetValue(entry)!;
        pair.SetEpoch(2);
        Check(ReferenceEquals(cached, dataField.GetValue(entry)), "unchanged compaction reserialized a retained entry");
        byte[] file = ReadBytes(pair.CurrentPath);
        Check(!ReferenceEquals(cached, file) && cached.SequenceEqual(JsonSerializer.SerializeToUtf8Bytes(pair.Current.Entries(Now)[0])),
            "cache retains a complete file view or altered canonical bytes");
        return Task.CompletedTask;
    }

    private static string Header() => "{\"Schema\":1,\"InstanceId\":\"" + Instance + "\",\"Sequence\":0,\"Epoch\":0}\n";
    private static byte[] ReadBytes(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var copy = new MemoryStream(); stream.CopyTo(copy); return copy.ToArray();
    }
    private static string[] ReadLines(string path) => Encoding.UTF8.GetString(ReadBytes(path)).Split('\n', StringSplitOptions.RemoveEmptyEntries);
    private static void PrivateDirectory(string path)
    {
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(path);
        else Directory.CreateDirectory(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    private sealed class Pair : IDisposable
    {
        private readonly string _folder;
        private readonly int _maxBytes;
        private readonly bool _syncProbes;
        public readonly string OldPath, CurrentPath;
        public Phase1ReplayJournal Old = null!;
        public ProtectReplayJournal Current = null!;
        public bool FailDirectorySync;
        public int SyncsOld, SyncsCurrent;

        public Pair(string? seed = null, int maxBytes = 131072, bool syncProbes = false)
        {
            _maxBytes = maxBytes; _syncProbes = syncProbes;
            _folder = Path.Combine(Path.GetTempPath(), "synthetic-journal-optimization-" + Guid.NewGuid().ToString("N"));
            PrivateDirectory(_folder);
            OldPath = Path.Combine(_folder, "old.jsonl"); CurrentPath = Path.Combine(_folder, "current.jsonl");
            foreach (string path in new[] { OldPath, CurrentPath })
            {
                File.WriteAllText(path, seed ?? Header(), new UTF8Encoding(false));
                if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
            try { Open(); Compare(Now); }
            catch { Dispose(); throw; }
        }

        private Action<string>? Sync(string path, bool current) => !_syncProbes ? null : directory =>
        {
            Check(Path.GetFullPath(directory) == Path.GetFullPath(_folder) && File.Exists(path) && !File.Exists(path + ".new"),
                "directory sync preceded rename or escaped private scope");
            Check(ReadLines(path).All(line => JsonSerializer.Deserialize<JsonElement>(line).ValueKind == JsonValueKind.Object),
                "directory sync observed an incomplete file");
            if (current) SyncsCurrent++; else SyncsOld++;
            if (FailDirectorySync) throw new IOException("synthetic directory-sync failure");
        };
        private void Open()
        {
            Old = new(OldPath, _maxBytes, TimeSpan.FromMinutes(1), Now, Sync(OldPath, false));
            Current = new(CurrentPath, _maxBytes, TimeSpan.FromMinutes(1), Now, Sync(CurrentPath, true));
        }
        public void Append(ProtectAlarmPush push, DateTimeOffset now)
        { Old.Append(push, now); Current.Append(push, now); Compare(now); }
        public void SetEpoch(long epoch) { Old.SetEpoch(epoch); Current.SetEpoch(epoch); Compare(Now); }
        public void Entries(DateTimeOffset now) { Old.Entries(now); Current.Entries(now); Compare(now); }
        public void Restart(bool partialTail = false)
        {
            Old.Dispose(); Current.Dispose();
            if (partialTail) { File.AppendAllText(OldPath, "{incomplete"); File.AppendAllText(CurrentPath, "{incomplete"); }
            Open(); Compare(Now);
        }
        private void Compare(DateTimeOffset now)
        {
            Check(Old.InstanceId == Current.InstanceId && Old.Sequence == Current.Sequence && Old.Epoch == Current.Epoch,
                "identity/cursor/epoch differ from frozen phase1 journal");
            Check(JsonSerializer.Serialize(Old.Entries(now)) == JsonSerializer.Serialize(Current.Entries(now)),
                "logical replay differs from frozen phase1 journal");
            Check(JsonSerializer.Serialize(Old.Diagnostics()) == JsonSerializer.Serialize(Current.Diagnostics()),
                "diagnostics/byte accounting differs from frozen phase1 journal");
            Check(ReadBytes(OldPath).SequenceEqual(ReadBytes(CurrentPath)),
                "physical canonical journal differs from frozen phase1 journal");
            Check(Current.SerializationCacheCharge <= _maxBytes && Current.SerializedCacheBytes <= _maxBytes,
                "retained serialization cache grew beyond its separate budget");
        }
        public void Dispose()
        {
            Old?.Dispose(); Current?.Dispose();
            Check(Path.GetFullPath(_folder).StartsWith(Path.Combine(Path.GetTempPath(), "synthetic-journal-optimization-"), StringComparison.Ordinal),
                "refusing cleanup outside journal optimization fixture");
            if (Directory.Exists(_folder)) Directory.Delete(_folder, recursive: true);
        }
    }
}
