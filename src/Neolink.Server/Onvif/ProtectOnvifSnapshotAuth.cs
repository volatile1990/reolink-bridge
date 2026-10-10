// HTTP Digest for snapshot and camera-event GETs which do not implement Basic challenges.
// Licensed under AGPL-3.0; see LICENSE.
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Neolink.Onvif;

/// <summary>Bounded, short-lived MD5/qop=auth HTTP Digest state for snapshots and event replay. SOAP and metrics do not use it.</summary>
internal sealed class SnapshotDigestAuthentication(IReadOnlyDictionary<string, string> users, TimeProvider clock)
{
    internal const string Realm = "reolink-bridge";
    internal const int MaxNonces = 256, MaxReplayTuples = 16, MaxAuthorizationLength = 2048;
    internal static readonly TimeSpan NonceLifetime = TimeSpan.FromSeconds(60);
    private readonly object _gate = new();
    private readonly Dictionary<string, Nonce> _nonces = new(StringComparer.Ordinal);
    private long _issuedSequence;

    private sealed class Nonce(long issued, long sequence)
    {
        internal long Issued { get; } = issued;
        internal long Sequence { get; } = sequence;
        internal Dictionary<(string User, string Cnonce), uint> Counts { get; } = new();
    }

    internal string Challenge()
    {
        string nonce;
        lock (_gate)
        {
            Purge(clock.GetTimestamp());
            while (_nonces.Count >= MaxNonces)
                _nonces.Remove(_nonces.Aggregate((oldest, next) => next.Value.Sequence < oldest.Value.Sequence ? next : oldest).Key);
            do { nonce = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant(); }
            while (_nonces.ContainsKey(nonce));
            _nonces.Add(nonce, new Nonce(clock.GetTimestamp(), ++_issuedSequence));
        }
        return $"Digest realm=\"{Realm}\", nonce=\"{nonce}\", algorithm=MD5, qop=\"auth\", charset=UTF-8";
    }

    internal bool Authenticate(string? authorization, string method, string requestTarget)
    {
        if (method != "GET" || !TryParse(authorization, out var fields)
            || !fields.TryGetValue("username", out var user) || user.Length is < 1 or > 256
            || !fields.TryGetValue("realm", out var realm) || realm != Realm
            || !fields.TryGetValue("nonce", out var nonce) || nonce.Length != 64 || !Hex(nonce)
            || !fields.TryGetValue("uri", out var uri) || uri != requestTarget
            || !fields.TryGetValue("response", out var response) || response.Length != 32 || !Hex(response)
            || !fields.TryGetValue("qop", out var qop) || qop != "auth"
            || !fields.TryGetValue("nc", out var nc) || nc.Length != 8 || !Hex(nc)
            || !uint.TryParse(nc, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var count) || count == 0
            || !fields.TryGetValue("cnonce", out var cnonce) || cnonce.Length is < 1 or > 128
            || (fields.TryGetValue("algorithm", out var algorithm) && !algorithm.Equals("MD5", StringComparison.OrdinalIgnoreCase))
            || !users.TryGetValue(user, out var password))
            return false;

        // Hash the exact request-target (including any escaping/query), never a normalized URI.
        string ha1 = Md5Hex($"{user}:{Realm}:{password}");
        string ha2 = Md5Hex($"GET:{requestTarget}");
        byte[] expected = MD5.HashData(Encoding.UTF8.GetBytes($"{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}"));
        if (!CryptographicOperations.FixedTimeEquals(expected, Convert.FromHexString(response))) return false;
        lock (_gate)
        {
            Purge(clock.GetTimestamp());
            if (!_nonces.TryGetValue(nonce, out var state)) return false;
            var tuple = (user, cnonce);
            if (state.Counts.TryGetValue(tuple, out uint previous))
            {
                if (count <= previous) return false;
            }
            else if (state.Counts.Count >= MaxReplayTuples) return false;
            // Invalid responses never consume counts; concurrent duplicates cannot both succeed.
            state.Counts[tuple] = count;
            return true;
        }
    }

    private void Purge(long now)
    {
        foreach (var entry in _nonces.ToArray())
        {
            var age = clock.GetElapsedTime(entry.Value.Issued, now);
            if (age < TimeSpan.Zero || age >= NonceLifetime) _nonces.Remove(entry.Key);
        }
    }

    private static string Md5Hex(string value) => Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static bool Hex(string value) => value.All(character => character is >= '0' and <= '9' or >= 'a' and <= 'f' or >= 'A' and <= 'F');
    private static bool Token(char character) => character is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9'
        || "!#$%&'*+-.^_`|~".Contains(character);

    // Parse quoted-string escapes and HTTP tokens, but reject duplicates, unknown directives,
    // controls, unclosed quotes, trailing junk and oversized input before allocating hash work.
    private static bool TryParse(string? authorization, out Dictionary<string, string> fields)
    {
        fields = new(StringComparer.OrdinalIgnoreCase);
        if (authorization == null || authorization.Length > MaxAuthorizationLength || authorization.Length < 8
            || !authorization.StartsWith("Digest", StringComparison.OrdinalIgnoreCase)
            || authorization[6] is not (' ' or '\t')) return false;
        int position = 6;
        void Whitespace() { while (position < authorization.Length && authorization[position] is ' ' or '\t') position++; }
        Whitespace();
        while (position < authorization.Length)
        {
            int start = position;
            while (position < authorization.Length && Token(authorization[position])) position++;
            if (position == start) return false;
            string key = authorization[start..position];
            if (key.ToLowerInvariant() is not ("username" or "realm" or "nonce" or "uri" or "response" or "algorithm" or "qop" or "nc" or "cnonce"))
                return false;
            Whitespace();
            if (position >= authorization.Length || authorization[position++] != '=') return false;
            Whitespace();
            if (position >= authorization.Length) return false;
            string value;
            if (authorization[position] == '"')
            {
                position++;
                var quoted = new StringBuilder();
                bool closed = false;
                while (position < authorization.Length)
                {
                    char character = authorization[position++];
                    if (character == '"') { closed = true; break; }
                    if (character == '\\')
                    {
                        if (position >= authorization.Length) return false;
                        character = authorization[position++];
                    }
                    if (character is < ' ' or > '~') return false;
                    quoted.Append(character);
                }
                if (!closed) return false;
                value = quoted.ToString();
            }
            else
            {
                start = position;
                while (position < authorization.Length && Token(authorization[position])) position++;
                if (position == start) return false;
                value = authorization[start..position];
            }
            if (!fields.TryAdd(key, value) || fields.Count > 9) return false;
            Whitespace();
            if (position == authorization.Length) return true;
            if (authorization[position++] != ',') return false;
            Whitespace();
            if (position == authorization.Length) return false;
        }
        return false;
    }
}
