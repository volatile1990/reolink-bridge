using System.Diagnostics;
using System.Threading.Channels;

namespace Neolink.Streaming;

public sealed class QueueBudgetExceededException() : IOException("Media queue exceeded its byte or packet budget");
public readonly record struct ByteQueueDiagnostics(long Bytes, long PeakBytes, int Items, int PeakItems,
    double OldestAgeMs, long Overflows);

/// <summary>Preserves every queued byte. Overflow faults the stream instead of dropping arbitrary media chunks.</summary>
internal sealed class ByteBoundedChannel<T> : Channel<T>
{
    private readonly object _gate = new();
    private readonly Func<T, int> _size;
    private readonly long _maxBytes;
    private readonly int _maxItems;
    private readonly Channel<Entry> _inner;
    private long _bytes, _peakBytes, _overflows;
    private int _items, _peakItems;
    private bool _closed;
    private sealed record Entry(T Value, int Bytes, long At);

    public ByteBoundedChannel(long maxBytes, int maxItems, Func<T, int> size)
    {
        if (maxBytes < 1 || maxItems < 1) throw new ArgumentOutOfRangeException(nameof(maxBytes));
        _maxBytes = maxBytes; _maxItems = maxItems; _size = size;
        _inner = Channel.CreateBounded<Entry>(new BoundedChannelOptions(maxItems)
        { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });
        Reader = new BudgetReader(this); Writer = new BudgetWriter(this);
    }

    public ByteQueueDiagnostics Diagnostics()
    {
        lock (_gate)
            return new(_bytes, _peakBytes, _items, _peakItems,
                _inner.Reader.TryPeek(out var first) ? Stopwatch.GetElapsedTime(first.At).TotalMilliseconds : 0, _overflows);
    }

    private bool Put(T item)
    {
        int bytes = _size(item);
        if (bytes < 0) throw new ArgumentOutOfRangeException(nameof(item));
        lock (_gate)
        {
            if (_closed) return false;
            if (bytes > _maxBytes - _bytes || _items >= _maxItems)
            {
                _closed = true; _overflows++;
                _inner.Writer.TryComplete(new QueueBudgetExceededException());
                return false;
            }
            _bytes += bytes; _items++;
            _peakBytes = Math.Max(_peakBytes, _bytes); _peakItems = Math.Max(_peakItems, _items);
            if (_inner.Writer.TryWrite(new(item, bytes, Stopwatch.GetTimestamp()))) return true;
            _bytes -= bytes; _items--;
            return false;
        }
    }

    private bool Take(out T item)
    {
        lock (_gate)
        {
            if (_inner.Reader.TryRead(out var entry))
            {
                _bytes -= entry.Bytes; _items--; item = entry.Value; return true;
            }
            item = default!; return false;
        }
    }

    private sealed class BudgetReader(ByteBoundedChannel<T> owner) : ChannelReader<T>
    {
        public override Task Completion => owner._inner.Reader.Completion;
        public override bool CanCount => true;
        public override int Count { get { lock (owner._gate) return owner._items; } }
        public override bool TryRead(out T item) => owner.Take(out item);
        public override ValueTask<bool> WaitToReadAsync(CancellationToken ct = default) => owner._inner.Reader.WaitToReadAsync(ct);
    }

    private sealed class BudgetWriter(ByteBoundedChannel<T> owner) : ChannelWriter<T>
    {
        public override bool TryWrite(T item) => owner.Put(item);
        public override ValueTask WriteAsync(T item, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            return TryWrite(item) ? ValueTask.CompletedTask : ValueTask.FromException(new QueueBudgetExceededException());
        }
        public override bool TryComplete(Exception? error = null)
        {
            lock (owner._gate) { owner._closed = true; return owner._inner.Writer.TryComplete(error); }
        }
        public override ValueTask<bool> WaitToWriteAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            lock (owner._gate) return ValueTask.FromResult(!owner._closed);
        }
    }
}
