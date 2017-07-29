using Fugu.Common;
using System;
using StoreIndex = Fugu.Common.AaTree<Fugu.IndexEntry>;

namespace Fugu
{
    public sealed class Snapshot : IDisposable
    {
        private readonly StoreIndex _index;
        private readonly Action<Snapshot> _onDisposed;
        private bool _disposed = false;

        public Snapshot(StateVector clock, StoreIndex index, Action<Snapshot> onDisposed)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(onDisposed, nameof(onDisposed));

            Clock = clock;
            _index = index;
            _onDisposed = onDisposed;
        }

        public StateVector Clock { get; }

        public byte[] TryGetValue(byte[] key)
        {
            Guard.NotNull(key, nameof(key));
            ThrowIfDisposed();

            if (!_index.TryGetValue(key, out var indexEntry))
            {
                // Index contains no entry for that key
                return null;
            }

            IndexEntry.Value valueIndexEntry = indexEntry as IndexEntry.Value;
            if (valueIndexEntry == null)
            {
                // Index contains an entry for that key, but it's a tombstone
                return null;
            }

            using (var reader = valueIndexEntry.Segment.Table.GetReader(valueIndexEntry.Offset, valueIndexEntry.ValueLength))
            {
                return reader.ReadBytes(valueIndexEntry.ValueLength);
            }
        }

        #region IDisposable

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                _onDisposed(this);
            }
        }

        #endregion

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(Snapshot));
            }
        }
    }
}
