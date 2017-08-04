using Fugu.Common;
using System;
using System.Collections.Generic;
using StoreIndex = Fugu.Common.AaTree<Fugu.IndexEntry>;

namespace Fugu
{
    /// <summary>
    /// An immutable snapshot of the contents of a <see cref="KeyValueStore"/> instance.
    /// </summary>
    public sealed class Snapshot : IDisposable
    {
        private readonly StoreIndex _index;
        private readonly Action<Snapshot> _onDisposed;
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Snapshot"/> class.
        /// </summary>
        /// <param name="clock">Clock vector representing the snapshotted state of the store.</param>
        /// <param name="index">Index to the store contents.</param>
        /// <param name="onDisposed">Callback to invoke when this snapshot is disposed of.</param>
        public Snapshot(StateVector clock, StoreIndex index, Action<Snapshot> onDisposed)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(onDisposed, nameof(onDisposed));

            Clock = clock;
            _index = index;
            _onDisposed = onDisposed;
        }

        /// <summary>
        /// Gets the state vector associated with this snapshot.
        /// </summary>
        public StateVector Clock { get; }

        /// <summary>
        /// Gets the value corresponding to a given key, or throws a <see cref="KeyNotFoundException"/> if the given key
        /// is not contained in the snapshot.
        /// </summary>
        /// <param name="key">The key for which to retrieve the value.</param>
        /// <returns>The value associated with the given key.</returns>
        public ReadOnlySpan<byte> this[byte[] key]
        {
            get
            {
                if (!TryGetValue(key, out var span))
                {
                    throw new KeyNotFoundException();
                }

                return span;
            }
        }

        /// <summary>
        /// Attempts to retrieve the value corresponding to a given key if it is contained in the snapshot.
        /// </summary>
        /// <param name="key">The key for which to retrieve the value.</param>
        /// <param name="value">Output parameter that will hold the retrieved key if successful.</param>
        /// <returns>A value indicating whether the given key was present in the snapshot.</returns>
        public bool TryGetValue(byte[] key, out ReadOnlySpan<byte> value)
        {
            Guard.NotNull(key, nameof(key));
            ThrowIfDisposed();

            if (!_index.TryGetValue(key, out var indexEntry) || !(indexEntry is IndexEntry.Value valueIndexEntry))
            {
                // Index contains no entry for that key, or it's a tombstone
                value = default(Span<byte>);
                return false;
            }

            value = valueIndexEntry.Segment.Table.GetSpan(valueIndexEntry.Offset).Slice(0, valueIndexEntry.ValueLength);
            return true;
        }

        #region IDisposable

        /// <summary>
        /// Releases the current snapshot and allows associated resources to be cleaned up.
        /// </summary>
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
