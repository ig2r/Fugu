using Fugu.Common;
using System;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu
{
    public sealed class Snapshot : IDisposable
    {
        private readonly StateVector _clock;
        private readonly Index _index;
        private readonly Action<Snapshot> _onDisposed;
        private bool _disposed = false;

        public Snapshot(StateVector clock, Index index, Action<Snapshot> onDisposed)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(onDisposed, nameof(onDisposed));

            _clock = clock;
            _index = index;
            _onDisposed = onDisposed;
        }

        public async Task<byte[]> TryGetValueAsync(byte[] key)
        {
            Guard.NotNull(key, nameof(key));
            ThrowIfDisposed();

            IndexEntry indexEntry;
            if (!_index.TryGetValue(key, out indexEntry))
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

            var buffer = new byte[valueIndexEntry.ValueLength];

            using (var inputStream = valueIndexEntry.Segment.Table.GetInputStream(valueIndexEntry.Offset, valueIndexEntry.ValueLength))
            {
                int bytesRead = 0;
                while (bytesRead < valueIndexEntry.ValueLength)
                {
                    int n = await inputStream.ReadAsync(buffer, bytesRead, valueIndexEntry.ValueLength - bytesRead).ConfigureAwait(false);

                    if (n <= 0)
                    {
                        throw new InvalidOperationException("Failed to read value from input stream.");
                    }

                    bytesRead += n;
                }
            }

            return buffer;
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
