using Fugu.Common;
using System;
using System.Threading.Tasks;

namespace Fugu
{
    using Index = CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry>;

    public class Snapshot : IDisposable
    {
        private readonly Index _index;
        private readonly Action<Snapshot> _onDisposed;

        public Snapshot(Index index, VectorClock clock, Action<Snapshot> onDisposed)
        {
            _index = index;
            Clock = clock;
            _onDisposed = onDisposed;
        }

        public VectorClock Clock { get; }

        public async Task<byte[]> TryGetValueAsync(byte[] key)
        {
            IndexEntry indexEntry;
            if (!_index.TryGetValue(key, out indexEntry))
            {
                return null;
            }

            IndexEntry.Value valueIndexEntry = indexEntry as IndexEntry.Value;
            if (valueIndexEntry == null)
            {
                return null;
            }

            var buffer = new byte[valueIndexEntry.ValueLength];

            using (var inputStream = valueIndexEntry.Segment.Table.GetInputStream(valueIndexEntry.Offset, valueIndexEntry.Size))
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
            Dispose(true);
        }

        #endregion

        protected virtual void Dispose(bool disposing)
        {
            _onDisposed(this);
        }
    }
}
