using Fugu.Common;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Fugu
{
    public class WriteBatch
    {
        private readonly ConcurrentDictionary<byte[], WriteBatchItem> _changes =
            new ConcurrentDictionary<byte[], WriteBatchItem>(new ByteArrayEqualityComparer());

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteBatch"/> class.
        /// </summary>
        public WriteBatch()
        {
        }

        public IReadOnlyDictionary<byte[], WriteBatchItem> Changes => _changes;

        public void Put(byte[] key, byte[] value)
        {
            Guard.NotNull(key, nameof(key));
            Guard.NotNull(value, nameof(value));

            _changes[key] = new WriteBatchItem.Put(value);
        }

        public void Delete(byte[] key)
        {
            Guard.NotNull(key, nameof(key));

            _changes[key] = new WriteBatchItem.Delete();
        }
    }
}
