using Fugu.Common;
using System;
using System.Collections.Generic;

namespace Fugu
{
    /// <summary>
    /// Gathers put and delete operations against a store that will be committed as a group.
    /// </summary>
    public class WriteBatch
    {
        private readonly Dictionary<byte[], WriteBatchItem> _changes =
            new Dictionary<byte[], WriteBatchItem>(new ByteArrayEqualityComparer());

        /// <summary>
        /// Gets the set of pending operations contained in this instance.
        /// </summary>
        public IReadOnlyDictionary<byte[], WriteBatchItem> Changes
        {
            get => _changes;
        }

        /// <summary>
        /// Registers the given key/value pair to be written to the store upon committing.
        /// </summary>
        /// <param name="key">The key of the new/updated data item.</param>
        /// <param name="value">The payload data to write.</param>
        public void Put(byte[] key, byte[] value)
        {
            Guard.NotNull(key, nameof(key));
            Guard.NotNull(value, nameof(value));

            if (key.Length > short.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(key));
            }

            _changes[key] = new WriteBatchItem.Put(value);
        }

        /// <summary>
        /// Registers the given key to be deleted from the store upon committing. If no such key is
        /// present in the store, does nothing.
        /// </summary>
        /// <param name="key">The key of the data item to delete.</param>
        public void Delete(byte[] key)
        {
            Guard.NotNull(key, nameof(key));

            _changes[key] = new WriteBatchItem.Delete();
        }
    }
}
