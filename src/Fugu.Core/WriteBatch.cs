using Fugu.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Fugu
{
    public class WriteBatch
    {
        private readonly ConcurrentDictionary<byte[], WriteBatchItem> _changes =
            new ConcurrentDictionary<byte[], WriteBatchItem>(new ByteArrayEqualityComparer());

        private bool _isFrozen = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteBatch"/> class.
        /// </summary>
        /// <param name="snapshot">
        /// Baseline snapshot for any changes. When committing the current write batch, the store will verify that none
        /// of the affected keys have been modified concurrently.
        /// </param>
        public WriteBatch(Snapshot snapshot)
        {
            Guard.NotNull(snapshot, nameof(snapshot));
            Snapshot = snapshot;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteBatch"/> class.
        /// </summary>
        public WriteBatch()
        {
            Snapshot = null;
        }

        public Snapshot Snapshot { get; }

        public IReadOnlyDictionary<byte[], WriteBatchItem> Changes => _changes;

        public void Freeze()
        {
            _isFrozen = true;
        }

        public void Put(byte[] key, byte[] value)
        {
            Guard.NotNull(key, nameof(key));
            Guard.NotNull(value, nameof(value));
            ThrowIfFrozen();

            _changes[key] = new WriteBatchItem.Put(value);
        }

        public void Delete(byte[] key)
        {
            Guard.NotNull(key, nameof(key));
            ThrowIfFrozen();

            _changes[key] = new WriteBatchItem.Delete();
        }

        private void ThrowIfFrozen()
        {
            if (_isFrozen)
            {
                throw new InvalidOperationException("WriteBatch is frozen.");
            }
        }
    }
}
