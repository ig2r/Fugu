using Fugu.Common;
using Fugu.IO.Records;
using System;
using System.Runtime.CompilerServices;

namespace Fugu.IO
{
    /// <summary>
    /// Provides functionality to determine space requirements for payload items (puts or deletes). This is
    /// needed to pre-allocate space when committing to the store, when deciding which segments to compact, etc.
    /// </summary>
    public static class Measure
    {
        public static long GetSize(byte[] key, IndexEntry indexEntry)
        {
            Guard.NotNull(key, nameof(key));
            Guard.NotNull(indexEntry, nameof(indexEntry));

            switch (indexEntry)
            {
                case IndexEntry.Value value:
                    return Unsafe.SizeOf<PutRecord>() + key.Length + value.ValueLength;
                case IndexEntry.Tombstone _:
                    return Unsafe.SizeOf<TombstoneRecord>() + key.Length;
                default:
                    throw new NotSupportedException();
            }
        }

        public static long GetSize(byte[] key, WriteBatchItem writeBatchItem)
        {
            Guard.NotNull(key, nameof(key));
            Guard.NotNull(writeBatchItem, nameof(writeBatchItem));

            switch (writeBatchItem)
            {
                case WriteBatchItem.Put put:
                    return Unsafe.SizeOf<PutRecord>() + key.Length + put.Value.Length;
                case WriteBatchItem.Delete _:
                    return Unsafe.SizeOf<TombstoneRecord>() + key.Length;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
