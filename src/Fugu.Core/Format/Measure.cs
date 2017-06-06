using Fugu.Common;
using System;
using System.Runtime.InteropServices;

namespace Fugu.Format
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
                    return Marshal.SizeOf<PutRecord>() + key.Length + value.ValueLength;
                case IndexEntry.Tombstone _:
                    return Marshal.SizeOf<TombstoneRecord>() + key.Length;
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
                    return Marshal.SizeOf<PutRecord>() + key.Length + put.Value.Length;
                case WriteBatchItem.Delete _:
                    return Marshal.SizeOf<TombstoneRecord>() + key.Length;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
