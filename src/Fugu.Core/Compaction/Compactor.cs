using Fugu.Common;
using Fugu.DataFormat;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Compaction
{
    public class Compactor : ICompactor
    {
        private readonly ITableFactory _tableFactory;

        public Compactor(ITableFactory tableFactory)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));

            _tableFactory = tableFactory;
        }

        #region ICompactor

        public async Task<CompactionResult> CompactAsync(Index index, long minGeneration, long maxGeneration, bool dropTombstones)
        {
            Guard.NotNull(index, nameof(index));

            if (minGeneration <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minGeneration));
            }

            if (maxGeneration <= 0 || maxGeneration < minGeneration)
            {
                throw new ArgumentOutOfRangeException(nameof(maxGeneration));
            }

            // Perform full index scan to find items in source generation range
            var sourceItems = (from kvp in index
                               where minGeneration <= kvp.Value.Segment.MinGeneration &&
                                 kvp.Value.Segment.MaxGeneration <= maxGeneration
                               select kvp).ToArray();

            var requiredCapacity = Marshal.SizeOf<HeaderRecord>() + sourceItems.Sum(kvp => GetRequiredSpace(kvp.Key, kvp.Value));

            // Build up compacted segment from source index entries
            var compactedTable = await _tableFactory.CreateTableAsync(requiredCapacity);
            var compactedSegment = new Segment(minGeneration, maxGeneration, compactedTable);
            var droppedTombstones = new List<byte[]>();

            using (var tableWriter = new TableWriter(compactedTable.OutputStream))
            using (var commitBuilder = new CommitBuilder<IndexEntry.Value>(
                compactedTable.OutputStream,
                compactedSegment,
                valueEntry => valueEntry.ValueLength,
                (valueEntry, targetStream) =>
                {
                    using (var sourceStream = valueEntry.Segment.Table.GetInputStream(valueEntry.Offset, valueEntry.Size))
                    {
                        sourceStream.CopyTo(targetStream);
                    }
                }))
            {
                tableWriter.WriteHeader(minGeneration, maxGeneration);

                foreach (var kvp in sourceItems)
                {
                    var valueIndexEntry = kvp.Value as IndexEntry.Value;
                    if (valueIndexEntry != null)
                    {
                        // Put
                        commitBuilder.AddPut(kvp.Key, valueIndexEntry);
                    }
                    else
                    {
                        // Tombstone
                        if (dropTombstones)
                        {
                            droppedTombstones.Add(kvp.Key);
                        }
                        else
                        {
                            commitBuilder.AddTombstone(kvp.Key);
                        }
                    }
                }

                var indexUpdates = commitBuilder.Complete();
                tableWriter.WriteCommit();

                // TODO: Write footer

                return new CompactionResult(compactedSegment, indexUpdates, droppedTombstones);
            }
        }

        #endregion

        private long GetRequiredSpace(byte[] key, IndexEntry indexEntry)
        {
            long requiredSpace = key.Length;

            var value = indexEntry as IndexEntry.Value;
            if (value != null)
            {
                requiredSpace += value.ValueLength + Marshal.SizeOf<PutRecord>();
            }
            else
            {
                requiredSpace += Marshal.SizeOf<TombstoneRecord>();
            }

            requiredSpace += Marshal.SizeOf<StartDataRecord>();
            requiredSpace += Marshal.SizeOf<CommitRecord>();

            return requiredSpace;
        }
    }
}
