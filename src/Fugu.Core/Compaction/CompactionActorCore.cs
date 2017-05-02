using Fugu.Actors;
using Fugu.Common;
using Fugu.Eviction;
using Fugu.Format;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Compaction
{
    /// <summary>
    /// Monitors data distribution across segments in the store, and evicts/merges segments to retain balance.
    /// </summary>
    public class CompactionActorCore
    {
        private readonly ICompactionStrategy _compactionStrategy;
        private readonly ITableFactory _tableFactory;
        private readonly ITargetBlock<EvictSegmentMessage> _evictSegmentBlock;
        private readonly ITargetBlock<TotalCapacityChangedMessage> _totalCapacityChangedBlock;
        private readonly Dictionary<Segment, SegmentStats> _segmentStats = new Dictionary<Segment, SegmentStats>();
        private readonly ITargetBlock<UpdateIndexMessage> _updateIndexBlock;

        private StateVector _compactionThreshold;

        public CompactionActorCore(
            ICompactionStrategy compactionStrategy,
            ITableFactory tableFactory,
            ITargetBlock<EvictSegmentMessage> evictSegmentBlock,
            ITargetBlock<TotalCapacityChangedMessage> totalCapacityChangedBlock,
            ITargetBlock<UpdateIndexMessage> updateIndexBlock)
        {
            Guard.NotNull(compactionStrategy, nameof(compactionStrategy));
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(evictSegmentBlock, nameof(evictSegmentBlock));
            Guard.NotNull(totalCapacityChangedBlock, nameof(totalCapacityChangedBlock));
            Guard.NotNull(updateIndexBlock, nameof(updateIndexBlock));

            _compactionStrategy = compactionStrategy;
            _tableFactory = tableFactory;
            _evictSegmentBlock = evictSegmentBlock;
            _totalCapacityChangedBlock = totalCapacityChangedBlock;
            _updateIndexBlock = updateIndexBlock;
        }

        public async Task OnSegmentSizesChangedAsync(
            StateVector clock,
            IReadOnlyList<KeyValuePair<Segment, SegmentSizeChange>> sizeChanges,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index)
        {
            // Apply updates
            foreach (var (key, change) in sizeChanges)
            {
                var stats = _segmentStats.TryGetValue(key, out var existingStats)
                    ? existingStats
                    : default(SegmentStats);

                _segmentStats[key] = stats + change;
            }

            // Schedule segments for eviction if they no longer hold useful data
            await EvictEmptyImmutableSegmentsAsync(clock).ConfigureAwait(false);

            // If the given clock has not yet surpassed the bar set by a previous compaction, stop here
            if (!(clock >= _compactionThreshold))
            {
                return;
            }

            // Check if we need to compact a range of segments
            var compactableStats = _segmentStats
                .Where(kvp => kvp.Key.MaxGeneration < clock.OutputGeneration)
                .OrderBy(kvp => kvp.Key.MinGeneration)
                .ToArray();

            if (_compactionStrategy.TryGetRangeToCompact(compactableStats, out var range))
            {
                // Yes, we should compact. First off, update the "compaction threshold" clock so that we will not
                // re-trigger another compaction until this one is visible in the index.
                _compactionThreshold = clock.NextCompaction();

                // Get items from index that need to go into the newly compacted segment
                var minGeneration = compactableStats[range.offset].Key.MinGeneration;
                var maxGeneration = compactableStats[range.offset + range.count - 1].Key.MaxGeneration;

                var query = from kvp in index
                            let segment = kvp.Value.Segment
                            where minGeneration <= segment.MinGeneration && segment.MaxGeneration <= maxGeneration
                            select kvp;
                var changeCount = query.Count();

                // Determine space required for the compacted table
                var requiredCapacity = Marshal.SizeOf<TableHeaderRecord>() + Marshal.SizeOf<TableFooterRecord>() +
                    Marshal.SizeOf<CommitHeaderRecord>() + Marshal.SizeOf<CommitFooterRecord>() +
                    query.Select(Measure).Sum();

                var outputTable = await _tableFactory.CreateTableAsync(requiredCapacity).ConfigureAwait(false);
                var outputSegment = new Segment(minGeneration, maxGeneration, outputTable);

                using (var tableWriter = new TableWriter(outputTable.OutputStream))
                {
                    tableWriter.WriteTableHeader(minGeneration, maxGeneration);
                    tableWriter.WriteCommitHeader(changeCount);

                    // First pass: write put/tombstone keys
                    foreach (var (key, indexEntry) in query)
                    {
                        switch (indexEntry)
                        {
                            case IndexEntry.Value value:
                                tableWriter.WritePut(key, value.ValueLength);
                                break;
                            case IndexEntry.Tombstone tombstone:
                                tableWriter.WriteTombstone(key);
                                break;
                            default:
                                throw new NotSupportedException();
                        }
                    }

                    // Second pass: write payload and assemble index updates
                    var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>(changeCount);

                    foreach (var (key, indexEntry) in query)
                    {
                        switch (indexEntry)
                        {
                            case IndexEntry.Value src:
                                var valueEntry = new IndexEntry.Value(outputSegment, tableWriter.Position, src.ValueLength);
                                indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, valueEntry));

                                // Copy data
                                using (var inputStream = src.Segment.Table.GetInputStream(src.Offset, src.ValueLength))
                                {
                                    inputStream.CopyTo(tableWriter.OutputStream);
                                }

                                break;
                            case IndexEntry.Tombstone _:
                                var tombstoneEntry = new IndexEntry.Tombstone(outputSegment);
                                indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, tombstoneEntry));
                                break;
                        }
                    }

                    // Finish output segment
                    tableWriter.WriteCommitFooter();
                    tableWriter.WriteTableFooter();

                    // Notify environment
                    await _totalCapacityChangedBlock.SendAsync(new TotalCapacityChangedMessage(outputTable.Capacity)).ConfigureAwait(false);

                    var updateIndexMessage = new UpdateIndexMessage(_compactionThreshold, indexUpdates, null);
                    await _updateIndexBlock.SendAsync(updateIndexMessage).ConfigureAwait(false);
                }
            }
        }

        private Task EvictEmptyImmutableSegmentsAsync(StateVector clock)
        {
            var emptySegments = _segmentStats
                .Where(kvp => kvp.Key.MaxGeneration < clock.OutputGeneration && kvp.Value.LiveBytes == 0)
                .Select(kvp => kvp.Key)
                .ToArray();

            foreach (var s in emptySegments)
            {
                _segmentStats.Remove(s);
            }

            long deltaCapacity = 0;
            var tasks = new List<Task>();

            tasks.AddRange(emptySegments.Select(s =>
            {
                deltaCapacity -= s.Table.Capacity;
                var msg = new EvictSegmentMessage(clock, s);
                return _evictSegmentBlock.SendAsync(msg);
            }));

            if (deltaCapacity != 0)
            {
                tasks.Add(_totalCapacityChangedBlock.SendAsync(new TotalCapacityChangedMessage(deltaCapacity)));
            }

            return Task.WhenAll(tasks);
        }

        private long Measure(KeyValuePair<byte[], IndexEntry> indexItem)
        {
            long size = indexItem.Key.Length;

            switch (indexItem.Value)
            {
                case IndexEntry.Value value:
                    return size + Marshal.SizeOf<PutRecord>() + value.ValueLength;
                case IndexEntry.Tombstone tombstone:
                    return size + Marshal.SizeOf<TombstoneRecord>();
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
