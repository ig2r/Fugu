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
        private readonly ITargetBlock<UpdateIndexMessage> _updateIndexBlock;

        private readonly HashSet<Segment> _activeSegments;

        private StateVector _compactionThreshold;

        public CompactionActorCore(
            ICompactionStrategy compactionStrategy,
            ITableFactory tableFactory,
            IEnumerable<Segment> loadedSegments,
            ITargetBlock<EvictSegmentMessage> evictSegmentBlock,
            ITargetBlock<TotalCapacityChangedMessage> totalCapacityChangedBlock,
            ITargetBlock<UpdateIndexMessage> updateIndexBlock)
        {
            Guard.NotNull(compactionStrategy, nameof(compactionStrategy));
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(loadedSegments, nameof(loadedSegments));
            Guard.NotNull(evictSegmentBlock, nameof(evictSegmentBlock));
            Guard.NotNull(totalCapacityChangedBlock, nameof(totalCapacityChangedBlock));
            Guard.NotNull(updateIndexBlock, nameof(updateIndexBlock));

            _compactionStrategy = compactionStrategy;
            _tableFactory = tableFactory;
            _evictSegmentBlock = evictSegmentBlock;
            _totalCapacityChangedBlock = totalCapacityChangedBlock;
            _updateIndexBlock = updateIndexBlock;

            _activeSegments = new HashSet<Segment>(loadedSegments);
        }

        public void OnSegmentCreated(Segment segment)
        {
            // We'll keep track of this segment so that we can evict it as soon as it no longer holds valid data
            _activeSegments.Add(segment);
        }

        public async Task OnSegmentStatsChangedAsync(
            StateVector clock,
            IReadOnlyList<KeyValuePair<Segment, SegmentStats>> stats,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index)
        {
            // If the given clock has not yet surpassed the bar set by a previous compaction, stop here. This is important
            // so that we don't interleave compactions
            if (!(clock >= _compactionThreshold))
            {
                return;
            }

            // Figure out if there are any segments that are no longer part of the index, and which we should drop
            await EvictUnusedSegmentsAsync(clock, stats);

            // Check if we need to compact a range of segments
            var compactableSegments = stats
                .Where(kvp => kvp.Key.MaxGeneration < clock.OutputGeneration)
                .ToArray();

            var compactableStats = compactableSegments
                .Select(kvp => kvp.Value)
                .ToArray();

            if (_compactionStrategy.TryGetRangeToCompact(compactableStats, out var range))
            {
                // Yes, we should compact. First off, update the "compaction threshold" clock so that we will not
                // re-trigger another compaction until this one is visible in the index.
                _compactionThreshold = clock.NextCompaction();

                // Get items from index that need to go into the newly compacted segment
                var minGeneration = compactableSegments[range.Offset].Key.MinGeneration;
                var maxGeneration = compactableSegments[range.Offset + range.Count - 1].Key.MaxGeneration;

                var query = from kvp in index
                            let segment = kvp.Value.Segment
                            where minGeneration <= segment.MinGeneration && segment.MaxGeneration <= maxGeneration
                            select kvp;

                // Determine space required for the compacted table
                var requiredCapacity =
                    Marshal.SizeOf<TableHeaderRecord>() +
                    Marshal.SizeOf<CommitHeaderRecord>() +
                    query.Select(kvp => Measure.GetSize(kvp.Key, kvp.Value)).Sum() +
                    Marshal.SizeOf<CommitFooterRecord>() +
                    Marshal.SizeOf<TableFooterRecord>();

                // Allocate output segment and track it as active
                var outputTable = await _tableFactory.CreateTableAsync(requiredCapacity).ConfigureAwait(false);
                var outputSegment = new Segment(minGeneration, maxGeneration, outputTable);
                _activeSegments.Add(outputSegment);

                using (var outputStream = outputTable.GetOutputStream(0, outputTable.Capacity))
                using (var tableWriter = new TableWriter(outputStream))
                {
                    tableWriter.WriteTableHeader(minGeneration, maxGeneration);
                    tableWriter.WriteCommitHeader(query.Count());

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
                    var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();

                    foreach (var (key, indexEntry) in query)
                    {
                        switch (indexEntry)
                        {
                            case IndexEntry.Value src:
                                var valueEntry = new IndexEntry.Value(outputSegment, outputStream.Position, src.ValueLength);
                                indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, valueEntry));

                                // Copy data
                                using (var inputStream = src.Segment.Table.GetInputStream(src.Offset, src.ValueLength))
                                {
                                    inputStream.CopyTo(outputStream);
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
                    await Task.WhenAll(
                        _totalCapacityChangedBlock.SendAsync(new TotalCapacityChangedMessage(outputTable.Capacity)),
                        _updateIndexBlock.SendAsync(new UpdateIndexMessage(_compactionThreshold, indexUpdates, null)))
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task EvictUnusedSegmentsAsync(StateVector clock, IReadOnlyList<KeyValuePair<Segment, SegmentStats>> stats)
        {
            // Search for segments that are not present in the segment stats collection, and span a max generation range
            // that's less then the output generation associated with the stats collection. Those segments no longer hold
            // values that can be accessed from the current index, and since they're different from the output generation, they
            // can't hold usable data in the future either.
            var unusedSegments = _activeSegments
                .Except(stats.Select(s => s.Key))
                .Where(s => s.MaxGeneration < clock.OutputGeneration)
                .ToArray();

            // Schedule unused segments for eviction, then notify observers that the overall capacity of the table set has changed
            long deltaCapacity = 0;
            foreach (var segment in unusedSegments)
            {
                _activeSegments.Remove(segment);
                deltaCapacity -= segment.Table.Capacity;
                _evictSegmentBlock.Post(new EvictSegmentMessage(clock, segment));
            }

            if (deltaCapacity != 0)
            {
                await _totalCapacityChangedBlock.SendAsync(new TotalCapacityChangedMessage(deltaCapacity)).ConfigureAwait(false);
            }
        }
    }
}
