using Fugu.Common;
using Fugu.Compaction;
using Fugu.DataFormat;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class CompactionActor : ICompactionActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();
        private readonly ICompactionStrategy _compactionStrategy;
        private readonly ITableFactory _tableFactory;
        private readonly IEvictionActor _evictionActor;

        private readonly List<Segment> _compactableSegments = new List<Segment>();

        private VectorClock _clock;

        public CompactionActor(
            ICompactionStrategy compactionStrategy,
            ITableFactory tableFactory,
            IEvictionActor evictionActor)
        {
            Guard.NotNull(compactionStrategy, nameof(compactionStrategy));
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(evictionActor, nameof(evictionActor));

            _compactionStrategy = compactionStrategy;
            _tableFactory = tableFactory;
            _evictionActor = evictionActor;
        }

        /// <summary>
        /// Fires when the total of payload bytes (both live and dead) in compactable segments changes after a
        /// newly written segment has been registered for compactions. The writer actor depends on this number
        /// in order to choose an approriate capacity when starting new segments.
        /// </summary>
        public event Action<VectorClock, long> CompactableBytesChanged;

        public async void RegisterCompactableSegment(VectorClock clock, Segment segment)
        {
            Guard.NotNull(segment, nameof(segment));

            using (await _messageLoop)
            {
                // Increment internal clock. This will cause incoming index updates to not trigger a compaction
                // until these updates are also tagged with a clock value that's at least as recent.
                _clock = VectorClock.Merge(_clock, clock);

                _compactableSegments.Add(segment);

                // Notify subscribers of the new total of payload bytes in the range of compactable segments
                var compactableBytes = _compactableSegments.Sum(s => s.Stats.TotalBytes);
                CompactableBytesChanged?.Invoke(_clock, compactableBytes);
            }
        }

        public async void OnIndexUpdated(
            IIndexActor sender,
            VectorClock clock,
            IReadOnlyList<IndexEntry> addedEntries,
            IReadOnlyList<IndexEntry> removedEntries,
            Index index)
        {
            Guard.NotNull(sender, nameof(sender));
            Guard.NotNull(addedEntries, nameof(addedEntries));
            Guard.NotNull(removedEntries, nameof(removedEntries));
            Guard.NotNull(index, nameof(index));

            using (await _messageLoop)
            {
                // Update segment stats from updates in any case
                UpdateStats(addedEntries, removedEntries);

                // Consider evicting/compacting segments only if we know we're on top of things so far
                if (clock >= _clock)
                {
                    _clock = VectorClock.Merge(_clock, clock);

                    EvictEmptySegments();

                    var stats = _compactableSegments.Select(s => s.Stats).ToArray();
                    Range compactionRange;
                    if (_compactionStrategy.TryGetCompactionRange(stats, out compactionRange))
                    {
                        await CompactAsync(sender, index, compactionRange);
                    }
                }
            }
        }

        private void UpdateStats(IReadOnlyList<IndexEntry> addedEntries, IReadOnlyList<IndexEntry> removedEntries)
        {
            foreach (var added in addedEntries)
            {
                added.Segment.Stats.AddLiveBytes(added.Size);
            }

            foreach (var removed in removedEntries)
            {
                removed.Segment.Stats.MarkBytesAsDead(removed.Size);
            }
        }

        /// <summary>
        /// Scans the list of compactable segments for segments whose live-data count has dropped to zero
        /// and schedules them for eviction once they are no longer visible in any snapshot.
        /// </summary>
        private void EvictEmptySegments()
        {
            for (int i = 0; i < _compactableSegments.Count;)
            {
                if (_compactableSegments[i].Stats.LiveBytes > 0)
                {
                    i++;
                    continue;
                }

                _evictionActor.ScheduleEviction(_clock, _compactableSegments[i]);
                _compactableSegments.RemoveAt(i);
            }
        }

        private async Task CompactAsync(IIndexActor sender, Index index, Range compactionRange)
        {
            long minGeneration = _compactableSegments[compactionRange.Index].MinGeneration;
            long maxGeneration = _compactableSegments[compactionRange.Index + compactionRange.Count - 1].MaxGeneration;
            bool dropTombstones = compactionRange.Index == 0;

            // Run full index scan to find items from input generation range and partition items into lists
            // of source items (which will be copied during compaction) and items which will be dropped because
            // they're tombstones that are not needed anymore
            var sourceItems = new List<KeyValuePair<byte[], IndexEntry>>();
            var droppedItems = new List<byte[]>();
            foreach (var kvp in index)
            {
                if (minGeneration <= kvp.Value.Segment.MinGeneration &&
                    kvp.Value.Segment.MaxGeneration <= maxGeneration)
                {
                    if (!dropTombstones || kvp.Value is IndexEntry.Value)
                    {
                        sourceItems.Add(kvp);
                    }
                    else
                    {
                        droppedItems.Add(kvp.Key);
                    }
                }
            }

            var compactedTable = await CreateCompactionTableAsync(sourceItems);
            var compactedSegment = new Segment(minGeneration, maxGeneration, compactedTable);
            var indexUpdates = WriteCompactionTable(
                sourceItems,
                compactedTable.OutputStream,
                compactedSegment);

            // Insert compacted segment into the list of immutable, compactable segments
            _compactableSegments.Insert(compactionRange.Index, compactedSegment);

            // Increment the compaction component of the state vector so that we can resume compactions once
            // we receive an index that has been updated to reflect these changes
            _clock = new VectorClock(_clock.Modification, _clock.Compaction + 1);

            // Propagate index updates to index actor
            if (droppedItems.Count > 0)
            {
                sender.RemoveEntries(droppedItems, compactedSegment.MaxGeneration);
            }

            var indexUpdateTask = sender.MergeIndexUpdatesAsync(_clock, indexUpdates);
        }

        private Task<IWritableTable> CreateCompactionTableAsync(IReadOnlyList<KeyValuePair<byte[], IndexEntry>> sourceItems)
        {
            var requiredCapacity = Marshal.SizeOf<HeaderRecord>() + sourceItems.Sum(kvp => GetRequiredSpace(kvp.Key, kvp.Value));
            return _tableFactory.CreateTableAsync(requiredCapacity);
        }

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

        private IReadOnlyList<KeyValuePair<byte[], IndexEntry>> WriteCompactionTable(
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> sourceItems,
            Stream outputStream,
            Segment compactedSegment)
        {
            using (var tableWriter = new TableWriter(outputStream))
            using (var commitBuilder = new CommitBuilder<IndexEntry.Value>(
                outputStream,
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
                tableWriter.WriteHeader(compactedSegment.MinGeneration, compactedSegment.MaxGeneration);

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
                        commitBuilder.AddTombstone(kvp.Key);
                    }
                }

                var indexUpdates = commitBuilder.Complete();
                tableWriter.WriteCommit();
                outputStream.Flush();

                // TODO: Write footer

                return indexUpdates;
            }
        }
    }
}
