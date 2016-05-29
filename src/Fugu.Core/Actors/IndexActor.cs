using Fugu.Common;
using Fugu.Compaction;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class IndexActor : IIndexActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();

        private readonly ICompactionStrategy _compactionStrategy;
        private readonly ICompactor _compactor;
        private readonly ISnapshotsActor _snapshotsActor;
        private readonly IEvictionActor _evictionActor;

        // Actor state: master index and associated clock vector
        private VectorClock _clock = new VectorClock();
        private Index _index = Index.Empty;

        // Actor state: balance-related fields
        private readonly List<Segment> _compactableSegments = new List<Segment>();
        //private bool _isCompacting = false;

        public IndexActor(
            ICompactionStrategy compactionStrategy,
            ICompactor compactor,
            ISnapshotsActor snapshotsActor,
            IEvictionActor evictionActor)
        {
            Guard.NotNull(compactionStrategy, nameof(compactionStrategy));
            Guard.NotNull(compactor, nameof(compactor));
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));
            Guard.NotNull(evictionActor, nameof(evictionActor));

            _compactionStrategy = compactionStrategy;
            _compactor = compactor;
            _snapshotsActor = snapshotsActor;
            _evictionActor = evictionActor;
        }

        /// <summary>
        /// Raised when the total capacity of compactable segments has changed due to a compaction.
        /// </summary>
        public event Action<long> CompactableCapacityChanged;

        public event Action<IReadOnlyList<SegmentStats>> CompactableSegmentsChanged;

        public async Task UpdateAsync(VectorClock clock, IEnumerable<KeyValuePair<byte[], IndexEntry>> updates)
        {
            Guard.NotNull(updates, nameof(updates));

            Task continuation;

            using (await _messageLoop)
            {
                // Consolidate clock
                _clock = VectorClock.Merge(_clock, clock);

                // Apply updates, attempt to restore balance if necessary, then notify downstream actors
                ProcessUpdatesAndEnsureBalance(updates);
                continuation = _snapshotsActor.UpdateIndexAsync(_clock, _index);
            }

            await continuation;
        }

        public async void RegisterCompactableSegment(Segment segment)
        {
            Guard.NotNull(segment, nameof(segment));

            using (await _messageLoop)
            {
                // TODO: delay adding the segment to the list until we can be sure that the index data is fully available.
                // Edge case here is that a new segment is registered, but a compaction finishes (and triggers another
                // rebalance that may pick up the newly added segment) before the final write results for this segment come
                // through.
                // Note that it may be sufficient to register the segment from the writer actor only AFTER the final index
                // update for it has been triggered.
                _compactableSegments.Add(segment);

                // Notify other actors
                var segmentStats = _compactableSegments.Select(s => new SegmentStats(s.LiveBytes, s.DeadBytes)).ToArray();
                CompactableSegmentsChanged?.Invoke(segmentStats);
            }
        }

        private void ProcessUpdatesAndEnsureBalance(IEnumerable<KeyValuePair<byte[], IndexEntry>> indexUpdates)
        {
            UpdateIndexAndStats(indexUpdates);

            // Check balance invariants if we're not currently waiting for the results of a previous compaction
            //if (!_isCompacting)
            //{
            //    EvictEmptySegments();
            //    Range compactionRange;
            //    if (_compactionStrategy.TryGetCompactionRange(_compactableSegments, out compactionRange))
            //    {
            //        RunCompaction(compactionRange);
            //    }
            //}
        }

        private void UpdateIndexAndStats(IEnumerable<KeyValuePair<byte[], IndexEntry>> indexUpdates)
        {
            foreach (var update in indexUpdates)
            {
                // Required either way
                update.Value.Segment.AddLiveBytes(update.Value.Size);

                IndexEntry existingEntry;
                if (_index.TryGetValue(update.Key, out existingEntry))
                {
                    // If the index already holds an entry for the current key, only replace it if the new entry is
                    // associated with a generation that's as recent or newer
                    if (update.Value.Segment.MaxGeneration >= existingEntry.Segment.MaxGeneration)
                    {
                        existingEntry.Segment.MarkBytesAsDead(existingEntry.Size);
                        _index = _index.SetItem(update.Key, update.Value);
                    }
                    else
                    {
                        update.Value.Segment.MarkBytesAsDead(update.Value.Size);
                    }
                }
                else if (update.Value is IndexEntry.Tombstone)
                {
                    // New entry is a tombstone, but the index doesn't contain an entry for that key
                    update.Value.Segment.MarkBytesAsDead(update.Value.Size);
                }
                else
                {
                    // Adding a new value for a key that's not yet in the index
                    _index = _index.SetItem(update.Key, update.Value);
                }
            }
        }

        /// <summary>
        /// Scans the list of compactable segments for segments whose live-data count has dropped to zero
        /// and schedules them for eviction once they are no longer visible in any snapshot.
        /// </summary>
        private void EvictEmptySegments()
        {
            long delta = 0;

            for (int i = 0; i < _compactableSegments.Count; )
            {
                if (_compactableSegments[i].LiveBytes > 0)
                {
                    i++;
                    continue;
                }

                delta -= _compactableSegments[i].Table.Capacity;

                _evictionActor.ScheduleEviction(_clock, _compactableSegments[i]);
                _compactableSegments.RemoveAt(i);
            }

            if (delta != 0)
            {
                CompactableCapacityChanged?.Invoke(delta);
            }
        }

        //private async void RunCompaction(Range compactionRange)
        //{
        //    try
        //    {
        //        _isCompacting = true;

        //        // Input parameters for compaction
        //        long minGeneration = _compactableSegments[compactionRange.Index].MinGeneration;
        //        long maxGeneration = _compactableSegments[compactionRange.Index + compactionRange.Count - 1].MaxGeneration;
        //        bool dropTombstones = compactionRange.Index == 0;

        //        // Run the compaction asynchronously, this will likely take us off the message loop
        //        var result = await _compactor.CompactAsync(_index, minGeneration, maxGeneration, dropTombstones);

        //        // Re-enter the message loop so that we may apply the result
        //        using (await _messageLoop)
        //        {
        //            _clock = new VectorClock(_clock.Modification, _clock.Compaction + 1);
        //            _compactableSegments.Insert(compactionRange.Index, result.CompactedSegment);
        //            CompactableCapacityChanged?.Invoke(result.CompactedSegment.Table.Capacity);

        //            RemoveTombstones(result.DroppedTombstones, result.CompactedSegment.MaxGeneration);

        //            // Reset flag here already so that it doesn't interfere with the balancing in ProcessUpdatesAsync
        //            _isCompacting = false;

        //            ProcessUpdatesAndEnsureBalance(result.IndexUpdates);
        //            var continuation = _snapshotsActor.UpdateIndexAsync(_clock, _index);
        //        }
        //    }
        //    finally
        //    {
        //        _isCompacting = false;
        //    }
        //}

        /// <summary>
        /// Removes tombstones from the index after they have disappeared in a compaction of the oldest
        /// segments.
        /// </summary>
        /// <param name="keys">Keys of tombstones to remove from the index.</param>
        /// <param name="maxGeneration">
        /// Most recent generation included in compaction so that we can detect if the same key has been
        /// modified concurrently, in which case we need to retain the new value.
        /// </param>
        //private void RemoveTombstones(IReadOnlyList<byte[]> keys, long maxGeneration)
        //{
        //    foreach (var tombstoneKey in keys)
        //    {
        //        IndexEntry existingTombstone;
        //        if (_index.TryGetValue(tombstoneKey, out existingTombstone) &&
        //            maxGeneration >= existingTombstone.Segment.MaxGeneration)
        //        {
        //            existingTombstone.Segment.MarkBytesAsDead(existingTombstone.Size);
        //            _index = _index.Remove(tombstoneKey);
        //        }
        //    }
        //}
    }
}
