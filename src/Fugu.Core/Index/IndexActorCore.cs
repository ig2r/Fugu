using Fugu.Actors;
using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using StoreIndex = Fugu.Common.AaTree<Fugu.IndexEntry>;

namespace Fugu.Index
{
    public class IndexActorCore
    {
        private readonly ITargetBlock<SnapshotsUpdateMessage> _snapshotsUpdateBlock;
        private readonly ITargetBlock<SegmentStatsChangedMessage> _segmentStatsChangedBlock;

        // Accumulates changes to the size of segments in response to index updates
        private readonly SegmentSizeTracker _tracker;

        private StateVector _clock = new StateVector();

        // Current state of the master index for this store instance
        private StoreIndex.Builder _indexBuilder = new StoreIndex.Builder();

        public IndexActorCore(
            ITargetBlock<SnapshotsUpdateMessage> snapshotsUpdateBlock,
            ITargetBlock<SegmentStatsChangedMessage> segmentStatsChangedBlock)
        {
            Guard.NotNull(snapshotsUpdateBlock, nameof(snapshotsUpdateBlock));
            Guard.NotNull(segmentStatsChangedBlock, nameof(segmentStatsChangedBlock));

            _snapshotsUpdateBlock = snapshotsUpdateBlock;
            _segmentStatsChangedBlock = segmentStatsChangedBlock;
            _tracker = new SegmentSizeTracker();
        }

        public Task UpdateIndexAsync(
            StateVector clock,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _clock = StateVector.Max(_clock, clock);

            foreach (var update in indexUpdates)
            {
                // Determine if the index currently holds an entry for this key
                bool keyExists = _indexBuilder.TryGetValue(update.Key, out var existingEntry);

                // Reject the new key+value if one of the following holds:
                // 1. The index already contains a newer entry for the given key;
                // 2. The given update is a deletion for a key that's either not in the index, or is already a tombstone.
                var currentEntryIsNewer = keyExists && update.Value.Segment.MaxGeneration < existingEntry.Segment.MinGeneration;
                var noValueToDelete = update.Value is IndexEntry.Tombstone && !(keyExists && existingEntry is IndexEntry.Value);
                if (currentEntryIsNewer || noValueToDelete)
                {
                    // Reject change
                    _tracker.OnItemRejected(update.Key, update.Value);
                }
                else
                {
                    // Add new entry to index
                    _indexBuilder[update.Key] = update.Value;
                    _tracker.OnItemAdded(update.Key, update.Value);

                    // If there was a previous entry for that key, remove it
                    if (keyExists)
                    {
                        _tracker.OnItemRemoved(update.Key, existingEntry);
                    }
                }
            }

            var index = _indexBuilder.ToImmutable();

            // Make new segment stats available to observers
            _tracker.Prune();
            _segmentStatsChangedBlock.Post(new SegmentStatsChangedMessage(_clock, _tracker.Stats, index));

            // Notify downstream actors of update
            return _snapshotsUpdateBlock.SendAsync(new SnapshotsUpdateMessage(_clock, index, replyChannel));
        }
    }
}
