using Fugu.Actors;
using Fugu.Common;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CritBitIndex = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Index
{
    public class IndexActorCore
    {
        private readonly ITargetBlock<SnapshotsUpdateMessage> _snapshotsUpdateBlock;
        private readonly ITargetBlock<SegmentSizesChangedMessage> _segmentSizesChangedBlock;

        // Accumulates changes to the size of segments in response to index updates
        private readonly SegmentSizeChangeTracker _tracker;

        private StateVector _clock = new StateVector();

        // Current state of the master index for this store instance
        private CritBitIndex _index = CritBitIndex.Empty;

        public IndexActorCore(
            ITargetBlock<SnapshotsUpdateMessage> snapshotsUpdateBlock,
            ITargetBlock<SegmentSizesChangedMessage> segmentSizesChangedBlock)
        {
            Guard.NotNull(snapshotsUpdateBlock, nameof(snapshotsUpdateBlock));
            Guard.NotNull(segmentSizesChangedBlock, nameof(segmentSizesChangedBlock));

            _snapshotsUpdateBlock = snapshotsUpdateBlock;
            _segmentSizesChangedBlock = segmentSizesChangedBlock;
            _tracker = new SegmentSizeChangeTracker();
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
                bool keyExists = _index.TryGetValue(update.Key, out var existingEntry);

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
                    _index = _index.SetItem(update.Key, update.Value);
                    _tracker.OnItemAdded(update.Key, update.Value);

                    // If there was a previous entry for that key, remove it
                    if (keyExists)
                    {
                        _tracker.OnItemRemoved(update.Key, existingEntry);
                    }
                }
            }

            if (_tracker.TryGetBatchedSizeChanges(_clock, out var sizeChanges))
            {
                var accepted = _segmentSizesChangedBlock.Post(new SegmentSizesChangedMessage(_clock, sizeChanges, _index));
                Debug.Assert(accepted, "Posting SegmentSizesChangedMessage must always succeed.");
            }

            // Notify downstream actors of update
            return _snapshotsUpdateBlock.SendAsync(new SnapshotsUpdateMessage(_clock, _index, replyChannel));
        }
    }
}
