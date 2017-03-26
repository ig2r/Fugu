using Fugu.Channels;
using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class IndexActorCore
    {
        private readonly Channel<SnapshotsUpdateMessage> _snapshotsUpdateChannel;
        private readonly Channel<SegmentSizesChangedMessage> _segmentSizesChangedChannel;

        // Accumulates changes to the size of segments in response to index updates
        private readonly SegmentSizeChangeTracker _tracker;

        private StateVector _clock = new StateVector();

        // Current state of the master index for this store instance
        private Index _index = Index.Empty;

        public IndexActorCore(
            Channel<SnapshotsUpdateMessage> snapshotsUpdateChannel,
            Channel<SegmentSizesChangedMessage> segmentSizesChangedChannel)
        {
            Guard.NotNull(snapshotsUpdateChannel, nameof(snapshotsUpdateChannel));
            Guard.NotNull(segmentSizesChangedChannel, nameof(segmentSizesChangedChannel));

            _snapshotsUpdateChannel = snapshotsUpdateChannel;
            _segmentSizesChangedChannel = segmentSizesChangedChannel;
            _tracker = new SegmentSizeChangeTracker();
        }

        public async void UpdateIndex(
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
                // 2. The given update is a deletion for a key that's either not in the index, or is already a tombstone
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
                await _segmentSizesChangedChannel.SendAsync(new SegmentSizesChangedMessage(_clock, sizeChanges, _index));
            }

            // Notify downstream actors of update
            await _snapshotsUpdateChannel.SendAsync(new SnapshotsUpdateMessage(_clock, _index, replyChannel));
        }
    }
}
