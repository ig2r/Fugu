using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class IndexActorCore
    {
        private readonly ISnapshotsActor _snapshotsActor;

        private StateVector _clock = new StateVector();

        // Current state of the master index for this store instance
        private Index _index = Index.Empty;

        public IndexActorCore(ISnapshotsActor snapshotsActor)
        {
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));
            _snapshotsActor = snapshotsActor;
        }

        #region IIndexActor

        public void UpdateIndex(
            StateVector clock,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _clock = StateVector.Max(_clock, clock);
            _clock = _clock.NextIndex();

            foreach (var update in indexUpdates)
            {
                // Determine if the index currently holds an entry for this key
                IndexEntry existingEntry;
                bool keyExists = _index.TryGetValue(update.Key, out existingEntry);

                // Reject the new key+value if one of the following holds:
                // 1. The index already contains a newer entry for the given key;
                // 2. The given update is a deletion for a key that's either not in the index, or is already
                //    a tombstone in the index
                if ((keyExists && update.Value.Segment.MaxGeneration < existingEntry.Segment.MinGeneration) ||
                    (update.Value is IndexEntry.Tombstone && (!keyExists || existingEntry is IndexEntry.Tombstone)))
                {
                    //_compactionActor.OnItemRejected(update);
                }
                else
                {
                    // Update index
                    _index = _index.SetItem(update.Key, update.Value);
                    //_compactionActor.OnItemAdded(update);

                    // If there was a previous entry for that key, remove it
                    if (keyExists)
                    {
                        //_compactionActor.OnItemDisplaced(new KeyValuePair<byte[], IndexEntry>(update.Key, existingEntry));
                    }
                }
            }

            //_compactionActor.EnsureBalance(_clock);

            // Hand off to snapshots actor
            _snapshotsActor.UpdateIndex(_clock, _index, replyChannel);
        }

        #endregion
    }
}
