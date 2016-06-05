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
        private readonly ISnapshotsActor _snapshotsActor;
        private readonly ICompactionActor _compactionActor;

        // Actor state: master index and associated clock vector
        private VectorClock _clock = new VectorClock();
        private Index _index = Index.Empty;

        public IndexActor(ISnapshotsActor snapshotsActor, ICompactionActor compactionActor)
        {
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));
            Guard.NotNull(compactionActor, nameof(compactionActor));
            _snapshotsActor = snapshotsActor;
            _compactionActor = compactionActor;
        }

        public async Task MergeIndexUpdatesAsync(VectorClock clock, IEnumerable<KeyValuePair<byte[], IndexEntry>> updates)
        {
            Guard.NotNull(updates, nameof(updates));

            Task continuation;

            using (await _messageLoop)
            {
                _clock = VectorClock.Merge(_clock, clock);

                var addedEntries = new List<IndexEntry>(updates.Select(kvp => kvp.Value));
                var removedEntries = UpdateIndex(updates);
                continuation = NotifyIndexChangedAsync(addedEntries, removedEntries);
            }

            await continuation;
        }

        /// <summary>
        /// Removes a set of keys from the index and notifies dependent actors of the change.
        /// </summary>
        /// <param name="keys">The keys to remove.</param>
        /// <param name="maxGeneration">
        /// The maximum generation that each key may be in to qualify for removal. If a given key is present
        /// in the index, but is associated with a higher generation than indicated by this parameter, the key
        /// will not be removed because it was modified in a way that the caller did not witness.
        /// </param>
        public async void RemoveEntries(IEnumerable<byte[]> keys, long maxGeneration)
        {
            Guard.NotNull(keys, nameof(keys));
            if (maxGeneration < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxGeneration));
            }

            using (await _messageLoop)
            {
                var removedEntries = new List<IndexEntry>();

                foreach (var key in keys)
                {
                    IndexEntry existingEntry;
                    if (_index.TryGetValue(key, out existingEntry) && existingEntry.Segment.MaxGeneration <= maxGeneration)
                    {
                        _index = _index.Remove(key);
                        removedEntries.Add(existingEntry);
                    }
                }

                // Make downstream actors aware of these changes
                if (removedEntries.Count > 0)
                {
                    var task = NotifyIndexChangedAsync(new IndexEntry[0], removedEntries);
                }
            }
        }

        private IReadOnlyList<IndexEntry> UpdateIndex(IEnumerable<KeyValuePair<byte[], IndexEntry>> updates)
        {
            var removedEntries = new List<IndexEntry>();
            foreach (var update in updates)
            {
                IndexEntry existingEntry;
                if (_index.TryGetValue(update.Key, out existingEntry))
                {
                    // If the index already holds an entry for the current key, only replace it if the new entry is
                    // associated with a generation that's as recent or newer
                    if (update.Value.Segment.MaxGeneration >= existingEntry.Segment.MaxGeneration)
                    {
                        removedEntries.Add(existingEntry);
                        _index = _index.SetItem(update.Key, update.Value);
                    }
                    else
                    {
                        removedEntries.Add(update.Value);
                    }
                }
                else if (update.Value is IndexEntry.Tombstone)
                {
                    // New entry is a tombstone, but the index doesn't contain an entry for that key
                    removedEntries.Add(update.Value);
                }
                else
                {
                    // Adding a new value for a key that's not yet in the index
                    _index = _index.SetItem(update.Key, update.Value);
                }
            }

            return removedEntries;
        }

        private Task NotifyIndexChangedAsync(
            IReadOnlyList<IndexEntry> addedEntries,
            IReadOnlyList<IndexEntry> removedEntries)
        {
            _compactionActor.OnIndexUpdated(this, _clock, addedEntries, removedEntries, _index);
            return _snapshotsActor.UpdateIndexAsync(_clock, _index);
        }
    }
}
