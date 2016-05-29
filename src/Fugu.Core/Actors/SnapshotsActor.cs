using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    using Index = CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry>;

    /// <summary>
    /// Manages the collection of snapshots for a store instance. Primary responsibilities are to create new snapshots
    /// of the store's data on request and keeping track of which snapshots are currently open.
    /// </summary>
    public class SnapshotsActor : ISnapshotsActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();
        private readonly HashSet<Snapshot> _activeSnapshots = new HashSet<Snapshot>();

        // Most up-to-date state of the store's index
        private VectorClock _clock = new VectorClock();
        private Index _index = Index.Empty;

        public event Action<VectorClock> OldestLiveSnapshotChanged;

        /// <summary>
        /// Retrieves an immutable snapshot of the store.
        /// </summary>
        /// <returns>Snapshot that lets consumers read from the store.</returns>
        public async Task<Snapshot> GetSnapshotAsync()
        {
            using (await _messageLoop)
            {
                var snapshot = new Snapshot(_index, _clock, OnSnapshotDisposed);
                _activeSnapshots.Add(snapshot);
                return snapshot;
            }
        }

        /// <summary>
        /// Notifies the actor that the store's index has changed and this updated representation should be reflected
        /// in future snapshots.
        /// </summary>
        /// <param name="clock">The clock value associated with the new store state.</param>
        /// <param name="index">The new index of the store.</param>
        /// <returns></returns>
        public async Task UpdateIndexAsync(VectorClock clock, Index index)
        {
            Guard.NotNull(index, nameof(index));

            using (await _messageLoop)
            {
                var previousClock = _clock;
                _clock = VectorClock.Merge(_clock, clock);

                if (previousClock <= _clock)
                {
                    _index = index;

                    if (_activeSnapshots.Count == 0 && previousClock < _clock)
                    {
                        OldestLiveSnapshotChanged?.Invoke(_clock);
                    }
                }
            }
        }

        /// <summary>
        /// Invoked when a snapshot is disposed. Removes the respective snapshot from internal data structures
        /// and figures out if this causes the clock value of the oldest live snapshot to change; if this is the
        /// case, notifies subscribers via the <see cref="OldestLiveSnapshotChanged"/> topic.
        /// </summary>
        /// <param name="snapshot">The snapshot that was disposed of.</param>
        private async void OnSnapshotDisposed(Snapshot snapshot)
        {
            Guard.NotNull(snapshot, nameof(snapshot));

            using (await _messageLoop)
            {
                if (_activeSnapshots.Remove(snapshot))
                {
                    VectorClock oldestActiveClock = _clock;
                    foreach (var activeSnapshot in _activeSnapshots)
                    {
                        if (activeSnapshot.Clock < oldestActiveClock)
                        {
                            oldestActiveClock = activeSnapshot.Clock;
                        }
                    }

                    if (snapshot.Clock < oldestActiveClock)
                    {
                        OldestLiveSnapshotChanged?.Invoke(oldestActiveClock);
                    }
                }
            }
        }
    }
}
