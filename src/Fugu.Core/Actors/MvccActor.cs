using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class MvccActor : IMvccActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();

        private readonly IWriterActor _writerActor;

        // TODO: Review if it's sufficient to store only the modification timestamp in this dictionary, not the full vector
        private readonly Dictionary<byte[], VectorClock> _recentlyModifiedKeys = new Dictionary<byte[], VectorClock>(new ByteArrayEqualityComparer());

        private VectorClock _clock = new VectorClock();
        private VectorClock _oldestActiveClock = new VectorClock();

        public MvccActor(IWriterActor writerActor)
        {
            Guard.NotNull(writerActor, nameof(writerActor));
            _writerActor = writerActor;
        }

        public async Task CommitAsync(WriteBatch batch)
        {
            Guard.NotNull(batch, nameof(batch));

            // Disallow future modification of write batch, short-circuit if empty
            batch.Freeze();
            if (batch.Changes.Count == 0)
            {
                return;
            }

            Task continuation;

            using (await _messageLoop)
            {
                // If the changes in the current write batch are based on a snapshot, check that (a) the snapshot is recent enough
                // so that we still have a record of modified keys since the time the snapshot was taken (because we might not be
                // able to detect some conflicting modifications otherwise), and (b) the set of changes in the write batch do not
                // conflict with any other changes to affected keys since the snapshot was taken.
                if (batch.Snapshot != null)
                {
                    EnsureSnapshotCausallyDerivedFromOldestTrackedSnapshot(batch.Snapshot);
                    EnsureNoConcurrencyConflicts(batch);

                    // Update internal clock to reflect most recent known global state. Not strictly necessary, but simplifies debugging.
                    _clock = VectorClock.Merge(_clock, batch.Snapshot.Clock);
                }

                // Step "modification" component of clock to associate the changes in the current write batch with the that new clock
                // value, then track the affected keys as recent modifications in the internal dictionary. From now on, clients will
                // only be able to modify these keys (a) through blind writes, or (b) when they present a baseline snapshot with a
                // "modification" clock component that indicates that the snapshot reflects these changes.
                _clock = new VectorClock(_clock.Modification + 1, _clock.Compaction);
                MarkKeysAsRecentlyModified(_clock, batch);

                // Hand off changes to writer
                continuation = _writerActor.WriteAsync(_clock, batch);
            }

            await continuation;
        }

        /// <summary>
        /// Notifies the current instance that the clock value of the oldest retained snapshot has moved forward, i.e.,
        /// that the previously oldest snapshot has been released. When this happens, this actor can also stop tracking
        /// modified keys for concurrency violations.
        /// </summary>
        /// <param name="oldestActiveClock">The clock value of the new oldest active snapshot.</param>
        public async void OnOldestLiveSnapshotChanged(VectorClock oldestActiveClock)
        {
            using (await _messageLoop)
            {
                _oldestActiveClock = oldestActiveClock;
                _clock = VectorClock.Merge(_clock, oldestActiveClock);

                StopTrackingModificationsBeforeOldestActiveSnapshot();
            }
        }

        /// <summary>
        /// Makes sure we don't accept changes that are based on a snapshot from a logical point in time for which
        /// we may not be tracking modifications anymore.
        /// </summary>
        /// <param name="snapshot">The snapshot to verify.</param>
        private void EnsureSnapshotCausallyDerivedFromOldestTrackedSnapshot(Snapshot snapshot)
        {
            if (!(snapshot.Clock >= _oldestActiveClock))
            {
                throw new InvalidOperationException("Submitted write batch is based on a snapshot that doesn't causally " +
                    "derive from the oldest active clock.");
            }
        }

        /// <summary>
        /// Checks if a set of changes based on a given snapshot conflicts with recent modifications that may have
        /// occurrred after the baseline snapshot was taken.
        /// </summary>
        /// <param name="batch">Write batch to check for conflicts.</param>
        private void EnsureNoConcurrencyConflicts(WriteBatch batch)
        {
            if (batch.Snapshot == null)
            {
                throw new ArgumentException(nameof(batch));
            }

            foreach (var change in batch.Changes)
            {
                VectorClock lastModified;
                if (_recentlyModifiedKeys.TryGetValue(change.Key, out lastModified) &&
                    lastModified.Modification > batch.Snapshot.Clock.Modification)
                {
                    throw new OptimisticConcurrencyException(change.Key);
                }
            }
        }

        private void MarkKeysAsRecentlyModified(VectorClock clock, WriteBatch batch)
        {
            foreach (var change in batch.Changes)
            {
                _recentlyModifiedKeys[change.Key] = _clock;
            }
        }

        /// <summary>
        /// Purges tracked modifications from the internal dictionary that relate to changes that occurred at a point before the
        /// oldest snapshot was taken, mainly because we're never going to accept any changes based on snapshots older than that.
        /// </summary>
        private void StopTrackingModificationsBeforeOldestActiveSnapshot()
        {
            // Note that it is sufficient to track only modifications that occurred *after* the oldest live snapshot was taken,
            // hence the "<=" comparison
            foreach (var key in _recentlyModifiedKeys
                .Where(kvp => kvp.Value.Modification <= _oldestActiveClock.Modification)
                .Select(kvp => kvp.Key)
                .ToArray())
            {
                _recentlyModifiedKeys.Remove(key);
            }
        }
    }
}
