using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    /// <summary>
    /// Manages snapshots, which let clients retrieve data from the store.
    /// </summary>
    public class SnapshotsActorCore
    {
        private readonly HashSet<Snapshot> _activeSnapshots = new HashSet<Snapshot>();

        private StateVector _clock;
        private Index _index = Index.Empty;

        #region ISnapshotsActor

        public void UpdateIndex(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            _clock = StateVector.Max(_clock, clock);
            _index = index;

            replyChannel.SetResult(new VoidTaskResult());
        }

        /// <summary>
        /// Retrieves an immutable snapshot of the store.
        /// </summary>
        /// <returns>Snapshot that lets consumers read from the store.</returns>
        public Snapshot GetSnapshot()
        {
            var snapshot = new Snapshot(_clock, _index, OnSnapshotDisposed);

            lock (_activeSnapshots)
            {
                _activeSnapshots.Add(snapshot);
            }

            return snapshot;
        }

        #endregion

        private void OnSnapshotDisposed(Snapshot snapshot)
        {
            lock (_activeSnapshots)
            {
                _activeSnapshots.Remove(snapshot);
            }
        }
    }
}
