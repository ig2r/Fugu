using Fugu.Channels;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    /// <summary>
    /// Manages snapshots, which provide consumers an immutable, point-in-time view of the store's contents.
    /// </summary>
    public class SnapshotsActorCore
    {
        private readonly Channel<StateVector> _oldestVisibleStateChangedChannel;
        private readonly HashSet<Snapshot> _activeSnapshots = new HashSet<Snapshot>();

        private StateVector _clock;
        private Index _index = Index.Empty;

        private StateVector _oldestVisibleState = default(StateVector);

        public SnapshotsActorCore(Channel<StateVector> oldestVisibleStateChangedChannel)
        {
            Guard.NotNull(oldestVisibleStateChangedChannel, nameof(oldestVisibleStateChangedChannel));
            _oldestVisibleStateChangedChannel = oldestVisibleStateChangedChannel;
        }

        public async Task UpdateIndexAsync(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            // replyChannel may be null
            Guard.NotNull(index, nameof(index));

            _clock = StateVector.Max(_clock, clock);
            _index = index;

            // If no snapshot is currently active, this is now effectively the oldest state that could potentially ever be
            // retained by a snapshot now, so notify dependent actors accordingly
            if (_activeSnapshots.Count == 0)
            {
                _oldestVisibleState = _clock;
                await _oldestVisibleStateChangedChannel.SendAsync(_oldestVisibleState);
            }

            replyChannel?.SetResult(new VoidTaskResult());
        }

        /// <summary>
        /// Retrieves an immutable snapshot of the store.
        /// </summary>
        /// <param name="onDisposed">Delegate to execute when the consumer disposes of the snapshot.</param>
        /// <returns>Snapshot that lets consumers read from the store.</returns>
        public Snapshot GetSnapshot(Action<Snapshot> onDisposed)
        {
            var snapshot = new Snapshot(_clock, _index, onDisposed);
            _activeSnapshots.Add(snapshot);
            return snapshot;
        }

        public async Task OnSnapshotDisposedAsync(Snapshot snapshot)
        {
            if (_activeSnapshots.Remove(snapshot) && snapshot.Clock == _oldestVisibleState)
            {
                // The disposed snapshot was one of those which held the oldest store state visible, so we need to
                // check what the oldest visible state is now; if no snapshots are active, this will be the current
                // index' associated clock value, otherwise it's the clock value of the oldest snapshot
                var oldestNow = _activeSnapshots.Aggregate(_clock, (c, s) => s.Clock < c ? s.Clock : c);

                Debug.Assert(_oldestVisibleState <= oldestNow, "Oldest visible state is now older than before.");

                if (oldestNow != _oldestVisibleState)
                {
                    _oldestVisibleState = oldestNow;
                    await _oldestVisibleStateChangedChannel.SendAsync(_oldestVisibleState);
                }
            }
        }
    }
}
