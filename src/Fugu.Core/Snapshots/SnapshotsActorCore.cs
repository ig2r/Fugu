﻿using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using StoreIndex = Fugu.Common.AaTree<Fugu.IndexEntry>;

namespace Fugu.Snapshots
{
    /// <summary>
    /// Manages snapshots, which provide consumers an immutable, point-in-time view of the store's contents.
    /// </summary>
    public class SnapshotsActorCore
    {
        private readonly ITargetBlock<StateVector> _oldestVisibleStateChangedBlock;
        private readonly HashSet<Snapshot> _activeSnapshots = new HashSet<Snapshot>();

        private StateVector _clock;
        private StoreIndex _index = StoreIndex.Empty;

        private StateVector _oldestVisibleState = default(StateVector);

        public SnapshotsActorCore(ITargetBlock<StateVector> oldestVisibleStateChangedBlock)
        {
            Guard.NotNull(oldestVisibleStateChangedBlock, nameof(oldestVisibleStateChangedBlock));
            _oldestVisibleStateChangedBlock = oldestVisibleStateChangedBlock;
        }

        public Task UpdateIndexAsync(StateVector clock, StoreIndex index, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            // replyChannel may be null
            Guard.NotNull(index, nameof(index));

            _clock = StateVector.Max(_clock, clock);
            _index = index;

            replyChannel?.SetResult(new VoidTaskResult());

            // If no snapshot is currently active, this is now effectively the oldest state that could potentially ever be
            // retained by a snapshot now, so notify dependent actors accordingly
            if (_activeSnapshots.Count == 0)
            {
                _oldestVisibleState = _clock;
                _oldestVisibleStateChangedBlock.Post(_oldestVisibleState);
            }

            return Task.CompletedTask;
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

        public Task OnSnapshotDisposedAsync(Snapshot snapshot)
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
                    _oldestVisibleStateChangedBlock.Post(_oldestVisibleState);
                }
            }

            return Task.CompletedTask;
        }
    }
}
