using Fugu.Actors;
using Fugu.Bootstrapping;
using Fugu.Common;
using Fugu.Compaction;
using Fugu.Eviction;
using Fugu.Index;
using Fugu.Partitioning;
using Fugu.Snapshots;
using Fugu.Writer;
using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu
{
    /// <summary>
    /// A key-value store for <c>byte[]</c> keys and data.
    /// </summary>
    public sealed class KeyValueStore : IDisposable
    {
        internal const int DEFAULT_BOUNDED_CAPACITY = 512;

        private readonly PartitioningActorShell _partitioningActor;
        private readonly WriterActorShell _writerActor;
        private readonly IndexActorShell _indexActor;
        private readonly SnapshotsActorShell _snapshotsActor;
        private readonly CompactionActorShell _compactionActor;
        private readonly EvictionActorShell _evictionActor;
        private readonly IDisposable _balancingLinks;

        private KeyValueStore(
            PartitioningActorShell partitioningActor,
            WriterActorShell writerActor,
            IndexActorShell indexActor,
            SnapshotsActorShell snapshotsActor,
            CompactionActorShell compactionActor,
            EvictionActorShell evictionActor,
            IDisposable balancingLinks)
        {
            Guard.NotNull(partitioningActor, nameof(partitioningActor));
            Guard.NotNull(writerActor, nameof(writerActor));
            Guard.NotNull(indexActor, nameof(indexActor));
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));
            Guard.NotNull(compactionActor, nameof(compactionActor));
            Guard.NotNull(evictionActor, nameof(evictionActor));
            Guard.NotNull(balancingLinks, nameof(balancingLinks));

            _partitioningActor = partitioningActor;
            _writerActor = writerActor;
            _indexActor = indexActor;
            _snapshotsActor = snapshotsActor;
            _compactionActor = compactionActor;
            _evictionActor = evictionActor;
            _balancingLinks = balancingLinks;
        }

        /// <summary>
        /// Creates a new instance of the <see cref="KeyValueStore"/> class backed by the given table set.
        /// </summary>
        /// <param name="tableSet">Backing table set underlying the store instance.</param>
        /// <returns>An asynchronoous operation that returns a fully initialized store instance upon completion.</returns>
        public static async Task<KeyValueStore> CreateAsync(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));

            // Set up blocks to broadcast balancing information once the store is up
            var segmentCreatedBuffer = new BufferBlock<Segment>();
            var segmentStatsChangedBroadcast = new BroadcastBlock<SegmentStatsChangedMessage>(msg => msg);
            var oldestVisibleStateBroadcast = new BroadcastBlock<StateVector>(clock => clock);

            // Create actors managing store state
            var snapshotsActor = new SnapshotsActorShell(
                new SnapshotsActorCore(oldestVisibleStateBroadcast));

            var indexActor = new IndexActorShell(
                new IndexActorCore(snapshotsActor.SnapshotsUpdateBlock, segmentStatsChangedBroadcast));

            // Bootstrap store state from given table set
            var bootstrapper = new Bootstrapper();
            var bootstrapResult = await bootstrapper.RunAsync(
                tableSet, indexActor.UpdateIndexBlock, segmentCreatedBuffer).ConfigureAwait(false);

            // Create actors accepting new writes to the store
            var writerActor = new WriterActorShell(
                new WriterActorCore(indexActor.UpdateIndexBlock, segmentCreatedBuffer));

            var partitioningActor = new PartitioningActorShell(
                new PartitioningActorCore(bootstrapResult.MaxGeneration, bootstrapResult.TotalCapacity, tableSet, writerActor.WriteBlock));

            // Create actors enforcing balance invariants
            var evictionActor = new EvictionActorShell(
                new EvictionActorCore(tableSet));

            var compactionActor = new CompactionActorShell(
                new CompactionActorCore(
                    new RatioCompactionStrategy(1024 * 1024, 2.0),
                    tableSet,
                    evictionActor.EvictSegmentBlock,
                    partitioningActor.TotalCapacityChangedBlock,
                    indexActor.UpdateIndexBlock));

            // Connect blocks that relay information on store stats so that compaction and eviction of empty
            // blocks can commence
            var segmentCreatedLink = segmentCreatedBuffer.LinkTo(compactionActor.SegmentCreatedBlock);
            var segmentStatsChangeLink = segmentStatsChangedBroadcast.LinkTo(compactionActor.SegmentStatsChangedBlock);
            var oldestVisibleStateLink = oldestVisibleStateBroadcast.LinkTo(evictionActor.OldestVisibleStateChangedBlock);

            var balancingLinks = new Disposable(() =>
            {
                segmentCreatedLink.Dispose();
                segmentStatsChangeLink.Dispose();
                oldestVisibleStateLink.Dispose();
            });

            // From these components, create the store object itself
            var store = new KeyValueStore(
                partitioningActor,
                writerActor,
                indexActor,
                snapshotsActor,
                compactionActor,
                evictionActor,
                balancingLinks);

            return store;
        }

        /// <summary>
        /// Acquires an immutable, point-in-time snapshot of the store's contents.
        /// </summary>
        /// <returns>The resulting snapshot of the store, represented as an asynchronous operation.</returns>
        public Task<Snapshot> GetSnapshotAsync()
        {
            var replyChannel = new TaskCompletionSource<Snapshot>();
            var accepted = _snapshotsActor.GetSnapshotBlock.Post(replyChannel);
            Debug.Assert(accepted, "Posting message to retrieve a store snapshot must always succeed.");

            return replyChannel.Task;
        }

        /// <summary>
        /// Accepts a set of related changes and commits it atomically to the store.
        /// </summary>
        /// <param name="writeBatch">A group of puts/deletions to commit as a group.</param>
        /// <returns>Asynchronous operation tracking successful completion of the commit.</returns>
        public Task CommitAsync(WriteBatch writeBatch)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));

            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            var accepted = _partitioningActor.CommitWriteBatchBlock.Post(new CommitWriteBatchMessage(writeBatch, replyChannel));
            Debug.Assert(accepted, "Posting a CommitWriteBatchMessage must always succeed.");

            return replyChannel.Task;
        }

        /// <summary>
        /// Closes down the store instance to a consistent, stable state.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CloseAsync()
        {
            // Cut links from state-managing actors to balancing actors so that they no longer receive updates about
            // changes to the core store state
            _balancingLinks.Dispose();

            // Complete balancing actors and wait for compaction/eviction activities to cease
            _compactionActor.Complete();
            await _compactionActor.Completion;

            _evictionActor.Complete();
            await _evictionActor.Completion;

            // Complete commit/writer actors so that no more changes will be accepted to the store
            _partitioningActor.Complete();
            await _partitioningActor.Completion;

            _writerActor.Complete();
            await _writerActor.Completion;

            // Complete core state-managing actors (index & snapshots)
            _indexActor.Complete();
            await _indexActor.Completion;

            _snapshotsActor.Complete();
            await _snapshotsActor.Completion;
        }

        #region IDisposable

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden.
            // GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                // TODO: dispose managed state (managed objects).
            }

            // TODO: free unmanaged resources (unmanaged objects) and override a finalizer.
            // TODO: set large fields to null.
        }

        #endregion
    }
}
