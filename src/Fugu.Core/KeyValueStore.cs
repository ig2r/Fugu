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
    /// A key-value store for <c>byte[]</c> payloads.
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
            var bootstrapResult = await bootstrapper.RunAsync(tableSet, indexActor.UpdateIndexBlock).ConfigureAwait(false);

            // Create actors accepting new writes to the store
            var writerActor = new WriterActorShell(
                new WriterActorCore(bootstrapResult.MaxGenerationLoaded, indexActor.UpdateIndexBlock, segmentCreatedBuffer));

            var partitioningActor = new PartitioningActorShell(
                new PartitioningActorCore(tableSet, writerActor.WriteBlock));

            // Create actors enforcing balance invariants
            var evictionActor = new EvictionActorShell(
                new EvictionActorCore(tableSet));

            var compactionActor = new CompactionActorShell(
                new CompactionActorCore(
                    new RatioCompactionStrategy(4096, 2.0),
                    tableSet,
                    bootstrapResult.LoadedSegments,
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

        public Task<Snapshot> GetSnapshotAsync()
        {
            var replyChannel = new TaskCompletionSource<Snapshot>();
            var accepted = _snapshotsActor.GetSnapshotBlock.Post(replyChannel);
            Debug.Assert(accepted, "Posting message to retrieve a store snapshot must always succeed.");

            return replyChannel.Task;
        }

        public Task CommitAsync(WriteBatch writeBatch)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));

            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            var accepted = _partitioningActor.CommitWriteBatchBlock.Post(new CommitWriteBatchMessage(writeBatch, replyChannel));
            Debug.Assert(accepted, "Posting a CommitWriteBatchMessage must always succeed.");

            return replyChannel.Task;
        }

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
