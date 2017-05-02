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
    public sealed class KeyValueStore : IDisposable
    {
        internal const int DEFAULT_BOUNDED_CAPACITY = 512;

        private readonly ITargetBlock<TaskCompletionSource<Snapshot>> _getSnapshotBlock;
        private readonly ITargetBlock<CommitWriteBatchMessage> _commitWriteBatchBlock;

        private KeyValueStore(
            ITargetBlock<TaskCompletionSource<Snapshot>> getSnapshotBlock,
            ITargetBlock<CommitWriteBatchMessage> commitWriteBatchBlock)
        {
            Guard.NotNull(getSnapshotBlock, nameof(getSnapshotBlock));
            Guard.NotNull(commitWriteBatchBlock, nameof(commitWriteBatchBlock));

            _getSnapshotBlock = getSnapshotBlock;
            _commitWriteBatchBlock = commitWriteBatchBlock;
        }

        public static async Task<KeyValueStore> CreateAsync(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));

            // Set up buffers we need to facilitate upward links
            var totalCapacityChangedBuffer = new BufferBlock<TotalCapacityChangedMessage>(
                new DataflowBlockOptions { BoundedCapacity = DEFAULT_BOUNDED_CAPACITY });
            var updateIndexBuffer = new BufferBlock<UpdateIndexMessage>(
                new DataflowBlockOptions { BoundedCapacity = DEFAULT_BOUNDED_CAPACITY });

            // Create actors managing store state
            var evictionActor = new EvictionActorShell(
                new EvictionActorCore(tableSet));

            var compactionActor = new CompactionActorShell(
                new CompactionActorCore(
                    new AlwaysCompactCompactionStrategy(),
                    //new VoidCompactionStrategy(),
                    tableSet,
                    evictionActor.EvictSegmentBlock,
                    totalCapacityChangedBuffer,
                    updateIndexBuffer));

            var snapshotsActor = new SnapshotsActorShell(
                new SnapshotsActorCore(evictionActor.OldestVisibleStateChangedBlock));

            var indexActor = new IndexActorShell(
                new IndexActorCore(snapshotsActor.SnapshotsUpdateBlock, compactionActor.SegmentSizesChangedBlock));

            // Bootstrap store state from given table set
            var bootstrapper = new Bootstrapper();
            var result = await bootstrapper.RunAsync(tableSet, indexActor.UpdateIndexBlock).ConfigureAwait(false);

            // Create actors accepting new writes to the store
            var writerActor = new WriterActorShell(
                new WriterActorCore(result.MaxGenerationLoaded, indexActor.UpdateIndexBlock));

            var partitioningActor = new PartitioningActorShell(
                new PartitioningActorCore(tableSet, writerActor.WriteBlock));

            totalCapacityChangedBuffer.LinkTo(partitioningActor.TotalCapacityChangedBlock);
            updateIndexBuffer.LinkTo(indexActor.UpdateIndexBlock);

            // From these components, create the store object itself
            var store = new KeyValueStore(snapshotsActor.GetSnapshotBlock, partitioningActor.CommitWriteBatchBlock);

            return store;
        }

        public Task<Snapshot> GetSnapshotAsync()
        {
            var replyChannel = new TaskCompletionSource<Snapshot>();
            var accepted = _getSnapshotBlock.Post(replyChannel);
            Debug.Assert(accepted, "Posting message to retrieve a store snapshot must always succeed.");

            return replyChannel.Task;
        }

        public Task CommitAsync(WriteBatch writeBatch)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));

            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            var accepted = _commitWriteBatchBlock.Post(new CommitWriteBatchMessage(writeBatch, replyChannel));
            Debug.Assert(accepted, "Posting a CommitWriteBatchMessage must always succeed.");

            return replyChannel.Task;
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
