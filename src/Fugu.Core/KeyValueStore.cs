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
using System.Threading.Tasks;

namespace Fugu
{
    public sealed class KeyValueStore : IDisposable
    {
        private readonly IPartitioningActor _partitioningActor;
        private readonly IWriterActor _writerActor;
        private readonly IIndexActor _indexActor;
        private readonly ISnapshotsActor _snapshotsActor;
        private readonly ICompactionActor _compactionActor;

        private readonly Channel<TaskCompletionSource<Snapshot>> _getSnapshotChannel;
        private readonly Channel<CommitWriteBatchMessage> _commitWriteBatchChannel;

        private KeyValueStore(
            IPartitioningActor partitioningActor,
            IWriterActor writerActor,
            IIndexActor indexActor,
            ISnapshotsActor snapshotsActor,
            ICompactionActor compactionActor,
            Channel<TaskCompletionSource<Snapshot>> getSnapshotChannel,
            Channel<CommitWriteBatchMessage> commitWriteBatchChannel)
        {
            Guard.NotNull(partitioningActor, nameof(partitioningActor));
            Guard.NotNull(writerActor, nameof(writerActor));
            Guard.NotNull(indexActor, nameof(indexActor));
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));
            Guard.NotNull(compactionActor, nameof(compactionActor));
            Guard.NotNull(getSnapshotChannel, nameof(getSnapshotChannel));
            Guard.NotNull(commitWriteBatchChannel, nameof(commitWriteBatchChannel));

            _partitioningActor = partitioningActor;
            _writerActor = writerActor;
            _indexActor = indexActor;
            _snapshotsActor = snapshotsActor;
            _compactionActor = compactionActor;
            _getSnapshotChannel = getSnapshotChannel;
            _commitWriteBatchChannel = commitWriteBatchChannel;
        }

        public static async Task<KeyValueStore> CreateAsync(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));

            // Create channels that will transmit messages between actors
            var updateIndexChannel = new UnbufferedChannel<UpdateIndexMessage>();
            var snapshotsUpdateChannel = new UnbufferedChannel<SnapshotsUpdateMessage>();
            var getSnapshotChannel = new UnbufferedChannel<TaskCompletionSource<Snapshot>>();
            var writeChannel = new UnbufferedChannel<CommitWriteBatchToSegmentMessage>();
            var commitWriteBatchChannel = new UnbufferedChannel<CommitWriteBatchMessage>();
            var totalCapacityChangedChannel = new UnbufferedChannel<TotalCapacityChangedMessage>();
            var segmentSizesChangedChannel = new UnbufferedChannel<SegmentSizesChangedMessage>();
            var evictSegmentChannel = new UnbufferedChannel<EvictSegmentMessage>();
            var oldestVisibleStateChangedChannel = new UnbufferedChannel<StateVector>();

            // Create actors managing store state
            var snapshotsActor = new SnapshotsActorShell(
                new SnapshotsActorCore(oldestVisibleStateChangedChannel),
                snapshotsUpdateChannel, getSnapshotChannel);
            snapshotsActor.Run();

            var indexActor = new IndexActorShell(
                new IndexActorCore(snapshotsUpdateChannel, segmentSizesChangedChannel),
                updateIndexChannel);
            indexActor.Run();

            var compactionActor = new CompactionActorShell(
                new CompactionActorCore(
                    //new VoidCompactionStrategy(),
                    new AlwaysCompactCompactionStrategy(),
                    tableSet,
                    evictSegmentChannel,
                    totalCapacityChangedChannel,
                    updateIndexChannel),
                segmentSizesChangedChannel);
            compactionActor.Run();

            var evictionActor = new EvictionActorShell(
                new EvictionActorCore(tableSet),
                evictSegmentChannel, oldestVisibleStateChangedChannel);
            evictionActor.Run();

            // Bootstrap store state from given table set
            var bootstrapper = new Bootstrapper();
            var result = await bootstrapper.RunAsync(tableSet, indexActor, updateIndexChannel);

            // Create actors accepting new writes to the store
            var writerActor = new WriterActorShell(
                new WriterActorCore(result.MaxGenerationLoaded, updateIndexChannel),
                writeChannel);
            writerActor.Run();

            var partitioningActor = new PartitioningActorShell(
                new PartitioningActorCore(tableSet, writeChannel),
                commitWriteBatchChannel,
                totalCapacityChangedChannel);
            partitioningActor.Run();

            // From these components, create the store object itself
            var store = new KeyValueStore(
                partitioningActor, writerActor, indexActor, snapshotsActor, compactionActor,
                getSnapshotChannel, commitWriteBatchChannel);

            return store;
        }

        public Task<Snapshot> GetSnapshotAsync()
        {
            var replyChannel = new TaskCompletionSource<Snapshot>();
            _getSnapshotChannel.SendAsync(replyChannel);
            return replyChannel.Task;
        }

        public Task CommitAsync(WriteBatch writeBatch)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            _commitWriteBatchChannel.SendAsync(new CommitWriteBatchMessage(writeBatch, replyChannel));
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
