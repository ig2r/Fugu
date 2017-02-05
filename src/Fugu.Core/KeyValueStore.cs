using Fugu.Actors;
using Fugu.Bootstrapping;
using Fugu.Common;
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

        private KeyValueStore(
            IPartitioningActor partitioningActor,
            IWriterActor writerActor,
            IIndexActor indexActor,
            ISnapshotsActor snapshotsActor)
        {
            Guard.NotNull(partitioningActor, nameof(partitioningActor));
            Guard.NotNull(writerActor, nameof(writerActor));
            Guard.NotNull(indexActor, nameof(indexActor));
            Guard.NotNull(snapshotsActor, nameof(snapshotsActor));

            _partitioningActor = partitioningActor;
            _writerActor = writerActor;
            _indexActor = indexActor;
            _snapshotsActor = snapshotsActor;
        }

        public static async Task<KeyValueStore> CreateAsync(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));

            // Create actors managing store state
            var snapshotsActor = new SnapshotsActorShell(new SnapshotsActorCore());
            var indexActor = new IndexActorShell(new IndexActorCore(snapshotsActor));

            // Bootstrap store state from given table set
            var bootstrapper = new Bootstrapper();
            var result = await bootstrapper.RunAsync(tableSet, indexActor);

            // Create actors accepting new writes to the store
            var writerActor = new WriterActorShell(new WriterActorCore(indexActor, result.MaxGenerationLoaded));
            var partitioningActor = new PartitioningActorShell(new PartitioningActorCore(tableSet, writerActor));

            var store = new KeyValueStore(partitioningActor, writerActor, indexActor, snapshotsActor);
            return store;
        }

        public Task<Snapshot> GetSnapshotAsync()
        {
            var replyChannel = new TaskCompletionSource<Snapshot>();
            _snapshotsActor.GetSnapshot(replyChannel);
            return replyChannel.Task;
        }

        public Task CommitAsync(WriteBatch writeBatch)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            var replyChannel = new TaskCompletionSource<VoidTaskResult>();
            _partitioningActor.Commit(writeBatch, replyChannel);
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
