using Fugu.Actors;
using Fugu.Common;
using Fugu.Compaction;
using System;
using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore : IDisposable
    {
        private readonly ITableSet _tableSet;

        private MvccActor _mvccActor;
        private WriterActor _writerActor;
        private IndexActor _indexActor;
        private CompactionActor _compactionActor;
        private SnapshotsActor _snapshotsActor;
        private EvictionActor _evictionActor;

        private KeyValueStore(ITableSet tableSet)
        {
            _tableSet = tableSet;
        }

        public static Task<KeyValueStore> CreateAsync(ITableSet tableSet)
        {
            Guard.NotNull(tableSet, nameof(tableSet));
            var store = new KeyValueStore(tableSet);
            return store.InitializeAsync();
        }

        public Task<Snapshot> GetSnapshotAsync()
        {
            return _snapshotsActor.GetSnapshotAsync();
        }

        public Task CommitAsync(WriteBatch batch)
        {
            Guard.NotNull(batch, nameof(batch));
            return _mvccActor.CommitAsync(batch);
        }

        #region IDisposable

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden.
            // GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // TODO: dispose managed state (managed objects).
            }

            // TODO: free unmanaged resources (unmanaged objects) and override a finalizer.
            // TODO: set large fields to null.
        }

        #endregion

        private Task<KeyValueStore> InitializeAsync()
        {
            _evictionActor = new EvictionActor(_tableSet);
            _snapshotsActor = new SnapshotsActor();
            _compactionActor = new CompactionActor(new RatioCompactionStrategy(), _tableSet, _evictionActor);
            _indexActor = new IndexActor(_snapshotsActor, _compactionActor);
            _writerActor = new WriterActor(_tableSet, _compactionActor, _indexActor);
            _mvccActor = new MvccActor(_writerActor);

            _compactionActor.CompactableBytesChanged += _writerActor.OnCompactableBytesChanged;
            _snapshotsActor.OldestLiveSnapshotChanged += _mvccActor.OnOldestLiveSnapshotChanged;
            _snapshotsActor.OldestLiveSnapshotChanged += _evictionActor.OnOldestLiveSnapshotChanged;

            return Task.FromResult(this);
        }
    }
}
