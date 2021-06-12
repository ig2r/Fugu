using Fugu.Actors;
using System;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore : IAsyncDisposable
    {
        private readonly AllocationActor _allocationActor;
        private readonly WriterActor _writerActor;
        private readonly IndexActor _indexActor;
        private readonly SegmentStatsActor _segmentStatsActor;
        private readonly SnapshotsActor _snapshotsActor;
        private readonly CompactionActor _compactionActor;

        private readonly Channel<byte> _allocateWriteBatchChannel;
        private readonly Channel<byte> _writeWriteBatchChannel;
        private readonly Channel<byte> _updateIndexChannel;
        private readonly Channel<byte> _indexUpdatedChannel;
        private readonly Channel<byte> _acquireSnapshotChannel;
        private readonly Channel<byte> _releaseSnapshotChannel;
        private readonly Channel<byte> _updateSegmentStatsChannel;
        private readonly Channel<byte> _segmentStatsUpdatedChannel;
        private readonly Channel<byte> _segmentEmptiedChannel;
        private readonly Channel<byte> _segmentEvictedChannel;
        private readonly Channel<byte> _snapshotsUpdatedChannel;

        private KeyValueStore()
        {
            _allocateWriteBatchChannel = Channel.CreateUnbounded<byte>();
            _writeWriteBatchChannel = Channel.CreateUnbounded<byte>();
            _updateIndexChannel = Channel.CreateUnbounded<byte>();
            _indexUpdatedChannel = Channel.CreateUnbounded<byte>();
            _acquireSnapshotChannel = Channel.CreateUnbounded<byte>();
            _releaseSnapshotChannel = Channel.CreateUnbounded<byte>();
            _updateSegmentStatsChannel = Channel.CreateUnbounded<byte>();
            _segmentStatsUpdatedChannel = Channel.CreateUnbounded<byte>();
            _segmentEmptiedChannel = Channel.CreateUnbounded<byte>();
            _segmentEvictedChannel = Channel.CreateUnbounded<byte>();
            _snapshotsUpdatedChannel = Channel.CreateUnbounded<byte>();

            _allocationActor = new AllocationActor(
                _allocateWriteBatchChannel.Reader,
                _segmentEvictedChannel.Reader,
                _writeWriteBatchChannel.Writer);

            _writerActor = new WriterActor(
                _writeWriteBatchChannel.Reader,
                _updateIndexChannel.Writer);

            _indexActor = new IndexActor(
                _updateIndexChannel.Reader,
                _indexUpdatedChannel.Writer,
                _updateSegmentStatsChannel.Writer);

            _segmentStatsActor = new SegmentStatsActor(
                _updateSegmentStatsChannel.Reader,
                _segmentStatsUpdatedChannel.Writer,
                _segmentEmptiedChannel.Writer);

            _snapshotsActor = new SnapshotsActor(
                _acquireSnapshotChannel.Reader,
                _releaseSnapshotChannel.Reader,
                _indexUpdatedChannel.Reader,
                _snapshotsUpdatedChannel.Writer);

            _compactionActor = new CompactionActor(
                _segmentStatsUpdatedChannel.Reader,
                _segmentEmptiedChannel.Reader,
                _snapshotsUpdatedChannel.Reader,
                _updateIndexChannel.Writer,
                _segmentEvictedChannel.Writer);
        }

        public static Task<KeyValueStore> CreateAsync()
        {
            var store = new KeyValueStore();
            store.Start();
            return Task.FromResult(store);
        }

        public ValueTask<Snapshot> GetSnapshotAsync()
        {
            throw new NotImplementedException();
        }

        public ValueTask WriteAsync(WriteBatch batch)
        {
            // TODO: write batch instead of default
            return _allocateWriteBatchChannel.Writer.WriteAsync(default);
        }

        public ValueTask DisposeAsync()
        {
            // TODO: Implement graceful shutdown here
            _allocateWriteBatchChannel.Writer.Complete();
            _segmentEvictedChannel.Writer.Complete();

            return ValueTask.CompletedTask;
        }

        private void Start()
        {
            _ = Task.WhenAll(
                _allocationActor.ExecuteAsync(),
                _writerActor.ExecuteAsync(),
                _indexActor.ExecuteAsync(),
                _segmentStatsActor.ExecuteAsync(),
                _snapshotsActor.ExecuteAsync(),
                _compactionActor.ExecuteAsync());
        }
    }
}
