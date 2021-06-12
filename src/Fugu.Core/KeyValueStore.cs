using Fugu.Actors;
using Fugu.Messages;
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

        private readonly Channel<AllocateWriteBatchMessage> _allocateWriteBatchChannel;
        private readonly Channel<WriteAllocatedBatchMessage> _writeAllocatedBatchChannel;
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
            _allocateWriteBatchChannel = Channel.CreateUnbounded<AllocateWriteBatchMessage>();
            _writeAllocatedBatchChannel = Channel.CreateUnbounded<WriteAllocatedBatchMessage>();
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
                _writeAllocatedBatchChannel.Writer);

            _writerActor = new WriterActor(
                _writeAllocatedBatchChannel.Reader,
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

        public async ValueTask WriteAsync(WriteBatch batch)
        {
            // TODO: write batch instead of default
            var completionSource = new TaskCompletionSource();
            var message = new AllocateWriteBatchMessage(batch, completionSource);
            await _allocateWriteBatchChannel.Writer.WriteAsync(message);
            await completionSource.Task;
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
