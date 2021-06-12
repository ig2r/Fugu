using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Receives the latest index from IndexActor and serves snapshot acquire/release requests.
    /// Through this, it knows the "horizon" vector clock value, i.e., the oldest clock value that
    /// anyone can still reference to. This value controls when a stale (dead) segment can be safely
    /// evicted.
    /// </summary>
    public class SnapshotsActor
    {
        private readonly SemaphoreSlim _semaphore = new(1);

        private readonly ChannelReader<byte> _acquireSnapshotChannelReader;
        private readonly ChannelReader<byte> _releaseSnapshotChannelReader;
        private readonly ChannelReader<byte> _indexUpdatedChannelReader;
        private readonly ChannelWriter<byte> _snapshotsUpdatedChannelWriter;

        public SnapshotsActor(
            ChannelReader<byte> acquireSnapshotChannelReader,
            ChannelReader<byte> releaseSnapshotChannelReader,
            ChannelReader<byte> indexUpdatedChannelReader,
            ChannelWriter<byte> snapshotsUpdatedChannelWriter)
        {
            _acquireSnapshotChannelReader = acquireSnapshotChannelReader;
            _releaseSnapshotChannelReader = releaseSnapshotChannelReader;
            _indexUpdatedChannelReader = indexUpdatedChannelReader;
            _snapshotsUpdatedChannelWriter = snapshotsUpdatedChannelWriter;
        }

        public Task ExecuteAsync()
        {
            return Task.WhenAll(
                HandleAcquireSnapshotAsync(),
                HandleReleaseSnapshotAsync(),
                HandleIndexUpdatedAsync());
        }

        private async Task HandleAcquireSnapshotAsync()
        {
            while (await _acquireSnapshotChannelReader.WaitToReadAsync())
            {
                await _semaphore.WaitAsync();
                var message = await _acquireSnapshotChannelReader.ReadAsync();
                _semaphore.Release();
            }
        }

        private async Task HandleReleaseSnapshotAsync()
        {
            while (await _releaseSnapshotChannelReader.WaitToReadAsync())
            {
                await _semaphore.WaitAsync();
                var message = await _releaseSnapshotChannelReader.ReadAsync();
                _semaphore.Release();
            }
        }

        private async Task HandleIndexUpdatedAsync()
        {
            while (await _indexUpdatedChannelReader.WaitToReadAsync())
            {
                await _semaphore.WaitAsync();
                var message = await _indexUpdatedChannelReader.ReadAsync();
                _semaphore.Release();
            }
        }
    }
}
