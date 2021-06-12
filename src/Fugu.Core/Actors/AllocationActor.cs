using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Assigns a WriteBatch to a sufficiently-sized segment. Keeps track of:
    /// - the number of current segments (OR: total size of all segments?), to determine min size for the next;
    /// - remaining space within the current segment
    /// </summary>
    public class AllocationActor
    {
        private readonly SemaphoreSlim _semaphore = new(1);

        private readonly ChannelReader<byte> _allocateWriteBatchChannelReader;
        private readonly ChannelReader<byte> _segmentEvictedChannelReader;
        private readonly ChannelWriter<byte> _writeWriteBatchChannelWriter;

        public AllocationActor(
            ChannelReader<byte> allocateWriteBatchChannelReader,
            ChannelReader<byte> segmentEvictedChannelReader,
            ChannelWriter<byte> writeWriteBatchChannelWriter)
        {
            _allocateWriteBatchChannelReader = allocateWriteBatchChannelReader;
            _segmentEvictedChannelReader = segmentEvictedChannelReader;
            _writeWriteBatchChannelWriter = writeWriteBatchChannelWriter;
        }

        public Task ExecuteAsync()
        {
            return Task.WhenAll(
                HandleAllocateWriteBatchAsync(),
                HandleSegmentEvictedAsync());
        }

        private async Task HandleAllocateWriteBatchAsync()
        {
            while (await _allocateWriteBatchChannelReader.WaitToReadAsync())
            {
                await _semaphore.WaitAsync();
                var message = await _allocateWriteBatchChannelReader.ReadAsync();
                _semaphore.Release();
            }
        }

        private async Task HandleSegmentEvictedAsync()
        {
            while (await _segmentEvictedChannelReader.WaitToReadAsync())
            {
                await _semaphore.WaitAsync();
                var message = await _segmentEvictedChannelReader.ReadAsync();
                _semaphore.Release();
            }
        }
    }
}
