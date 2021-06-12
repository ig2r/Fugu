using Fugu.Messages;
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

        private readonly ChannelReader<AllocateWriteBatchMessage> _allocateWriteBatchChannelReader;
        private readonly ChannelReader<byte> _segmentEvictedChannelReader;
        private readonly ChannelWriter<WriteAllocatedBatchMessage> _writeAllocatedBatchChannelWriter;

        private Segment? _segment = null;

        public AllocationActor(
            ChannelReader<AllocateWriteBatchMessage> allocateWriteBatchChannelReader,
            ChannelReader<byte> segmentEvictedChannelReader,
            ChannelWriter<WriteAllocatedBatchMessage> writeAllocatedBatchChannelWriter)
        {
            _allocateWriteBatchChannelReader = allocateWriteBatchChannelReader;
            _segmentEvictedChannelReader = segmentEvictedChannelReader;
            _writeAllocatedBatchChannelWriter = writeAllocatedBatchChannelWriter;
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

                if (_segment == null)
                {
                    _segment = new Segment { Generation = 1 };
                }
                
                var message = await _allocateWriteBatchChannelReader.ReadAsync();
                await _writeAllocatedBatchChannelWriter.WriteAsync(
                    new WriteAllocatedBatchMessage(message.Batch, _segment, message.CompletionSource));
                
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
