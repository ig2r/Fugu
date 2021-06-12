using Fugu.Messages;
using System.Buffers;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Serializes a WriteBatch into the assigned segment and passes the resulting offsets
    /// on for indexing.
    /// </summary>
    public class WriterActor
    {
        private readonly ChannelReader<WriteAllocatedBatchMessage> _writeAllocatedBatchChannelReader;
        private readonly ChannelWriter<byte> _updateIndexChannelWriter;

        public WriterActor(
            ChannelReader<WriteAllocatedBatchMessage> writeAllocatedBatchChannelReader,
            ChannelWriter<byte> updateIndexChannelWriter)
        {
            _writeAllocatedBatchChannelReader = writeAllocatedBatchChannelReader;
            _updateIndexChannelWriter = updateIndexChannelWriter;
        }

        public async Task ExecuteAsync()
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            var formatter = new SegmentFormatter(bufferWriter);
            formatter.EmitHeader();

            while (await _writeAllocatedBatchChannelReader.WaitToReadAsync())
            {
                var message = await _writeAllocatedBatchChannelReader.ReadAsync();
                message.CompletionSource.SetResult();
            }
        }
    }
}
