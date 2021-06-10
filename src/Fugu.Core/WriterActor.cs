using System.Buffers;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Serializes a WriteBatch into the assigned segment and passes the resulting offsets
    /// on for indexing.
    /// </summary>
    public class WriterActor
    {
        private readonly ChannelReader<WriteBatch> _input;

        public WriterActor(ChannelReader<WriteBatch> input)
        {
            _input = input;
        }

        public async Task ExecuteAsync()
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            var formatter = new SegmentFormatter(bufferWriter);

            formatter.EmitHeader();

            var writtenData = bufferWriter.WrittenSpan.ToArray();

            // Loop until input channel completes, signalling graceful termination
            while (await _input.WaitToReadAsync())
            {
                if (!_input.TryRead(out var batch))
                {
                    continue;
                }

            }
        }
    }
}
