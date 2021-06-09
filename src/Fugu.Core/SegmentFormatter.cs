using Fugu.Format;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Fugu
{
    public class SegmentFormatter
    {
        private readonly IBufferWriter<byte> _bufferWriter;

        public SegmentFormatter(IBufferWriter<byte> bufferWriter)
        {
            _bufferWriter = bufferWriter;
        }

        public void EmitHeader()
        {
            var headerSize = Unsafe.SizeOf<HeaderBlock>();
            var span = _bufferWriter.GetSpan(headerSize);
            ref var header = ref MemoryMarshal.AsRef<HeaderBlock>(span);
            header = new HeaderBlock { Magic = 0x11220011 };
            _bufferWriter.Advance(headerSize);
        }
    }
}
