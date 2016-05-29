using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PutRecord
    {
        public RecordType Tag { get; set; }
        public short KeyLength { get; set; }
        public int ValueLength { get; set; }

        // Key data follows immediately, value data (payload) follows in data segment
    }
}
