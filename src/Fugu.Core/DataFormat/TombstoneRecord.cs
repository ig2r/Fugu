using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct TombstoneRecord
    {
        public RecordType Tag { get; set; }
        public short KeyLength { get; set; }

        // Key data follows immediately
    }
}
