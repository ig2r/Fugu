using System.Runtime.InteropServices;

namespace Fugu.Format
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct TombstoneRecord
    {
        public CommitRecordType Tag { get; set; }
        public short KeyLength { get; set; }

        // Key data follows immediately
    }
}
