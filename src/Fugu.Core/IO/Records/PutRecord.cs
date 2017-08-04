using System.Runtime.InteropServices;

namespace Fugu.IO.Records
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct PutRecord
    {
        public CommitRecordType Tag { get; set; }
        public short KeyLength { get; set; }
        public int ValueLength { get; set; }

        // Key data follows immediately, value data (payload) follows after all puts/tombstones for commit have been declared
    }
}
