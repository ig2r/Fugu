using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct CommitRecord
    {
        public RecordType Tag { get; set; }
        public long Checksum { get; set; }
    }
}
