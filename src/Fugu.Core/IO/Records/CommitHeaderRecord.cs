using System.Runtime.InteropServices;

namespace Fugu.IO.Records
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct CommitHeaderRecord
    {
        public TableRecordType Tag { get; set; }
        public int Count { get; set; }
    }
}
