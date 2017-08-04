using System.Runtime.InteropServices;

namespace Fugu.IO.Records
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct CommitFooterRecord
    {
        public uint CommitChecksum { get; set; }
    }
}
