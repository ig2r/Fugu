using System.Runtime.InteropServices;

namespace Fugu.Format
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct TableHeaderRecord
    {
        public ulong Magic { get; set; }
        public ushort FormatVersionMajor { get; set; }
        public ushort FormatVersionMinor { get; set; }
        public long MinGeneration { get; set; }
        public long MaxGeneration { get; set; }
        public uint HeaderChecksum { get; set; }
    }
}
