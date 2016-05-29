using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct HeaderRecord
    {
        public long Magic { get; set; }
        public short FormatMajor { get; set; }
        public short FormatMinor { get; set; }
        public long MinGeneration { get; set; }
        public long MaxGeneration { get; set; }
        public long HeaderChecksum { get; set; }

        // Filled in when writing to a table finishes normally
        public long EndOfDataPosition { get; set; }
        public long EndOfDataChecksum { get; set; }
    }
}
