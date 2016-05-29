using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct FooterRecord
    {
        public RecordType Tag { get; set; }
        public long Magic { get; set; }

        // File ends here
    }
}
