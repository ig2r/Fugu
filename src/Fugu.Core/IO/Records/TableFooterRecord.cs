using System.Runtime.InteropServices;

namespace Fugu.IO.Records
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct TableFooterRecord
    {
        public TableRecordType Tag { get; set; }
    }
}
