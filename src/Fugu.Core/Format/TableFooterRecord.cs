using System.Runtime.InteropServices;

namespace Fugu.Format
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct TableFooterRecord
    {
        public TableRecordType Tag { get; set; }
    }
}
