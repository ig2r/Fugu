using System.Runtime.InteropServices;

namespace Fugu.DataFormat
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct StartDataRecord
    {
        public RecordType Tag { get; set; }

        // Values follow
    }
}
