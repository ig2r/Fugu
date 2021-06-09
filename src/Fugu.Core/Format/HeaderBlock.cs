using System.Runtime.InteropServices;

namespace Fugu.Format
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct HeaderBlock
    {
        public ulong Magic { get; set; }
    }
}
