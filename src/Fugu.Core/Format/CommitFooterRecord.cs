﻿using System.Runtime.InteropServices;

namespace Fugu.Format
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct CommitFooterRecord
    {
        public uint CommitChecksum { get; set; }
    }
}
