using System;
using System.Collections.Generic;

namespace Fugu.Actors
{
    public struct SegmentSizeChange
    {
        public SegmentSizeChange(long liveBytesChange, long deadBytesChange)
        {
            LiveBytesChange = liveBytesChange;
            DeadBytesChange = deadBytesChange;
        }

        public long LiveBytesChange { get; }
        public long DeadBytesChange { get; }
    }
}
