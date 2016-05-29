using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Common
{
    public sealed class SegmentStats
    {
        public SegmentStats(long liveBytes, long deadBytes)
        {
            LiveBytes = liveBytes;
            DeadBytes = deadBytes;
        }

        public long LiveBytes { get; }
        public long DeadBytes { get; }
    }
}
