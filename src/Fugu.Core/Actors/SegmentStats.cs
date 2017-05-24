using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Actors
{
    /// <summary>
    /// Usage statistics for an associated segment, i.e., the number of "live" and "dead" payload bytes within.
    /// </summary>
    public struct SegmentStats
    {
        public SegmentStats(long liveBytes, long deadBytes)
        {
            if (liveBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(liveBytes));
            }

            if (deadBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(deadBytes));
            }

            LiveBytes = liveBytes;
            DeadBytes = deadBytes;
        }

        public long LiveBytes { get; }
        public long DeadBytes { get; }
        public long TotalBytes => LiveBytes + DeadBytes;
    }
}
