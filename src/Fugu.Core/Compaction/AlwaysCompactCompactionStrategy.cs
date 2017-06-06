using Fugu.Actors;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Compaction
{
    public class AlwaysCompactCompactionStrategy : ICompactionStrategy
    {
        #region ICompactionStrategy

        public bool TryGetRangeToCompact(IReadOnlyList<SegmentStats> segmentStats, out Range range)
        {
            if (segmentStats.Count >= 2)
            {
                range = new Range(0, segmentStats.Count);
                return true;
            }
            else
            {
                range = default(Range);
                return false;
            }
        }

        #endregion
    }
}
