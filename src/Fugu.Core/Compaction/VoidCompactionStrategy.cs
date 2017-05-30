using Fugu.Actors;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Compaction
{
    public class VoidCompactionStrategy : ICompactionStrategy
    {
        #region ICompactionStrategy

        public bool TryGetRangeToCompact(IReadOnlyList<SegmentStats> segmentStats, out Range range)
        {
            range = default(Range);
            return false;
        }

        #endregion
    }
}
