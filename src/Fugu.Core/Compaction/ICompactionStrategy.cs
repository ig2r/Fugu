using Fugu.Actors;
using Fugu.Common;
using System.Collections.Generic;

namespace Fugu.Compaction
{
    public interface ICompactionStrategy
    {
        bool TryGetRangeToCompact(IReadOnlyList<SegmentStats> segmentStats, out Range range);
    }
}
