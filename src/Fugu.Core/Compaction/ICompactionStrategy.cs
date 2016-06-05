using Fugu.Common;
using System.Collections.Generic;

namespace Fugu.Compaction
{
    /// <summary>
    /// A compaction strategy dictates when and how ranges of segments in an ordered hive should be
    /// merged in order to preserve or restore a certain invariant.
    /// </summary>
    public interface ICompactionStrategy
    {
        /// <summary>
        /// Provides a means for client code to check if segments in some given ordered hive should be
        /// merged together (compacted).
        /// </summary>
        /// <param name="segments">An ordered list of segments that make up the hive.</param>
        /// <param name="compactionRange">The range of segments to compact if required by the strategy.</param>
        /// <returns>Value indicating whether a compaction should be performed.</returns>
        bool TryGetCompactionRange(IReadOnlyList<SegmentStats> segments, out Range compactionRange);
    }
}
