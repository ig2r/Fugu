using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Compaction
{
    /// <summary>
    /// Defines a compaction strategy based on the idea that for a hive of a total size S, the
    /// data should be distributed across at most n partitions, where n ~ log(S). When this
    /// invariant is violated, this strategy will pick a range of neighboring partitions for
    /// compaction so that the ratio of the sum of partitions in the range vs. the size of the
    /// leftmost partition is as large as possible, so that a lot of data is shifted towards an
    /// older generation during each compaction.
    /// </summary>
    public class RatioCompactionStrategy : ICompactionStrategy
    {
        public bool TryGetCompactionRange(IReadOnlyList<Segment> segments, out Range compactionRange)
        {
            compactionRange = default(Range);
            long totalLiveBytes = segments.Sum(s => s.LiveBytes);

            // Determine the optimal number of levels for the total number of live bytes currently
            // in the hive. This number will be logarithmic in the raw byte size. We add +1 within
            // the logarithm to ensure we always see a non-negative result even for 0 bytes in the
            // hive. We also add a fixed +4 additional levels on top to make sure we always have at
            // least 4 levels in the hive.
            int targetLevelCount = (int)Math.Log(totalLiveBytes / 100 + 1) + 4;

            if (segments.Count <= targetLevelCount)
            {
                // We're fine number-of-levels-wise
                return false;
            }

            double maxRatio = 0;
            long bytesInRange = 0;
            Range? range = null;

            for (int from = 0, to = 0; from < segments.Count - 1; from++)
            {
                // Extend considered range as far to the right as possible while keeping its total size
                // below 50% of the total hive size
                while (to < segments.Count && bytesInRange + segments[to].LiveBytes <= totalLiveBytes / 2)
                {
                    bytesInRange += segments[to++].LiveBytes;
                }

                bytesInRange -= segments[from].LiveBytes;

                if (to > from + 1)
                {
                    double ratio = (double)bytesInRange / (segments[from].LiveBytes + 1);
                    if (ratio > maxRatio)
                    {
                        maxRatio = ratio;
                        range = new Range(from, to - from);
                    }
                }
            }

            compactionRange = range.GetValueOrDefault();
            return range.HasValue;
        }
    }
}
