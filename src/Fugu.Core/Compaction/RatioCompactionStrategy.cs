using Fugu.Actors;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Compaction
{
    /// <summary>
    /// Compaction strategy that reduces the number of segments in the store by merging adjacent segments
    /// while minimizing the number of bytes copied during each merge.
    /// </summary>
    public class RatioCompactionStrategy : ICompactionStrategy
    {
        // No matter how little live data is in the store, the strategy will always allow to have at least
        // this many segments around. Must be a positive integer.
        private const int MIN_SEGMENT_COUNT = 2;

        // The maximum number of segments the strategy is allowed to merge in one operation. Must be at least 2.
        private const int MAX_SEGMENT_MERGE_COUNT = 3;

        private readonly long _minCapacity;
        private readonly double _scaleFactor;

        public RatioCompactionStrategy(long minCapacity, double scaleFactor)
        {
            if (minCapacity <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minCapacity));
            }

            if (scaleFactor < 1 || double.IsNaN(scaleFactor))
            {
                throw new ArgumentOutOfRangeException(nameof(scaleFactor));
            }

            _minCapacity = minCapacity;
            _scaleFactor = scaleFactor;
        }

        #region ICompactionStrategy

        public bool TryGetRangeToCompact(IReadOnlyList<SegmentStats> segmentStats, out Range range)
        {
            Guard.NotNull(segmentStats, nameof(segmentStats));

            // Do we actually need to compact?
            if (IsBalanced(segmentStats))
            {
                range = default(Range);
                return false;
            }

            // Determine which range of segments to merge while copying as little data as possible
            long bestCost = long.MaxValue;
            int bestOffset = -1;
            int bestCount = 0;

            for (int i = 0; i < segmentStats.Count - 1; i++)
            {
                var liveBytesInRange = segmentStats[i].LiveBytes;

                for (int j = i + 1; j < i + MAX_SEGMENT_MERGE_COUNT && j < segmentStats.Count; j++)
                {
                    liveBytesInRange += segmentStats[j].LiveBytes;

                    // Figure out how much data we would need to copy if we merged this segment range
                    var cost = liveBytesInRange / (j - i);
                    if (cost < bestCost)
                    {
                        bestCost = cost;
                        bestOffset = i;
                        bestCount = j - i + 1;
                    }
                }
            }

            range = new Range(bestOffset, bestCount);
            return true;
        }

        #endregion

        private bool IsBalanced(IReadOnlyList<SegmentStats> segmentStats)
        {
            // Calculate the total of live bytes across all segments eligible for compaction
            var liveBytes = segmentStats.Sum(s => s.LiveBytes);

            // Derive limit on number of segments depending on number of live bytes
            var maxSegments = Math.Max(
                Math.Log(1 + liveBytes / _minCapacity, _scaleFactor),
                MIN_SEGMENT_COUNT);

            // Segments are in balance if their number does not exceed the set threshold
            return segmentStats.Count <= maxSegments;
        }
    }
}
