using Fugu.Actors;
using Fugu.Compaction;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Fugu.Core.Tests
{
    public class CompactionStrategyTests
    {
        [Fact]
        public void RatioCompactionStrategy_CompactEmptyRange_DeclinesToCompact()
        {
            // Arrange
            var strategy = new RatioCompactionStrategy(4096, 1.5);

            // Act
            var result = strategy.TryGetRangeToCompact(new SegmentStats[0], out var range);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void RatioCompactonStrategy_SustainedAdd_PassesBasicSanityChecks()
        {
            // Arrange
            const long minCapacity = 20;
            const double scaleFactor = 1.125;
            const int segmentCount = 100;

            // Choose higher scale factor to force recompactions during add
            var strategy = new RatioCompactionStrategy(minCapacity, scaleFactor + 1);

            long totalBytesAdded = 0;
            long totalBytesCompacted = 0;
            int totalCompactionCount = 0;
            var segmentStats = new List<SegmentStats>();

            // Act
            for (int i = 0; i < segmentCount; i++)
            {
                // Add segment, scaling segment size based on current total capacity just like partitioning actor does it
                var segmentSize = minCapacity + (long)(segmentStats.Sum(s => s.TotalBytes) * (scaleFactor - 1.0));
                segmentStats.Add(new SegmentStats(segmentSize, 0));
                totalBytesAdded += segmentSize;

                while (strategy.TryGetRangeToCompact(segmentStats, out var range))
                {
                    // Apply
                    var compacted = new SegmentStats(
                        segmentStats.Skip(range.Offset).Take(range.Count).Sum(s => s.LiveBytes),
                        0);

                    segmentStats.RemoveRange(range.Offset, range.Count);
                    segmentStats.Insert(range.Offset, compacted);
                    totalBytesCompacted += compacted.TotalBytes;
                    totalCompactionCount++;
                }
            }

            // Assert
            var writeAmplification = (double)(totalBytesAdded + totalBytesCompacted) / totalBytesAdded;

            Assert.True(totalCompactionCount <= segmentCount);
            Assert.InRange(writeAmplification, 1.0, 2.0);       // 2.0 is arbitrary upper bound we're willing to accept here
        }
    }
}
