using Fugu.Common;
using System.Collections.Generic;

namespace Fugu.Actors
{
    public struct SegmentStatsChangedMessage
    {
        public SegmentStatsChangedMessage(
            StateVector clock,
            IReadOnlyList<KeyValuePair<Segment, SegmentStats>> stats,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(stats, nameof(stats));

            Clock = clock;
            Index = index;
            Stats = stats;
        }

        public StateVector Clock { get; }
        public IReadOnlyList<KeyValuePair<Segment, SegmentStats>> Stats { get; }
        public CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> Index { get; }
    }
}
