using Fugu.Common;

namespace Fugu.Eviction
{
    public struct EvictSegmentMessage
    {
        public EvictSegmentMessage(StateVector evictAt, Segment segment)
        {
            Guard.NotNull(segment, nameof(segment));
            EvictAt = evictAt;
            Segment = segment;
        }

        public StateVector EvictAt { get; }
        public Segment Segment { get; }
    }
}
