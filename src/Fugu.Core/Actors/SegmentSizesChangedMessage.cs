using Fugu.Common;
using System.Collections.Generic;

namespace Fugu.Actors
{
    public struct SegmentSizesChangedMessage
    {
        public SegmentSizesChangedMessage(
            StateVector clock,
            IReadOnlyList<KeyValuePair<Segment, SegmentSizeChange>> sizeChanges,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(sizeChanges, nameof(sizeChanges));

            Clock = clock;
            Index = index;
            SizeChanges = sizeChanges;
        }

        public StateVector Clock { get; }
        public IReadOnlyList<KeyValuePair<Segment, SegmentSizeChange>> SizeChanges { get; }
        public CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> Index { get; }
    }
}
