using System.Collections.Generic;
using Fugu.Common;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public interface ICompactionActor
    {
        void RegisterCompactableSegment(VectorClock clock, Segment segment);
        void OnIndexUpdated(
            IIndexActor sender,
            VectorClock clock,
            IReadOnlyList<IndexEntry> addedEntries,
            IReadOnlyList<IndexEntry> removedEntries,
            Index index);
    }
}