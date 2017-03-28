using Fugu.Actors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Compaction
{
    public interface ICompactionStrategy
    {
        bool TryGetRangeToCompact(IReadOnlyList<KeyValuePair<Segment, SegmentStats>> compactableSegments, out (int offset, int count) range);
    }
}
