using Fugu.Actors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Compaction
{
    public class AlwaysCompactCompactionStrategy : ICompactionStrategy
    {
        #region ICompactionStrategy

        public bool TryGetRangeToCompact(IReadOnlyList<KeyValuePair<Segment, SegmentStats>> compactableSegments, out (int offset, int count) range)
        {
            if (compactableSegments.Count >= 2)
            {
                range = (0, compactableSegments.Count);
                return true;
            }
            else
            {
                range = default((int, int));
                return false;
            }
        }

        #endregion
    }
}
