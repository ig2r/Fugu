using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Actors
{
    public class VoidCompactionStrategy : ICompactionStrategy
    {
        #region ICompactionStrategy

        public bool TryGetRangeToCompact(IReadOnlyList<KeyValuePair<Segment, SegmentStats>> compactableSegments, out (int offset, int count) range)
        {
            range = default((int, int));
            return false;
        }

        #endregion
    }
}
