using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Compaction
{
    public class CompactionResult
    {
        public CompactionResult(
            Segment compactedSegment,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            IReadOnlyList<byte[]> droppedTombstones)
        {
            Guard.NotNull(compactedSegment, nameof(compactedSegment));
            Guard.NotNull(indexUpdates, nameof(indexUpdates));
            Guard.NotNull(droppedTombstones, nameof(droppedTombstones));

            CompactedSegment = compactedSegment;
            IndexUpdates = indexUpdates;
            DroppedTombstones = droppedTombstones;
        }

        public Segment CompactedSegment { get; }
        public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> IndexUpdates { get; }
        public IReadOnlyList<byte[]> DroppedTombstones { get; }
    }
}
