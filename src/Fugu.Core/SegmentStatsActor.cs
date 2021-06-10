using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Consumes index updates from IndexActor and tracks per-segment usage stats (total/live/dead bytes).
    /// Emits current stats to CompactionActor.
    /// When a segment's live count drops to zero AND it's not the current output segment, remove from stats
    /// and tell AllocationActor to reduce its live segment count by one.
    /// </summary>
    public class SegmentStatsActor
    {
    }
}
