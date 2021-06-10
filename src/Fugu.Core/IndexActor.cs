using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Maintains an immutable index of key-value pairs across all segments. Emits:
    /// - Updated index to SnaphotsActor
    /// - Updated index + changes to SegmentStatsActor
    /// </summary>
    public class IndexActor
    {
    }
}
