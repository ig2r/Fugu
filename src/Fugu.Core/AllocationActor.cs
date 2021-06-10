using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Assigns a WriteBatch to a sufficiently-sized segment. Keeps track of:
    /// - the number of current segments (OR: total size of all segments?), to determine min size for the next;
    /// - remaining space within the current segment
    /// </summary>
    public class AllocationActor
    {
    }
}
