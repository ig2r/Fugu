using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu
{
    /// <summary>
    /// Receives the latest index from IndexActor and serves snapshot acquire/release requests.
    /// Through this, it knows the "horizon" vector clock value, i.e., the oldest clock value that
    /// anyone can still reference to. This value controls when a stale (dead) segment can be safely
    /// evicted.
    /// </summary>
    public class SnapshotsActor
    {
    }
}
