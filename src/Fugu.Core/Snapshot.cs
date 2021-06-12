using System;

namespace Fugu
{
    public sealed class Snapshot : IDisposable
    {
        public Snapshot(VectorClock clock)
        {
            Clock = clock;
        }

        public VectorClock Clock { get; }

        public void Dispose()
        {
        }
    }
}
