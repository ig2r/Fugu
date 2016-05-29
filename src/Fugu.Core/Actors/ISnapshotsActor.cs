using System;
using System.Threading.Tasks;
using Fugu.Common;

namespace Fugu.Actors
{
    public interface ISnapshotsActor
    {
        event Action<VectorClock> OldestLiveSnapshotChanged;

        Task<Snapshot> GetSnapshotAsync();
        Task UpdateIndexAsync(VectorClock clock, CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index);
    }
}