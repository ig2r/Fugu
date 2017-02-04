using Fugu.Common;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public interface ISnapshotsActor
    {
        void UpdateIndex(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel);
        void GetSnapshot(TaskCompletionSource<Snapshot> replyChannel);
    }
}