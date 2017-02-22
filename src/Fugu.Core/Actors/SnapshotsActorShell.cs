using Fugu.Common;
using System.Threading.Tasks;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class SnapshotsActorShell : ISnapshotsActor
    {
        private readonly SnapshotsActorCore _core;
        private readonly MessageLoop _loop = new MessageLoop();

        public SnapshotsActorShell(SnapshotsActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
        }

        #region ISnapshotsActor

        public async void UpdateIndex(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            using (await _loop.WaitAsync())
            {
                _core.UpdateIndex(clock, index, replyChannel);
            }
        }

        public async void GetSnapshot(TaskCompletionSource<Snapshot> replyChannel)
        {
            using (await _loop.WaitAsync())
            {
                var snapshot = _core.GetSnapshot();
                replyChannel.SetResult(snapshot);
            }
        }

        #endregion
    }
}
