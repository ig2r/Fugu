using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class PartitioningActorShell : IPartitioningActor
    {
        private readonly PartitioningActorCore _core;
        private readonly MessageLoop _loop = new MessageLoop();

        public PartitioningActorShell(PartitioningActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
        }

        #region IPartitioningActor

        public async void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            using (await _loop.WaitAsync())
            {
                await _core.CommitAsync(writeBatch, replyChannel);
            }
        }

        #endregion
    }
}
