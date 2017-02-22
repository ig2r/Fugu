using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class IndexActorShell : IIndexActor
    {
        private readonly IndexActorCore _core;
        private readonly MessageLoop _loop = new MessageLoop();

        public IndexActorShell(IndexActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
        }

        #region IIndexActor

        public async void UpdateIndex(
            StateVector clock,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            using (await _loop.WaitAsync())
            {
                _core.UpdateIndex(clock, indexUpdates, replyChannel);
            }
        }

        #endregion
    }
}
