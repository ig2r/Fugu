using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Index
{
    public class IndexActorShell : IIndexActor
    {
        private readonly IndexActorCore _core;
        private readonly Channel<UpdateIndexMessage> _updateIndexChannel;

        public IndexActorShell(IndexActorCore core, Channel<UpdateIndexMessage> updateIndexChannel)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
            _updateIndexChannel = updateIndexChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_updateIndexChannel, msg =>
                {
                    _core.UpdateIndex(msg.Clock, msg.IndexUpdates, msg.ReplyChannel);
                    return Task.CompletedTask;
                })
                .SelectAsync(_ => true);
        }
    }
}
