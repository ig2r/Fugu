using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Index
{
    public class IndexActorShell
    {
        public IndexActorShell(IndexActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            UpdateIndexBlock = new ActionBlock<UpdateIndexMessage>(
                msg => core.UpdateIndexAsync(msg.Clock, msg.IndexUpdates, msg.ReplyChannel),
                new ExecutionDataflowBlockOptions { BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });
        }

        public ITargetBlock<UpdateIndexMessage> UpdateIndexBlock { get; }

        public Task Completion => UpdateIndexBlock.Completion;

        public void Complete()
        {
            UpdateIndexBlock.Complete();
        }
    }
}
