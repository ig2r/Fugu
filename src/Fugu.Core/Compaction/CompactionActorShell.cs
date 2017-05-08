using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Compaction
{
    public class CompactionActorShell
    {
        public CompactionActorShell(CompactionActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            SegmentSizesChangedBlock = new ActionBlock<SegmentSizesChangedMessage>(msg =>
                core.OnSegmentSizesChangedAsync(msg.Clock, msg.SizeChanges, msg.Index),
                new ExecutionDataflowBlockOptions { BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });
        }

        public ITargetBlock<SegmentSizesChangedMessage> SegmentSizesChangedBlock { get; }

        public Task Completion => SegmentSizesChangedBlock.Completion;

        public void Complete()
        {
            SegmentSizesChangedBlock.Complete();
        }
    }
}
