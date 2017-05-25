using Fugu.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Eviction
{
    public class EvictionActorShell
    {
        public EvictionActorShell(EvictionActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
            EvictSegmentBlock = new ActionBlock<EvictSegmentMessage>(
                msg => core.EvictSegmentAsync(msg.EvictAt, msg.Segment),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });
            OldestVisibleStateChangedBlock = new ActionBlock<StateVector>(
                clock => core.OnOldestVisibleStateChangedAsync(clock),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = 1 });
        }

        public ITargetBlock<EvictSegmentMessage> EvictSegmentBlock { get; }
        public ITargetBlock<StateVector> OldestVisibleStateChangedBlock { get; }

        public Task Completion => Task.WhenAll(EvictSegmentBlock.Completion, OldestVisibleStateChangedBlock.Completion);

        public void Complete()
        {
            EvictSegmentBlock.Complete();
            OldestVisibleStateChangedBlock.Complete();
        }
    }
}
