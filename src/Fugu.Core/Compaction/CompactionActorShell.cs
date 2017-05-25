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

            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
            SegmentCreatedBlock = new ActionBlock<Segment>(
                s => core.OnSegmentCreated(s),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = 1 });
            SegmentStatsChangedBlock = new ActionBlock<SegmentStatsChangedMessage>(
                msg => core.OnSegmentStatsChangedAsync(msg.Clock, msg.Stats, msg.Index),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = 1 });
        }

        public ITargetBlock<Segment> SegmentCreatedBlock { get; }
        public ITargetBlock<SegmentStatsChangedMessage> SegmentStatsChangedBlock { get; }

        public Task Completion => Task.WhenAll(
            SegmentCreatedBlock.Completion,
            SegmentStatsChangedBlock.Completion);

        public void Complete()
        {
            SegmentCreatedBlock.Complete();
            SegmentStatsChangedBlock.Complete();
        }
    }
}
