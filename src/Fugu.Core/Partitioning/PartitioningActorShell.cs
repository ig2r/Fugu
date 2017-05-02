using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Partitioning
{
    public class PartitioningActorShell : IPartitioningActor
    {
        public PartitioningActorShell(PartitioningActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            var scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;

            // Unconstrained because KeyValueStore will post to it
            CommitWriteBatchBlock = new ActionBlock<CommitWriteBatchMessage>(
                msg => core.CommitAsync(msg.WriteBatch, msg.ReplyChannel),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler });

            TotalCapacityChangedBlock = new ActionBlock<TotalCapacityChangedMessage>(
                msg => core.OnTotalCapacityChanged(msg.DeltaCapacity),
                new ExecutionDataflowBlockOptions { TaskScheduler = scheduler, BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });
        }

        public ITargetBlock<CommitWriteBatchMessage> CommitWriteBatchBlock { get; }
        public ITargetBlock<TotalCapacityChangedMessage> TotalCapacityChangedBlock { get; }
    }
}
