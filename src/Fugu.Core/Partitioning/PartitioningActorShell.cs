using Fugu.Actors;
using Fugu.Common;

namespace Fugu.Partitioning
{
    public class PartitioningActorShell : IPartitioningActor
    {
        private readonly PartitioningActorCore _core;
        private readonly Channel<CommitWriteBatchMessage> _commitWriteBatchChannel;
        private readonly Channel<TotalCapacityChangedMessage> _totalCapacityChangedChannel;

        public PartitioningActorShell(
            PartitioningActorCore core,
            Channel<CommitWriteBatchMessage> commitWriteBatchChannel,
            Channel<TotalCapacityChangedMessage> totalCapacityChangedChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(commitWriteBatchChannel, nameof(commitWriteBatchChannel));
            Guard.NotNull(totalCapacityChangedChannel, nameof(totalCapacityChangedChannel));

            _core = core;
            _commitWriteBatchChannel = commitWriteBatchChannel;
            _totalCapacityChangedChannel = totalCapacityChangedChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_commitWriteBatchChannel, msg => _core.CommitAsync(msg.WriteBatch, msg.ReplyChannel))
                .Case(_totalCapacityChangedChannel, msg => _core.OnTotalCapacityChangedAsync(msg.DeltaCapacity))
                .SelectAsync(_ => true);
        }
    }
}
