using Fugu.Channels;
using Fugu.Common;

namespace Fugu.Actors
{
    public class PartitioningActorShell : IPartitioningActor
    {
        private readonly PartitioningActorCore _core;
        private readonly Channel<CommitWriteBatchMessage> _commitWriteBatchChannel;

        public PartitioningActorShell(PartitioningActorCore core, Channel<CommitWriteBatchMessage> commitWriteBatchChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(commitWriteBatchChannel, nameof(commitWriteBatchChannel));

            _core = core;
            _commitWriteBatchChannel = commitWriteBatchChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_commitWriteBatchChannel, msg => _core.CommitAsync(msg.WriteBatch, msg.ReplyChannel))
                .SelectAsync(_ => true);
        }
    }
}
