using Fugu.Actors;
using Fugu.Common;

namespace Fugu.Writer
{
    public class WriterActorShell : IWriterActor
    {
        private readonly WriterActorCore _core;
        private readonly Channel<CommitWriteBatchToSegmentMessage> _commitWriteBatchChannel;

        public WriterActorShell(WriterActorCore core, Channel<CommitWriteBatchToSegmentMessage> commitWriteBatchChannel)
        {
            Guard.NotNull(core, nameof(core));
            Guard.NotNull(commitWriteBatchChannel, nameof(commitWriteBatchChannel));

            _core = core;
            _commitWriteBatchChannel = commitWriteBatchChannel;
        }

        public async void Run()
        {
            await new SelectBuilder()
                .Case(_commitWriteBatchChannel, msg => _core.CommitAsync(msg.Clock, msg.WriteBatch, msg.OutputTable, msg.ReplyChannel))
                .SelectAsync(_ => true);
        }
    }
}
