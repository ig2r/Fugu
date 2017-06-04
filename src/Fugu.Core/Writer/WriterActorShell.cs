using Fugu.Actors;
using Fugu.Common;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Writer
{
    public class WriterActorShell
    {
        public WriterActorShell(WriterActorCore core)
        {
            Guard.NotNull(core, nameof(core));

            WriteBlock = new ActionBlock<WriteToSegmentMessage>(
                msg => core.WriteAsync(msg.Clock, msg.WriteBatch, msg.OutputTable, msg.ReplyChannel),
                new ExecutionDataflowBlockOptions { BoundedCapacity = KeyValueStore.DEFAULT_BOUNDED_CAPACITY });
        }

        public ITargetBlock<WriteToSegmentMessage> WriteBlock { get; }

        public Task Completion => WriteBlock.Completion;

        public void Complete()
        {
            WriteBlock.Complete();
        }
    }
}
