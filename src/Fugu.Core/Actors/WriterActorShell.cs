using Fugu.Common;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Actors
{
    public class WriterActorShell : IWriterActor
    {
        private readonly WriterActorCore _core;
        private readonly ActionBlock<Message> _handlerBlock;

        public WriterActorShell(WriterActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
            _handlerBlock = new ActionBlock<Message>(HandleMessageAsync, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        }

        #region IWriterActor

        public void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _handlerBlock.Post(new Message.Commit(writeBatch, replyChannel));
        }

        public void StartNewSegment(IOutputTable outputTable)
        {
            _handlerBlock.Post(new Message.StartNewSegment(outputTable));
        }

        #endregion

        private Task HandleMessageAsync(Message message)
        {
            switch (message)
            {
                case Message.Commit commit:
                    _core.Commit(commit.WriteBatch, commit.ReplyChannel);
                    return Task.CompletedTask;
                case Message.StartNewSegment startNewSegment:
                    return _core.StartNewSegmentAsync(startNewSegment.OutputTable);
                default:
                    throw new NotSupportedException();
            }
        }

        #region Nested types

        private abstract class Message
        {
            private Message()
            {
            }

            public sealed class Commit : Message
            {
                public Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
                {
                    Guard.NotNull(writeBatch, nameof(writeBatch));
                    Guard.NotNull(replyChannel, nameof(replyChannel));

                    WriteBatch = writeBatch;
                    ReplyChannel = replyChannel;
                }

                public WriteBatch WriteBatch { get; }
                public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
            }

            public sealed class StartNewSegment : Message
            {
                public StartNewSegment(IOutputTable outputTable)
                {
                    OutputTable = outputTable;
                }

                public IOutputTable OutputTable { get; }
            }
        }

        #endregion
    }
}
