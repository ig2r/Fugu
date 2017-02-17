using Fugu.Common;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Actors
{
    public class PartitioningActorShell : IPartitioningActor
    {
        private readonly PartitioningActorCore _core;
        private readonly ActionBlock<Message> _handlerBlock;

        public PartitioningActorShell(PartitioningActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
            _handlerBlock = new ActionBlock<Message>(HandleMessageAsync, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        }

        #region IPartitioningActor

        public void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _handlerBlock.Post(new Message.Commit(writeBatch, replyChannel));
        }

        #endregion

        private Task HandleMessageAsync(Message message)
        {
            switch (message)
            {
                case Message.Commit commit:
                    return _core.CommitAsync(commit.WriteBatch, commit.ReplyChannel);
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
        }

        #endregion
    }
}
