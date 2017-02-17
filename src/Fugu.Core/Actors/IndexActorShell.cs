using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Actors
{
    public class IndexActorShell : IIndexActor
    {
        private readonly IndexActorCore _core;
        private readonly ActionBlock<Message> _handlerBlock;

        public IndexActorShell(IndexActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
            _handlerBlock = new ActionBlock<Message>(HandleMessageAsync, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        }

        #region IIndexActor

        public void UpdateIndex(
            StateVector clock,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _handlerBlock.Post(new Message.UpdateIndex(clock, indexUpdates, replyChannel));
        }

        #endregion

        private Task HandleMessageAsync(Message message)
        {
            switch (message)
            {
                case Message.UpdateIndex updateIndex:
                    _core.UpdateIndex(updateIndex.Clock, updateIndex.IndexUpdates, updateIndex.ReplyChannel);
                    return Task.CompletedTask;
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

            public sealed class UpdateIndex : Message
            {
                public UpdateIndex(
                    StateVector clock,
                    IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
                    TaskCompletionSource<VoidTaskResult> replyChannel)
                {
                    Guard.NotNull(indexUpdates, nameof(indexUpdates));
                    Guard.NotNull(replyChannel, nameof(replyChannel));

                    Clock = clock;
                    IndexUpdates = indexUpdates;
                    ReplyChannel = replyChannel;
                }

                public StateVector Clock { get; }
                public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> IndexUpdates { get; }
                public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
            }
        }

        #endregion
    }
}
