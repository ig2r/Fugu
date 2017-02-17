using Fugu.Common;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Actors
{
    public class SnapshotsActorShell : ISnapshotsActor
    {
        private readonly SnapshotsActorCore _core;
        private readonly ActionBlock<Message> _handlerBlock;

        public SnapshotsActorShell(SnapshotsActorCore core)
        {
            Guard.NotNull(core, nameof(core));
            _core = core;
            _handlerBlock = new ActionBlock<Message>(HandleMessageAsync, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        }

        #region ISnapshotsActor

        public void UpdateIndex(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _handlerBlock.Post(new Message.UpdateIndex(clock, index, replyChannel));
        }

        public void GetSnapshot(TaskCompletionSource<Snapshot> replyChannel)
        {
            _handlerBlock.Post(new Message.GetSnapshot(replyChannel));
        }

        #endregion

        private Task HandleMessageAsync(Message message)
        {
            switch (message)
            {
                case Message.UpdateIndex updateIndex:
                    _core.UpdateIndex(updateIndex.Clock, updateIndex.Index, updateIndex.ReplyChannel);
                    return Task.CompletedTask;
                case Message.GetSnapshot getSnapshot:
                    var snapshot = _core.GetSnapshot();
                    getSnapshot.ReplyChannel.SetResult(snapshot);
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
                public UpdateIndex(StateVector clock, Index index, TaskCompletionSource<VoidTaskResult> replyChannel)

                {
                    Guard.NotNull(index, nameof(index));
                    Guard.NotNull(replyChannel, nameof(replyChannel));

                    Clock = clock;
                    Index = index;
                    ReplyChannel = replyChannel;
                }

                public StateVector Clock { get; }
                public Index Index { get; }
                public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
            }

            public sealed class GetSnapshot : Message
            {
                public GetSnapshot(TaskCompletionSource<Snapshot> replyChannel)
                {
                    Guard.NotNull(replyChannel, nameof(replyChannel));
                    ReplyChannel = replyChannel;
                }

                public TaskCompletionSource<Snapshot> ReplyChannel { get; }
            }
        }

        #endregion
    }
}
