using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct CommitWriteBatchMessage
    {
        public CommitWriteBatchMessage(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
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
