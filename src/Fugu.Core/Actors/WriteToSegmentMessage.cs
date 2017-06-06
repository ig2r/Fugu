using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct WriteToSegmentMessage
    {
        public WriteToSegmentMessage(
            StateVector clock,
            WriteBatch writeBatch,
            IWritableTable outputTable,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            Guard.NotNull(outputTable, nameof(outputTable));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            Clock = clock;
            WriteBatch = writeBatch;
            OutputTable = outputTable;
            ReplyChannel = replyChannel;
        }

        public StateVector Clock { get; }
        public WriteBatch WriteBatch { get; }
        public IWritableTable OutputTable { get; }
        public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
    }
}
