using Fugu.Common;
using System.IO;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct WriteToSegmentMessage
    {
        public WriteToSegmentMessage(
            StateVector clock,
            WriteBatch writeBatch,
            Segment outputSegment,
            Stream outputStream,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            Guard.NotNull(outputSegment, nameof(outputSegment));
            Guard.NotNull(outputStream, nameof(outputStream));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            Clock = clock;
            WriteBatch = writeBatch;
            OutputSegment = outputSegment;
            OutputStream = outputStream;
            ReplyChannel = replyChannel;
        }

        public StateVector Clock { get; }
        public WriteBatch WriteBatch { get; }
        public Segment OutputSegment { get; }
        public Stream OutputStream { get; }
        public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
    }
}
