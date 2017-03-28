using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct UpdateIndexMessage
    {
        public UpdateIndexMessage(
            StateVector clock,
            IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            // Note that replyChannel is an optional parameter and may be null
            Guard.NotNull(indexUpdates, nameof(indexUpdates));

            Clock = clock;
            IndexUpdates = indexUpdates;
            ReplyChannel = replyChannel;
        }

        public StateVector Clock { get; }
        public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> IndexUpdates { get; }
        public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
    }
}
