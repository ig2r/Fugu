using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct SnapshotsUpdateMessage
    {
        public SnapshotsUpdateMessage(
            StateVector clock,
            AaTree<IndexEntry> index,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            // Note that replyChannel is an optional parameter and may be null
            Guard.NotNull(index, nameof(index));

            Clock = clock;
            Index = index;
            ReplyChannel = replyChannel;
        }

        public StateVector Clock { get; }
        public AaTree<IndexEntry> Index { get; }
        public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
    }
}
