using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public struct SnapshotsUpdateMessage
    {
        public SnapshotsUpdateMessage(
            StateVector clock,
            CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> index,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(index, nameof(index));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            Clock = clock;
            Index = index;
            ReplyChannel = replyChannel;
        }

        public StateVector Clock { get; }
        public CritBitTree<ByteArrayKeyTraits, byte[], IndexEntry> Index { get; }
        public TaskCompletionSource<VoidTaskResult> ReplyChannel { get; }
    }
}
