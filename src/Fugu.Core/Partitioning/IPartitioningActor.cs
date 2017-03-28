using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Partitioning
{
    /// <summary>
    /// Instances of this actor distribute incoming writes across segments. Each new segment is sized in proportion
    /// to the total amount of data already in the store, so that the number of segments is logarithmic in the amount
    /// of stored data.
    /// </summary>
    public interface IPartitioningActor
    {
        //void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel);
    }
}