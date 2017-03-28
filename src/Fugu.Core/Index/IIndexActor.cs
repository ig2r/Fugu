using Fugu.Common;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Fugu.Index
{
    /// <summary>
    /// Instances of this actor maintain the store's master index.
    /// </summary>
    /// <remarks>
    /// Implementing actors are expected to make the updated index available to downstream components, and may feed
    /// into balancing strategies to ensure that the store does not grow beyond assigned limits.
    /// </remarks>
    public interface IIndexActor
    {
        //void UpdateIndex(
        //    StateVector clock,
        //    IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates,
        //    TaskCompletionSource<VoidTaskResult> replyChannel);
    }
}
