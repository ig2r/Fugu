using Fugu.Common;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    /// <summary>
    /// Instances of this actor build segments from incoming write batches by writing their data to the current
    /// output table. After each write, implementing actors are expected to pass metadata on the written data
    /// to downstream components that update the store's master index accordingly.
    /// </summary>
    public interface IWriterActor
    {
    }
}
