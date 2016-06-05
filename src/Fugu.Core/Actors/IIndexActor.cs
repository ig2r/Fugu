using System.Collections.Generic;
using System.Threading.Tasks;
using Fugu.Common;
using Fugu.Compaction;

namespace Fugu.Actors
{
    public interface IIndexActor
    {
        Task MergeIndexUpdatesAsync(VectorClock clock, IEnumerable<KeyValuePair<byte[], IndexEntry>> updates);
        void RemoveEntries(IEnumerable<byte[]> keys, long maxGeneration);
    }
}