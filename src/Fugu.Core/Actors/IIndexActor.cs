using System.Collections.Generic;
using System.Threading.Tasks;
using Fugu.Common;

namespace Fugu.Actors
{
    public interface IIndexActor
    {
        void RegisterCompactableSegment(Segment segment);
        Task UpdateAsync(VectorClock clock, IEnumerable<KeyValuePair<byte[], IndexEntry>> updates);
    }
}