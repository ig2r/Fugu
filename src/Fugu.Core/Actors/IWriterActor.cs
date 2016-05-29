using System.Collections.Generic;
using System.Threading.Tasks;
using Fugu.Common;

namespace Fugu.Actors
{
    public interface IWriterActor
    {
        Task WriteAsync(VectorClock clock, WriteBatch batch);
    }
}