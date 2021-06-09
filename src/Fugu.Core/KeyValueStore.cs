using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore
    {
        public ValueTask<Snapshot> GetSnapshotAsync()
        {
            return ValueTask.FromResult(new Snapshot());
        }

        public ValueTask WriteAsync(WriteBatch batch)
        {
            return ValueTask.CompletedTask;
        }
    }
}
