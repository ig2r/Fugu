using System.Threading.Tasks;

namespace Fugu
{
    public class KeyValueStore
    {
        public ValueTask WriteAsync(WriteBatch batch)
        {
            return ValueTask.CompletedTask;
        }
    }
}
