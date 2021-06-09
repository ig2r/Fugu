using Fugu;
using System.Text;
using System.Threading.Tasks;

namespace BasicSample
{
    class Program
    {
        static async Task Main()
        {
            await using var store = new KeyValueStore();

            var batch = new WriteBatch
            {
                ["Hello"] = Encoding.UTF8.GetBytes("World")
            };

            await store.WriteAsync(batch);

            using (var snapshot = await store.GetSnapshotAsync())
            {

            }
        }
    }
}
