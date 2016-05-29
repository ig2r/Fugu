using Fugu.TableSets;
using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Tests
{
    public class KeyValueStoreTests
    {
        [Fact]
        public async Task KeyValueStore_Ctor_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                Assert.NotNull(store);
            }
        }

        [Fact]
        public async Task GetSnapshotAsync_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            using (var snapshot = await store.GetSnapshotAsync())
            {
                Assert.NotNull(snapshot);
            }
        }

        [Fact]
        public async Task CommitAsync_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:1"));
                await store.CommitAsync(batch);
            }
        }
    }
}
