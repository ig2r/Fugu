using Fugu.TableSets;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Tests
{
    public class DurabilityTests
    {
        [Fact]
        public async Task Durability_PutSingleKey_KeyIsAvailableAfterRecreatingStore()
        {
            // Arrange:
            var kvp1 = MakeKey("key:1", "value:1");
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp1.Key, kvp1.Value);
                await store.CommitAsync(batch);
            }

            // Act:
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                // Assert:
                using (var snapshot = await store.GetSnapshotAsync())
                {
                    var retrieved = await snapshot.TryGetValueAsync(kvp1.Key);
                    Assert.Equal<byte>(kvp1.Value, retrieved);
                }
            }
        }

        [Fact]
        public async Task Durability_WriteToStoreAfterRecreate_RetainsBothWrites()
        {
            // Arrange:
            var kvp1 = MakeKey("key:1", "value:1");
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp1.Key, kvp1.Value);
                await store.CommitAsync(batch);
            }

            // Act:
            var kvp2 = MakeKey("key:2", "value:2");
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp2.Key, kvp2.Value);
                await store.CommitAsync(batch);
            }

            // Assert:
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            using (var snapshot = await store.GetSnapshotAsync())
            {
                Assert.Equal(kvp1.Value, await snapshot.TryGetValueAsync(kvp1.Key));
                Assert.Equal(kvp2.Value, await snapshot.TryGetValueAsync(kvp2.Key));
            }
        }

        private static KeyValuePair<byte[], byte[]> MakeKey(string key, string value)
        {
            return new KeyValuePair<byte[], byte[]>(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
        }
    }
}
