using Fugu.TableSets;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Core.Tests
{
    public class DurabilityTests
    {
        [Fact]
        public async Task Durability_OpeningEmptyStoreTwice_SucceedsWithoutCreatingSegments()
        {
            // Arrange
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
            }

            // Act
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
            }

            // Assert
            int tableCount = (await tableSet.GetTablesAsync()).Count();
            Assert.Equal(0, tableCount);
        }

        [Fact]
        public async Task Durability_PutSingleKey_KeyIsAvailableAfterRecreatingStore()
        {
            // Arrange
            var kvp1 = MakeKey("key:1", "value:1");
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp1.Key, kvp1.Value);
                await store.CommitAsync(batch);
            }

            // Act
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                // Assert
                using (var snapshot = await store.GetSnapshotAsync())
                {
                    var retrieved = snapshot[kvp1.Key].ToArray();
                    Assert.Equal<byte>(kvp1.Value, retrieved);
                }
            }
        }

        [Fact]
        public async Task Durability_WriteToStoreAfterRecreate_RetainsBothWrites()
        {
            // Arrange
            var kvp1 = MakeKey("key:1", "value:1");
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp1.Key, kvp1.Value);
                await store.CommitAsync(batch);
            }

            // Act
            var kvp2 = MakeKey("key:2", "value:2");
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp2.Key, kvp2.Value);
                await store.CommitAsync(batch);
            }

            // Assert
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            using (var snapshot = await store.GetSnapshotAsync())
            {
                Assert.Equal(kvp1.Value, snapshot[kvp1.Key].ToArray());
                Assert.Equal(kvp2.Value, snapshot[kvp2.Key].ToArray());
            }
        }

        [Fact]
        public async Task Durability_FirstWriteAfterRecreate_StartsNewSegment()
        {
            // Arrange
            var kvp1 = MakeKey("key:1", "value:1");
            var tableSet = new InMemoryTableSet();

            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp1.Key, kvp1.Value);
                await store.CommitAsync(batch);
            }

            var tableCountBeforeWrite = (await tableSet.GetTablesAsync()).Count();

            // Act
            var kvp2 = MakeKey("key:2", "value:2");
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(kvp2.Key, kvp2.Value);
                await store.CommitAsync(batch);

                // Assert
                var tableCountAfterWrite = (await tableSet.GetTablesAsync()).Count();
                Assert.Equal(1, tableCountAfterWrite - tableCountBeforeWrite);
            }
        }

        private static KeyValuePair<byte[], byte[]> MakeKey(string key, string value)
        {
            return new KeyValuePair<byte[], byte[]>(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
        }
    }
}
