using Fugu.TableSets;
using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Fugu.Core.Tests
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
        public async Task CommitAsync_PutSingleKey_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:1"));
                await store.CommitAsync(batch);
            }
        }

        [Fact]
        public async Task CommitAsync_PutTwoKeysInTwoSequentialTransactions_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch1 = new WriteBatch();
                batch1.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:1"));
                await store.CommitAsync(batch1);

                var batch2 = new WriteBatch();
                batch2.Put(Encoding.UTF8.GetBytes("key:2"), Encoding.UTF8.GetBytes("value:2"));
                await store.CommitAsync(batch2);
            }
        }

        [Fact]
        public async Task CommitAsync_PutSameKeyTwiceInTwoSequentialTransactions_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch1 = new WriteBatch();
                batch1.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:1"));
                await store.CommitAsync(batch1);

                var batch2 = new WriteBatch();
                batch2.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:2"));
                await store.CommitAsync(batch2);
            }
        }

        [Fact]
        public async Task CommitAsync_PutSameKey1000Times_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                for (int i = 0; i < 1000; i++)
                {
                    var batch = new WriteBatch();
                    batch.Put(Encoding.UTF8.GetBytes("key:1"), Encoding.UTF8.GetBytes("value:1"));
                    await store.CommitAsync(batch);
                }
            }
        }

        [Fact]
        public async Task CommitAsync_Put1000RandomKeys_Succeeds()
        {
            var random = new Random();

            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                for (int i = 0; i < 1000; i++)
                {
                    var batch = new WriteBatch();
                    batch.Put(Encoding.UTF8.GetBytes("key:" + random.Next(100)), Encoding.UTF8.GetBytes("value:1"));
                    await store.CommitAsync(batch);
                }
            }
        }

        [Fact]
        public async Task CommitAsync_Put5000UniqueKeys_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                for (int i = 0; i < 5000; i++)
                {
                    var batch = new WriteBatch();
                    batch.Put(Encoding.UTF8.GetBytes("key:" + i), Encoding.UTF8.GetBytes("value:1"));
                    await store.CommitAsync(batch);
                }
            }
        }

        [Fact]
        public async Task GetSnapshotAsync_EmptyStore_Succeeds()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            using (var snapshot = await store.GetSnapshotAsync())
            {
                Assert.NotNull(snapshot);
            }
        }

        [Fact]
        public async Task CommitAsync_PutSingleKey_KeyIsVisibleInSnapshot()
        {
            // Arrange
            var key1 = Encoding.UTF8.GetBytes("key:1");
            var value1 = Encoding.UTF8.GetBytes("value:1");

            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var batch = new WriteBatch();
                batch.Put(key1, value1);

                // Act
                await store.CommitAsync(batch);

                // Assert
                using (var snapshot = await store.GetSnapshotAsync())
                {
                    var retrieved = snapshot[key1];
                    Assert.Equal<byte>(value1, retrieved.ToArray());
                }
            }
        }

        [Fact]
        public async Task GetSnapshotAsync_ReleasingOlderOfTwoSnapshots_UpdatesOldestVisibleState()
        {
            // Arrange
            var key1 = Encoding.UTF8.GetBytes("key:1");
            var value1 = Encoding.UTF8.GetBytes("value:1");

            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var snapshot1 = await store.GetSnapshotAsync();

                var batch = new WriteBatch();
                batch.Put(key1, value1);
                await store.CommitAsync(batch);

                var snapshot2 = await store.GetSnapshotAsync();

                batch = new WriteBatch();
                batch.Put(key1, value1);
                await store.CommitAsync(batch);

                // Act
                snapshot1.Dispose();
                snapshot2.Dispose();

                // Assert
            }
        }

        [Fact]
        public async Task CloseAsync_Succeeds()
        {
            // Arrange
            var tableSet = new InMemoryTableSet();
            var store = await KeyValueStore.CreateAsync(tableSet);

            var key1 = Encoding.UTF8.GetBytes("key:1");
            var value1 = Encoding.UTF8.GetBytes("value:1");
            var batch1 = new WriteBatch();
            batch1.Put(key1, value1);

            var key2 = Encoding.UTF8.GetBytes("key:2");
            var value2 = Encoding.UTF8.GetBytes("value:2");
            var batch2 = new WriteBatch();
            batch2.Put(key2, value2);

            await store.CommitAsync(batch1);

            // Act
            await store.CloseAsync();
        }
    }
}
