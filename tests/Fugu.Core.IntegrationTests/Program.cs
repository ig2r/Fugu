using Fugu.TableSets;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.IntegrationTests
{
    public class Program
    {
        const int N = 100000;
        const int CONCURRENT_WRITERS = 8;

        public static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Build();

            switch (configuration["mode"] ?? "bulkwrite")
            {
                case "bulkwrite":
                    RunBulkwriteAsync().Wait();
                    break;
                default:
                    Console.WriteLine("Unrecognized value for parameter mode.");
                    break;
            }
        }

        private static async Task RunBulkwriteAsync()
        {
            var data = new byte[256];
            new Random().NextBytes(data);

            var assemblyPath = Assembly.GetEntryAssembly().Location;
            var basePath = Path.Combine(Path.GetDirectoryName(assemblyPath), "Data");
            Directory.CreateDirectory(basePath);

            //var tableSet = new InMemoryTableSet();
            using (var tableSet = new MemoryMappedTableSet(basePath))
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var stopwatch = Stopwatch.StartNew();

                var tasks = new Task[CONCURRENT_WRITERS];
                for (int i = 0; i < CONCURRENT_WRITERS; i++)
                {
                    int bucket = i;
                    tasks[bucket] = Task.Run(() => DoWriteAsync(store, bucket, N / CONCURRENT_WRITERS, data));
                }

                await Task.WhenAll(tasks);

                // Stop timer and report results
                stopwatch.Stop();
                double opsPerSecond = 1000 * N / stopwatch.ElapsedMilliseconds;
                Console.WriteLine("Took {0} ms, that's {1} op/s", stopwatch.ElapsedMilliseconds, opsPerSecond);
            }
        }

        private static async Task DoWriteAsync(KeyValueStore store, int bucket, int count, byte[] data)
        {
            const string keyFormat = "bucket:{0}/key:{1}";

            for (int i = 0; i < count; i++)
            {
                var batch = new WriteBatch();
                var key = Encoding.UTF8.GetBytes(string.Format(keyFormat, bucket, i));
                batch.Put(key, data);
                await store.CommitAsync(batch);
            }

            #pragma warning disable CS0162 // Unreachable code detected

            if (CONCURRENT_WRITERS <= 16)
            {
                Console.WriteLine("Finished bucket {0}", bucket);
            }

            #pragma warning restore CS0162 // Unreachable code detected
        }
    }
}
