using Fugu.TableSets;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.IntegrationTests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                // Default configuration
                .AddInMemoryCollection(new Dictionary<string, string>
                {
                    ["mode"] = "bulkwrite",
                })
                // Configuration from command line
                .AddCommandLine(args)
                .Build();

            switch (configuration["mode"])
            {
                case "bulkwrite":
                    {
                        var items = configuration.GetValue("items", 100000);
                        var writers = configuration.GetValue("writers", 8);
                        RunBulkwriteAsync(items, writers).Wait();
                        break;
                    }
                default:
                    Console.WriteLine("Unrecognized value for parameter 'mode'.");
                    break;
            }
        }

        private static async Task RunBulkwriteAsync(int items, int writers)
        {
            Console.WriteLine($"{nameof(RunBulkwriteAsync)}:");
            Console.WriteLine($"  items   = {items}");
            Console.WriteLine($"  writers = {writers}");
            Console.WriteLine();

            var data = new byte[256];
            new Random().NextBytes(data);

            Console.WriteLine("Preparing...");
            var stopwatch = Stopwatch.StartNew();

            var assemblyPath = Assembly.GetEntryAssembly().Location;
            var basePath = Path.Combine(Path.GetDirectoryName(assemblyPath), "Data");
            Directory.CreateDirectory(basePath);

            using (var tableSet = new MemoryMappedTableSet(basePath))
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                stopwatch.Stop();
                Console.WriteLine($"Took {stopwatch.ElapsedMilliseconds} ms");
                Console.WriteLine("Writing...");
                stopwatch.Restart();

                await Task.WhenAll(from bucket in Enumerable.Range(0, writers)
                                   select Task.Run(() => DoWriteAsync(store, writers, bucket, items / writers, data)));

                // Stop timer and report results
                stopwatch.Stop();
                double opsPerSecond = 1000 * items / stopwatch.ElapsedMilliseconds;
                Console.WriteLine($"Took {stopwatch.ElapsedMilliseconds} ms, that's {opsPerSecond} op/s");
            }
        }

        private static async Task DoWriteAsync(KeyValueStore store, int writers, int bucket, int count, byte[] data)
        {
            const string keyFormat = "bucket:{0}/key:{1}";

            for (int i = 0; i < count; i++)
            {
                var batch = new WriteBatch();
                var key = Encoding.UTF8.GetBytes(string.Format(keyFormat, bucket, i));
                batch.Put(key, data);
                await store.CommitAsync(batch);
            }

            if (writers <= 16)
            {
                Console.WriteLine("Finished bucket {0}", bucket);
            }
        }
    }
}
