using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fugu.TableSets;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.Benchmarks
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Build();

            //BenchmarkRunner.Run<BenchmarkSample>();
            new BenchmarkSample().Commit1000WriteBatches().Wait();
        }
    }

    public class BenchmarkSample
    {
        [Benchmark]
        public async Task Commit1000WriteBatches()
        {
            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var tasks = new List<Task>();
                for (int i = 0; i < 16; i++)
                {
                    tasks.Add(Task.Run(() => DoBatchAsync(store, 10000 / 16)));
                }

                await Task.WhenAll(tasks);
            }
        }

        private async Task DoBatchAsync(KeyValueStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                var writeBatch = new WriteBatch();
                writeBatch.Put(Encoding.UTF8.GetBytes($"key:{i}"), new byte[64]);
                await store.CommitAsync(writeBatch);

                if (i % 10 == 0)
                {
                    await Task.Yield();
                }
            }
        }
    }
}