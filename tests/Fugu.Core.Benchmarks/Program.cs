using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fugu.TableSets;
using Microsoft.Extensions.Configuration;
using System;
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

            BenchmarkRunner.Run<BenchmarkSample>();
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
                for (int i = 0; i < 1000; i++)
                {
                    var writeBatch = new WriteBatch();
                    writeBatch.Put(Encoding.UTF8.GetBytes($"key:{i}"), new byte[64]);
                    await store.CommitAsync(writeBatch);
                }
            }
        }
    }
}