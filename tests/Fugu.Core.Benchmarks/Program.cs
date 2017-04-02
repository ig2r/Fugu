using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Fugu.TableSets;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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

            BenchmarkRunner.Run<SimpleCommitBenchmark>();
            //new SimpleCommitBenchmark { DegreeOfConcurrency = 1 }.Commit1000Batches().Wait();
        }
    }

    //[MemoryDiagnoser]
    public class SimpleCommitBenchmark
    {
        public int Iterations { get; set; } = 1000;

        [Params(1, 2, 16)]
        public int DegreeOfConcurrency { get; set; }

        [Benchmark]
        public async Task Commit1000Batches()
        {
            var iterationsPerSlice = Iterations / DegreeOfConcurrency;

            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var tasks = from i in Enumerable.Range(0, DegreeOfConcurrency)
                            let iteration = i
                            select Task.Run(async () =>
                            {
                                var data = new byte[64];
                                var offset = iteration * iterationsPerSlice;

                                for (int i = 0; i < iterationsPerSlice; i++)
                                {
                                    var writeBatch = new WriteBatch();
                                    writeBatch.Put(Encoding.UTF8.GetBytes($"key:{offset + i}"), data);
                                    await store.CommitAsync(writeBatch);
                                }
                            });

                await Task.WhenAll(tasks.ToArray());
            }
        }
    }
}