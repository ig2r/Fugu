using BenchmarkDotNet.Running;
using Microsoft.Extensions.Configuration;

namespace Fugu.Core.Benchmarks
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddCommandLine(args)
                .Build();

            BenchmarkRunner.Run<CritBitTreeInsertionBenchmark>();
            //BenchmarkRunner.Run<ByteArrayEqualityBenchmark>();
            //BenchmarkRunner.Run<SimpleCommitBenchmark>();
            //new SimpleCommitBenchmark { DegreeOfConcurrency = 1 }.Commit1000Batches().Wait();
        }
    }
}
