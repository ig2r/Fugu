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

            //var benchmark = new KeyValueStoreBenchmark { DegreeOfConcurrency = 8 };
            //benchmark.IterationSetup();
            //benchmark.PutUniqueKeys().Wait();
            //benchmark.IterationCleanup();

            BenchmarkRunner.Run<KeyValueStoreBenchmark>();

            //BenchmarkRunner.Run<WriterActorBenchmark>();
            //BenchmarkRunner.Run<FormatMmapBenchmark>();
            //BenchmarkRunner.Run<WriteMmapBenchmark>();
            //BenchmarkRunner.Run<CritBitTreeInsertionBenchmark>();
            //BenchmarkRunner.Run<ByteArrayEqualityBenchmark>();
        }
    }
}
