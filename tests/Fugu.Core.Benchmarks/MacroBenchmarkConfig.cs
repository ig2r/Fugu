using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace Fugu.Core.Benchmarks
{
    public class MacroBenchmarkConfig : ManualConfig
    {
        public MacroBenchmarkConfig()
        {
            Add(Job.Dry
                .WithId(nameof(MacroBenchmarkConfig))
                .With(RunStrategy.Monitoring)
                .WithTargetCount(3)
                .WithInvocationCount(1)
                .WithUnrollFactor(1));

            //Add(new MemoryDiagnoser());
        }
    }
}
