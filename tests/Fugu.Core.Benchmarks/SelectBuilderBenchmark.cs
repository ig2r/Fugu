using BenchmarkDotNet.Attributes;
using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.Benchmarks
{
    [MemoryDiagnoser]
    public class SelectBuilderBenchmark
    {
        const int ITERS = 1000;

        [Benchmark(OperationsPerInvoke = ITERS)]
        public async Task SelectOnUnbufferedChannel()
        {
            var channel = new UnbufferedChannel<int>();
            FeedChannel(channel);
            await new SelectBuilder()
                .Case(channel, x => Task.CompletedTask)
                .SelectAsync(n => n < ITERS - 1);
        }

        [Benchmark(OperationsPerInvoke = ITERS)]
        public async Task SelectOnUnboundedChannel()
        {
            var channel = new UnboundedChannel<int>();
            FeedChannel(channel);
            await new SelectBuilder()
                .Case(channel, x => Task.CompletedTask)
                .SelectAsync(n => n < ITERS - 1);
        }

        private async void FeedChannel(Channel<int> channel)
        {
            for (int i = 0; i < ITERS; i++)
            {
                await channel.SendAsync(i);
            }
        }
    }
}
