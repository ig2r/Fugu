using BenchmarkDotNet.Attributes;
using Fugu.TableSets;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.Benchmarks
{
    /// <summary>
    /// Macro benchmark for writes to a memory-backed <see cref="KeyValueStore"/> instance.
    /// </summary>
    [Config(typeof(MacroBenchmarkConfig))]
    public class KeyValueStoreBenchmark
    {
        public const int OPERATIONS = 100000;
        public const int DATALENGTH = 256;

        [Params(1, 2, 4, 16, 256)]
        public int DegreeOfConcurrency { get; set; } = 1;

        private KeyValueStore _store;

        [IterationSetup]
        public void IterationSetup()
        {
            var tableSet = new InMemoryTableSet();
            _store = KeyValueStore.CreateAsync(tableSet).Result;
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            _store.Dispose();
        }

        [Benchmark(OperationsPerInvoke = OPERATIONS)]
        public Task PutUniqueKeys()
        {
            var tasks = from bucket in Enumerable.Range(0, DegreeOfConcurrency)
                        select Task.Run(async () =>
                        {
                            var data = new byte[DATALENGTH];

                            for (int i = 0; i < OPERATIONS / DegreeOfConcurrency; i++)
                            {
                                var writeBatch = new WriteBatch();
                                writeBatch.Put(Encoding.UTF8.GetBytes($"bucket:{bucket}/key:{i}"), data);
                                await _store.CommitAsync(writeBatch);
                            }
                        });

            return Task.WhenAll(tasks);
        }
    }
}
