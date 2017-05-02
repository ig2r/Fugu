using BenchmarkDotNet.Attributes;
using Fugu.TableSets;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fugu.Core.Benchmarks
{
    //[MemoryDiagnoser]
    public class SimpleCommitBenchmark
    {
        public const int ITERATIONS = 1000;

        [Params(1, 2, 16)]
        public int DegreeOfConcurrency { get; set; }

        [Benchmark(OperationsPerInvoke = ITERATIONS)]
        public async Task CommitBatches()
        {
            var iterationsPerSlice = ITERATIONS / DegreeOfConcurrency;

            var tableSet = new InMemoryTableSet();
            using (var store = await KeyValueStore.CreateAsync(tableSet))
            {
                var tasks = from i in Enumerable.Range(0, DegreeOfConcurrency)
                            let iteration = i
                            select Task.Run(async () =>
                            {
                                var data = new byte[256];
                                var offset = iteration * iterationsPerSlice;

                                for (int i = 0; i < iterationsPerSlice; i++)
                                {
                                    var writeBatch = new WriteBatch();
                                    writeBatch.Put(GetKey(offset + i), data);
                                    await store.CommitAsync(writeBatch);
                                }
                            });

                await Task.WhenAll(tasks.ToArray());
            }
        }

        private static byte[] GetKey(int index)
        {
            return Encoding.UTF8.GetBytes("key:" + index);
        }
    }
}
