using BenchmarkDotNet.Attributes;
using Fugu.Actors;
using Fugu.Common;
using Fugu.Index;
using Fugu.TableSets;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Core.Benchmarks
{
    public class IndexActorBenchmark
    {
        public const int OPERATIONS = 100000;

        private IndexActorCore _actor;
        private Segment _segment;
        private TaskCompletionSource<VoidTaskResult> _replyChannel;

        [IterationSetup]
        public void IterationSetup()
        {
            var snapshotsUpdateBlock = DataflowBlock.NullTarget<SnapshotsUpdateMessage>();
            var segmentStatsChangedBlock = DataflowBlock.NullTarget<SegmentStatsChangedMessage>();
            _actor = new IndexActorCore(snapshotsUpdateBlock, segmentStatsChangedBlock);

            _segment = new Segment(1, 1, new InMemoryTable(1024));
            _replyChannel = new TaskCompletionSource<VoidTaskResult>();
        }

        [Benchmark(OperationsPerInvoke = OPERATIONS)]
        public async Task UpdateIndexAsync()
        {
            var clock = new StateVector(0, 1, 0);
            var indexUpdates = new KeyValuePair<byte[], IndexEntry>[1];

            for (int i = 0; i < OPERATIONS; i++)
            {
                clock = clock.NextCommit();
                indexUpdates[0] = new KeyValuePair<byte[], IndexEntry>(
                    Encoding.UTF8.GetBytes("key:" + i),
                    new IndexEntry.Value(_segment, 0, 1));
                await _actor.UpdateIndexAsync(clock, indexUpdates, _replyChannel);
            }
        }
    }
}
