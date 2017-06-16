using BenchmarkDotNet.Attributes;
using Fugu.Actors;
using Fugu.Common;
using Fugu.TableSets;
using Fugu.Writer;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Core.Benchmarks
{
    /// <summary>
    /// Throughput benchmark for <see cref="WriterActorCore"/>.
    /// </summary>
    public class WriterActorBenchmark
    {
        private WriterActorCore _actor;
        private StateVector _clock;
        private WriteBatch _writeBatch;
        private TaskCompletionSource<VoidTaskResult> _replyChannel;

        private int _writesToOutputTable = 0;
        private InMemoryTable _outputTable;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var indexUpdateBlock = DataflowBlock.NullTarget<UpdateIndexMessage>();
            var segmentCreatedBlock = DataflowBlock.NullTarget<Segment>();
            _actor = new WriterActorCore(indexUpdateBlock, segmentCreatedBlock);

            _clock = new StateVector(1, 1, 0);
            _writeBatch = new WriteBatch();
            _writeBatch.Put(Encoding.UTF8.GetBytes("key:1"), new byte[64]);
            _replyChannel = new TaskCompletionSource<VoidTaskResult>();
        }

        [Benchmark]
        public Task WriteAsync()
        {
            return _actor.WriteAsync(_clock, _writeBatch, GetOutputTable(), _replyChannel);
        }

        private IWritableTable GetOutputTable()
        {
            // Switch to a new output table once every 64k writes, assuming that 8 MB tables are sufficiently big
            // to take this amount of writes
            if (_writesToOutputTable % 65536 == 0)
            {
                _outputTable = new InMemoryTable(8 * 1024 * 1024);
            }

            _writesToOutputTable++;
            return _outputTable;
        }
    }
}
