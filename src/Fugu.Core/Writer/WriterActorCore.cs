using Fugu.Actors;
using Fugu.Common;
using Fugu.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Writer
{
    public sealed class WriterActorCore
    {
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;
        private readonly ITargetBlock<Segment> _segmentCreatedBlock;

        private readonly WriteBatchItemCommitBuilder _commitBuilder = new WriteBatchItemCommitBuilder();

        private StateVector _clock = new StateVector();
        private Segment _outputSegment;
        private TableWriter _tableWriter;

        public WriterActorCore(
            ITargetBlock<UpdateIndexMessage> indexUpdateBlock,
            ITargetBlock<Segment> segmentCreatedBlock)
        {
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));
            Guard.NotNull(segmentCreatedBlock, nameof(segmentCreatedBlock));
            _indexUpdateBlock = indexUpdateBlock;
            _segmentCreatedBlock = segmentCreatedBlock;
        }

        public Task WriteAsync(
            StateVector clock,
            WriteBatch writeBatch,
            IWritableTable outputTable,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            Guard.NotNull(outputTable, nameof(outputTable));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            // If the output segment changes, we need to close the old output and initialize the new segment
            if (_outputSegment?.Table != outputTable)
            {
                if (_tableWriter != null)
                {
                    // TODO: We should flush content to disk before writing the table footer
                    var checksum = (ulong)_outputSegment.MinGeneration ^ (ulong)_outputSegment.MaxGeneration;
                    _tableWriter.WriteTableFooter(checksum);
                }

                // Create new segment and notify observers
                _outputSegment = new Segment(clock.OutputGeneration, clock.OutputGeneration, outputTable);
                _segmentCreatedBlock.Post(_outputSegment);

                // Prepare output
                _tableWriter = new TableWriter(outputTable);
                _tableWriter.WriteTableHeader(_outputSegment.MinGeneration, _outputSegment.MaxGeneration);
            }

            // Prepare and write this batch
            _clock = StateVector.Max(_clock, clock).NextCommit();
            var indexUpdates = _commitBuilder.Build(writeBatch.Changes.ToArray(), _outputSegment, _tableWriter);

            // Hand off to index actor
            return _indexUpdateBlock.SendAsync(new UpdateIndexMessage(_clock, indexUpdates, replyChannel));
        }
    }
}
