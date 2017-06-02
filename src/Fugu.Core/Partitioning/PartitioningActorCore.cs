using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Partitioning
{
    public class PartitioningActorCore
    {
        // The minimum capacity, in bytes, when allocating a new segment
        private const long MIN_CAPACITY = 4096;

        // The factor by which each new segment should be larger than the previous one. Must be greater than 1.0.
        private const double SCALE_FACTOR = 1.125;

        private readonly long _tableHeaderSize;
        private readonly long _tableFooterSize;

        private readonly ITableFactory _tableFactory;
        private readonly ITargetBlock<WriteToSegmentMessage> _writeBlock;
        private readonly ITargetBlock<Segment> _segmentCreatedBlock;

        private StateVector _clock;
        private Segment _outputSegment;
        private Stream _outputStream;

        // The space, in bytes, that's left in the current output table
        private long _spaceLeftInOutputTable = 0;

        // The total capacity, in bytes, of all the tables that are currently in use
        private long _totalCapacity = 0;

        public PartitioningActorCore(
            long maxGeneration,
            ITableFactory tableFactory,
            ITargetBlock<WriteToSegmentMessage> writeBlock,
            ITargetBlock<Segment> segmentCreatedBlock)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(writeBlock, nameof(writeBlock));
            Guard.NotNull(segmentCreatedBlock, nameof(segmentCreatedBlock));

            _clock = new StateVector(0, maxGeneration, 0);
            _tableFactory = tableFactory;
            _writeBlock = writeBlock;
            _segmentCreatedBlock = segmentCreatedBlock;

            _tableHeaderSize = Marshal.SizeOf<TableHeaderRecord>();
            _tableFooterSize = Marshal.SizeOf<TableFooterRecord>();
        }

        public async Task CommitAsync(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            var requiredSpace = GetRequiredSpaceForWriteBatch(writeBatch);

            // If there isn't enough space left in the current output segment for the incoming write (or if there's
            // no output segment at all), we need to start a new segment
            if (_spaceLeftInOutputTable < requiredSpace + _tableFooterSize)
            {
                // At a minimum, the new segment must be large enough to fit the incoming write plus requisite headers.
                // Beyond that, we scale new segments in proportion to the amount of data that's already in the store.
                // For sustained writes, this will cause segment sizes to follow a geometric progression with a common
                // ratio of SCALE_FACTOR, thereby curtailing the number of segments created.
                var requiredCapacity = _tableHeaderSize + requiredSpace + _tableFooterSize;
                var desiredCapacity = MIN_CAPACITY + (long)(_totalCapacity * (SCALE_FACTOR - 1.0));
                var requestedCapacity = Math.Max(requiredCapacity, desiredCapacity);

                _clock = _clock.NextOutputGeneration();
                var outputTable = await _tableFactory.CreateTableAsync(requestedCapacity).ConfigureAwait(false);
                _outputSegment = new Segment(_clock.OutputGeneration, _clock.OutputGeneration, outputTable);
                _outputStream = outputTable.GetOutputStream(0, outputTable.Capacity);

                // Write table header; remaining content will be written by writer actor
                using (var tableWriter = new TableWriter(_outputStream))
                {
                    tableWriter.WriteTableHeader(_outputSegment.MinGeneration, _outputSegment.MaxGeneration);
                }

                // Keep track of table sizes. Note that _outputTable.Capacity may in fact be larger than the requested capacity.
                _totalCapacity += outputTable.Capacity;
                _spaceLeftInOutputTable = outputTable.Capacity - _tableHeaderSize;

                // Notify observers that a new segment has come into existence
                _segmentCreatedBlock.Post(_outputSegment);
            }

            _spaceLeftInOutputTable -= requiredSpace;

            // Pass on write batch to writer actor
            await _writeBlock
                .SendAsync(new WriteToSegmentMessage(_clock, writeBatch, _outputSegment, _outputStream, replyChannel))
                .ConfigureAwait(false);
        }

        public void OnTotalCapacityChanged(long deltaCapacity)
        {
            if (_totalCapacity + deltaCapacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(deltaCapacity));
            }

            _totalCapacity += deltaCapacity;
        }

        private long GetRequiredSpaceForWriteBatch(WriteBatch writeBatch)
        {
            return
                Marshal.SizeOf<CommitHeaderRecord>() +
                writeBatch.Changes.Sum(kvp => Measure.GetSize(kvp.Key, kvp.Value)) +
                Marshal.SizeOf<CommitFooterRecord>();
        }
    }
}
