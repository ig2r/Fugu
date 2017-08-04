using Fugu.Actors;
using Fugu.Common;
using Fugu.IO;
using Fugu.IO.Records;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Partitioning
{
    public class PartitioningActorCore
    {
        // The minimum capacity, in bytes, when allocating a new segment
        private const long MIN_CAPACITY = 1024 * 1024;

        // The factor by which each new segment should be larger than the previous one. Must be greater than 1.0.
        private const double SCALE_FACTOR = 1.25;

        private readonly long _tableHeaderSize;
        private readonly long _tableFooterSize;

        private readonly ITableFactory _tableFactory;
        private readonly ITargetBlock<WriteToSegmentMessage> _writeBlock;

        private StateVector _clock;
        private IWritableTable _outputTable;

        // The space, in bytes, that's left in the current output table
        private long _spaceLeftInOutputTable = 0;

        // The total capacity, in bytes, of all the tables that are currently in use
        private long _totalCapacity = 0;

        public PartitioningActorCore(
            long maxGeneration,
            long totalCapacity,
            ITableFactory tableFactory,
            ITargetBlock<WriteToSegmentMessage> writeBlock)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(writeBlock, nameof(writeBlock));

            _clock = new StateVector(0, maxGeneration, 0);
            _totalCapacity = totalCapacity;
            _tableFactory = tableFactory;
            _writeBlock = writeBlock;

            _tableHeaderSize = Unsafe.SizeOf<TableHeaderRecord>();
            _tableFooterSize = Unsafe.SizeOf<TableFooterRecord>();
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

                _outputTable = await _tableFactory.CreateTableAsync(requestedCapacity).ConfigureAwait(false);
                _totalCapacity += _outputTable.Capacity;
                _spaceLeftInOutputTable = _outputTable.Capacity - _tableHeaderSize;
            }

            _spaceLeftInOutputTable -= requiredSpace;

            // Pass on write batch to writer actor
            await _writeBlock
                .SendAsync(new WriteToSegmentMessage(_clock, writeBatch, _outputTable, replyChannel))
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
                Unsafe.SizeOf<CommitHeaderRecord>() +
                writeBatch.Changes.Sum(kvp => Measure.GetSize(kvp.Key, kvp.Value)) +
                Unsafe.SizeOf<CommitFooterRecord>();
        }
    }
}
