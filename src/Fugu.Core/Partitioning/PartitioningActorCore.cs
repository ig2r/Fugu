using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Partitioning
{
    public class PartitioningActorCore
    {
        // The minimum capacity, in bytes, when allocating a new segment
        private const long MIN_CAPACITY = 4096;
        private readonly long _tableHeaderSize;
        private readonly long _tableFooterSize;

        private readonly ITableFactory _tableFactory;
        private readonly ITargetBlock<WriteToSegmentMessage> _writeBlock;

        private StateVector _clock = new StateVector();

        // The current table to which incoming commits are directed
        private IOutputTable _outputTable;

        // The space, in bytes, that's left in the current output table
        private long _spaceLeftInOutputTable = 0;

        // The total space, in bytes, that's occupied by all the tables in the store's table set. This number is used to determine
        // the capacity of new output tables, as we aim to make output tables larger as the table set grows.
        private long _totalCapacity = 0;

        public PartitioningActorCore(ITableFactory tableFactory, ITargetBlock<WriteToSegmentMessage> writeBlock)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(writeBlock, nameof(writeBlock));

            _tableFactory = tableFactory;
            _writeBlock = writeBlock;

            _tableHeaderSize = Marshal.SizeOf<TableHeaderRecord>();
            _tableFooterSize = Marshal.SizeOf<TableFooterRecord>();
        }

        public async Task CommitAsync(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _clock = _clock.NextCommit();

            var requiredSpace = GetRequiredSpaceForWriteBatch(writeBatch);

            // If there isn't enough space left in the current output segment for the incoming write, or if there's
            // no output segment at all, determine requested size and tell writer actor to start a new segment
            if (_spaceLeftInOutputTable < requiredSpace + _tableFooterSize)
            {
                // We'll need at least this much space in the new segment
                var minimumRequiredCapacity = _tableHeaderSize + requiredSpace + _tableFooterSize;

                // However, based on how much data is already in the store, we want to apply an exponential scale to the capacity
                // of new segments so that the number of segments remains logarithmic in the total number of bytes in the store
                // even without compaction
                var targetCapacity = Math.Max(_totalCapacity / 8, MIN_CAPACITY);

                var requestedCapacity = Math.Max(minimumRequiredCapacity, targetCapacity);
                _outputTable = await _tableFactory.CreateTableAsync(requestedCapacity).ConfigureAwait(false);

                // Keep track of table sizes. Note that _outputTable.Capacity may in fact be larger than the requested capacity.
                _totalCapacity += _outputTable.Capacity;
                _spaceLeftInOutputTable = _outputTable.Capacity - _tableHeaderSize;
            }

            _spaceLeftInOutputTable -= requiredSpace;

            // Pass on write batch to writer actor
            await _writeBlock.SendAsync(new WriteToSegmentMessage(_clock, writeBatch, _outputTable, replyChannel)).ConfigureAwait(false);
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
            long size = Marshal.SizeOf<CommitHeaderRecord>() + Marshal.SizeOf<CommitFooterRecord>();

            foreach (var (key, writeBatchItem) in writeBatch.Changes)
            {
                size += key.Length;

                if (writeBatchItem is WriteBatchItem.Put put)
                {
                    size += Marshal.SizeOf<PutRecord>();
                    size += put.Value.Length;
                }
                else
                {
                    size += Marshal.SizeOf<TombstoneRecord>();
                }
            }

            return size;
        }
    }
}
