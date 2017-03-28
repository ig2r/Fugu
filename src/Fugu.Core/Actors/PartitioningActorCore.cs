using Fugu.Channels;
using Fugu.Common;
using Fugu.Format;
using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class PartitioningActorCore
    {
        // The minimum capacity, in bytes, when allocating a new segment
        private const long MIN_CAPACITY = 4096;
        private readonly long _tableHeaderSize;
        private readonly long _tableFooterSize;

        private readonly Channel<CommitWriteBatchToSegmentMessage> _commitWriteBatchChannel;
        private readonly ITableFactory _tableFactory;

        private StateVector _clock = new StateVector();

        // The current table to which incoming commits are directed
        private IOutputTable _outputTable;

        // The space, in bytes, that's left in the current output table
        private long _spaceLeftInOutputTable = 0;

        // The total space, in bytes, that's occupied by all the tables in the store's table set. This number is used to determine
        // the capacity of new output tables, as we aim to make output tables larger as the table set grows.
        private long _totalCapacity = 0;

        public PartitioningActorCore(ITableFactory tableFactory, Channel<CommitWriteBatchToSegmentMessage> commitWriteBatchChannel)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(commitWriteBatchChannel, nameof(commitWriteBatchChannel));

            _tableFactory = tableFactory;
            _commitWriteBatchChannel = commitWriteBatchChannel;

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
                // TODO: Assign ideal capacity as a percentage of the current total capacity of all tables in the table set
                var requestedCapacity = Math.Max(_tableHeaderSize + requiredSpace + _tableFooterSize, MIN_CAPACITY);
                _outputTable = await _tableFactory.CreateTableAsync(requestedCapacity);
                _spaceLeftInOutputTable = _outputTable.Capacity - _tableHeaderSize;

                // Keep track of the total space occupied by tables
                _totalCapacity += _outputTable.Capacity;
            }

            // Pass on write batch to writer actor
            await _commitWriteBatchChannel.SendAsync(new CommitWriteBatchToSegmentMessage(_clock, writeBatch, _outputTable, replyChannel));
            _spaceLeftInOutputTable -= requiredSpace;
        }

        public Task OnTotalCapacityChangedAsync(long deltaCapacity)
        {
            if (_totalCapacity + deltaCapacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(deltaCapacity));
            }

            _totalCapacity += deltaCapacity;
            return Task.CompletedTask;
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
