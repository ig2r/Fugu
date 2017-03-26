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

        private IOutputTable _outputTable;
        private long _totalCapacity = 0;
        private long _spaceLeftInSegment = 0;

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
            if (_spaceLeftInSegment < requiredSpace + _tableFooterSize)
            {
                var requestedCapacity = Math.Max(_tableHeaderSize + requiredSpace + _tableFooterSize, MIN_CAPACITY);
                _outputTable = await _tableFactory.CreateTableAsync(requestedCapacity);
                _totalCapacity += _outputTable.Capacity;
                _spaceLeftInSegment = _outputTable.Capacity - _tableHeaderSize;
            }

            // Pass on write batch to writer actor
            await _commitWriteBatchChannel.SendAsync(new CommitWriteBatchToSegmentMessage(_clock, writeBatch, _outputTable, replyChannel));
            _spaceLeftInSegment -= requiredSpace;
        }

        private long GetRequiredSpaceForWriteBatch(WriteBatch writeBatch)
        {
            long size = Marshal.SizeOf<CommitHeaderRecord>() + Marshal.SizeOf<CommitFooterRecord>();

            foreach (var change in writeBatch.Changes)
            {
                size += change.Key.Length;

                var put = change.Value as WriteBatchItem.Put;
                if (put != null)
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
