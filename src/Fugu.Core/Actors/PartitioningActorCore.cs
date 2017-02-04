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

        private readonly ITableFactory _tableFactory;
        private readonly IWriterActor _writerActor;

        private long _totalCapacity = 0;
        private long _spaceLeftInSegment = 0;

        public PartitioningActorCore(ITableFactory tableFactory, IWriterActor writerActor)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(writerActor, nameof(writerActor));

            _tableFactory = tableFactory;
            _writerActor = writerActor;

            _tableHeaderSize = Marshal.SizeOf<TableHeaderRecord>();
            _tableFooterSize = Marshal.SizeOf<TableFooterRecord>();
        }

        public async Task CommitAsync(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            var requiredSpace = GetRequiredSpaceForWriteBatch(writeBatch);

            // If there isn't enough space left in the current output segment for the incoming write, or if there's
            // no output segment at all, determine requested size and tell writer actor to start a new segment
            if (_spaceLeftInSegment < requiredSpace + _tableFooterSize)
            {
                var requestedCapacity = Math.Max(_tableHeaderSize + requiredSpace + _tableFooterSize, MIN_CAPACITY);
                var outputTable = await _tableFactory.CreateTableAsync(requestedCapacity);
                _writerActor.StartNewSegment(outputTable);

                _totalCapacity += outputTable.Capacity;
                _spaceLeftInSegment = outputTable.Capacity - _tableHeaderSize;
            }

            // Pass on write batch to writer actor
            _writerActor.Commit(writeBatch, replyChannel);
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
