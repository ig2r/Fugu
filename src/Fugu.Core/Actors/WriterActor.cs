using Fugu.Common;
using Fugu.DataFormat;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class WriterActor : IWriterActor
    {
        // Minimum capacity, in bytes, to allocate for new segments
        private const long MIN_TABLE_CAPACITY = 1024;

        // A factor by which each new segment should be larger than the previous segment, to achieve
        // exponential growth in segments
        private const double GROWTH_FACTOR = 1.25;

        private readonly MessageLoop _messageLoop = new MessageLoop();
        private readonly ITableFactory _tableFactory;
        private readonly ICompactionActor _compactionActor;
        private readonly IIndexActor _indexActor;

        private long _generation = 0;
        private TableWriter _tableWriter;
        private Segment _outputSegment;
        private CommitBuilder<WriteBatchItem.Put> _commitBuilder;

        // Total number of payload bytes (live and dead) in segments registered as compactable. Note
        // this is different from the capacity of a segment.
        private long _compactableBytes = 0;

        // Queue of logical points in time at which we have registered segments as compactable, and for which
        // we have not yet received an updated count of compactable bytes. When choosing a capacity for a new
        // segment, we factor the length of this queue into the ideal capacity.
        private readonly Queue<VectorClock> _completedSegmentsAt = new Queue<VectorClock>();

        /// <summary>
        /// Initializes a new instance of the <see cref="WriterActor"/> class.
        /// </summary>
        /// <param name="tableFactory">Enables the creation of new tables to write data to.</param>
        /// <param name="compactionActor">Actor that tracks stats and compactable segments.</param>
        /// <param name="indexActor">Actor to consolidate writes into the master index.</param>
        public WriterActor(ITableFactory tableFactory, ICompactionActor compactionActor, IIndexActor indexActor)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(compactionActor, nameof(compactionActor));
            Guard.NotNull(indexActor, nameof(indexActor));

            _tableFactory = tableFactory;
            _compactionActor = compactionActor;
            _indexActor = indexActor;
        }

        public async Task WriteAsync(VectorClock clock, WriteBatch batch)
        {
            Guard.NotNull(batch, nameof(batch));

            Task continuation;

            using (await _messageLoop)
            {
                // Figure out how much space we need for the incoming batch, then take care to close any current
                // segment that's too small and create a new segment that's large enough for this batch
                long requiredSpace = GetRequiredSpace(batch);
                CloseCurrentOutputSegmentIfTooSmall(clock, requiredSpace);
                await EnsureOutputSegmentAvailableAsync(requiredSpace).ConfigureAwait(false);

                // Write changes and derive resultant index updates
                foreach (var change in batch.Changes)
                {
                    change.Value.Match(
                        onPut: put => _commitBuilder.AddPut(change.Key, put),
                        onDelete: del => _commitBuilder.AddTombstone(change.Key));
                }

                var indexUpdates = _commitBuilder.Complete();

                // Dispatch to index actor
                continuation = _indexActor.MergeIndexUpdatesAsync(clock, indexUpdates);
            }

            await continuation;
        }

        /// <summary>
        /// Invoked when the total number of payload bytes (live and dead) in compactable segments has
        /// changed. The writer actor depends on this number to size new segments, as we generally want
        /// to allocate segments with larger capacity when there is already a lot of data in the table
        /// set.
        /// </summary>
        /// <param name="clock">Clock vector associated with the latest total.</param>
        /// <param name="compactableBytes">Number of payload bytes in compactable segments.</param>
        public async void OnCompactableBytesChanged(VectorClock clock, long compactableBytes)
        {
            if (compactableBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(compactableBytes));
            }

            using (await _messageLoop)
            {
                _compactableBytes = compactableBytes;

                // Stop tracking segments which are accounted for in this clock vector
                while (_completedSegmentsAt.Count > 0 && clock >= _completedSegmentsAt.Peek())
                {
                    _completedSegmentsAt.Dequeue();
                }
            }
        }

        private void CloseCurrentOutputSegmentIfTooSmall(VectorClock clock, long requiredSpace)
        {
            if (_outputSegment == null)
            {
                return;
            }

            long remainingSpace = _outputSegment.Table.Capacity - _tableWriter.Position;
            if (remainingSpace < requiredSpace)
            {
                // TODO: Update footer
                _commitBuilder.Dispose();
                _tableWriter.Dispose();
                _commitBuilder = null;
                _tableWriter = null;

                // Notify compaction actor that writing to this segment has completed and it can now
                // participate in compactions
                _compactionActor.RegisterCompactableSegment(clock, _outputSegment);
                _completedSegmentsAt.Enqueue(clock);

                _outputSegment = null;
            }
        }

        private async Task EnsureOutputSegmentAvailableAsync(long requiredSpace)
        {
            if (_outputSegment != null)
            {
                return;
            }

            // Determine capacity for the new segment. At the very least, the new segment must be big enough
            // to hold the incoming item plus file header. More generally, we aim for a scheme in which each
            // new segment becomes successively larger, based on how much data is already in the table set.
            // This is designed to avoid needing to stall on compactions under sustained writes.
            double fraction = Math.Pow(GROWTH_FACTOR, _completedSegmentsAt.Count + 1) - 1;
            long idealCapacity = Math.Max(MIN_TABLE_CAPACITY, (long)(_compactableBytes * fraction));
            long minCapacity = Marshal.SizeOf<HeaderRecord>() + requiredSpace;
            long capacity = Math.Max(idealCapacity, minCapacity);

            _generation++;

            var table = await _tableFactory.CreateTableAsync(capacity).ConfigureAwait(false);
            _tableWriter = new TableWriter(table.OutputStream);
            _outputSegment = new Segment(_generation, _generation, table);
            _commitBuilder = new CommitBuilder<WriteBatchItem.Put>(
                _tableWriter.OutputStream,
                _outputSegment,
                put => put.Value.Length,
                (put, stream) => stream.Write(put.Value, 0, put.Value.Length));

            _tableWriter.WriteHeader(_generation, _generation);
        }

        /// <summary>
        /// Calculates the required space (in bytes) to serialize a given write batch. This does not
        /// account for table headers, etc.
        /// </summary>
        /// <param name="batch">Write batch to measure.</param>
        /// <returns>Space required for the given batch.</returns>
        private long GetRequiredSpace(WriteBatch batch)
        {
            long requiredSpace = 0;

            foreach (var change in batch.Changes)
            {
                requiredSpace += change.Key.Length + change.Value.Match(
                    onPut: put => Marshal.SizeOf<PutRecord>() + put.Value.Length,
                    onDelete: del => Marshal.SizeOf<TombstoneRecord>());
            }

            requiredSpace += Marshal.SizeOf<StartDataRecord>();
            requiredSpace += Marshal.SizeOf<CommitRecord>();

            return requiredSpace;
        }
    }
}
