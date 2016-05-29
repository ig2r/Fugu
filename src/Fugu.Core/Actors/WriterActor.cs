using Fugu.Common;
using Fugu.DataFormat;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public class WriterActor : IWriterActor
    {
        private readonly MessageLoop _messageLoop = new MessageLoop();
        private readonly ITableFactory _tableFactory;
        private readonly IIndexActor _indexActor;

        private long _generation = 0;
        private TableWriter _tableWriter;
        private Segment _outputSegment;
        private CommitBuilder<WriteBatchItem.Put> _commitBuilder;

        private long _compactableCapacity = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="WriterActor"/> class.
        /// </summary>
        /// <param name="tableFactory">Enables the creation of new tables to write data to.</param>
        /// <param name="indexActor">Actor to consolidate writes into the master index.</param>
        public WriterActor(ITableFactory tableFactory, IIndexActor indexActor)
        {
            Guard.NotNull(tableFactory, nameof(tableFactory));
            Guard.NotNull(indexActor, nameof(indexActor));

            _tableFactory = tableFactory;
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
                CloseCurrentOutputSegmentIfTooSmall(requiredSpace);
                await EnsureOutputSegmentAvailableAsync(requiredSpace);

                // Write changes and derive resultant index updates
                foreach (var change in batch.Changes)
                {
                    change.Value.Match(
                        onPut: put => _commitBuilder.AddPut(change.Key, put),
                        onDelete: del => _commitBuilder.AddTombstone(change.Key));
                }

                var indexUpdates = _commitBuilder.Complete();

                // Dispatch to index actor
                continuation = _indexActor.UpdateAsync(clock, indexUpdates);
            }

            await continuation;
        }

        public async void OnCompactableCapacityChanged(long delta)
        {
            using (await _messageLoop)
            {
                Debug.Assert(_compactableCapacity + delta >= 0);
                _compactableCapacity += delta;
            }
        }

        private void CloseCurrentOutputSegmentIfTooSmall(long requiredSpace)
        {
            if (_outputSegment == null)
            {
                return;
            }

            long position = _tableWriter.Position;
            long remainingSpace = _outputSegment.Table.Capacity - position;

            if (remainingSpace < requiredSpace)
            {
                // TODO: Update footer
                _commitBuilder.Dispose();
                _tableWriter.Dispose();
                _commitBuilder = null;
                _tableWriter = null;

                // Notify compaction actor that writing to this segment has completed and it can now
                // participate in compactions
                _compactableCapacity += _outputSegment.Table.Capacity;
                _indexActor.RegisterCompactableSegment(_outputSegment);

                _outputSegment = null;
            }
        }

        private async Task EnsureOutputSegmentAvailableAsync(long requiredSpace)
        {
            if (_outputSegment != null)
            {
                return;
            }

            _generation++;

            // New segment must have sufficient capacity to hold at least the current batch
            long targetCapacity = Math.Max(1024, _compactableCapacity / 8);
            long minCapacity = Math.Max(targetCapacity, Marshal.SizeOf<HeaderRecord>() + requiredSpace);

            var table = await _tableFactory.CreateTableAsync(minCapacity);
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
