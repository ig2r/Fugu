using Fugu.Channels;
using Fugu.Common;
using Fugu.Format;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public sealed class WriterActorCore
    {
        private readonly Channel<UpdateIndexMessage> _indexUpdateChannel;

        private StateVector _clock;
        private Segment _segment;
        private TableWriter _tableWriter;

        public WriterActorCore(
            long maxGeneration,
            Channel<UpdateIndexMessage> indexUpdateChannel)
        {
            Guard.NotNull(indexUpdateChannel, nameof(indexUpdateChannel));

            _clock = new StateVector(0, maxGeneration, 0);
            _indexUpdateChannel = indexUpdateChannel;
        }

        public async Task CommitAsync(
            StateVector clock,
            WriteBatch writeBatch,
            IOutputTable outputTable,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            Guard.NotNull(outputTable, nameof(outputTable));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            _clock = StateVector.Max(_clock, clock);

            // If the output table changes, we need to close the old output table and initialze the new segment
            if (outputTable != _segment?.Table)
            {
                if (_segment != null)
                {
                    // Complete existing - note that we must make sure that all write have been flushed to disk before
                    // we write the table footer, as its presence guarantees that all data has been written
                    await _tableWriter.OutputStream.FlushAsync();
                    _tableWriter.WriteTableFooter();
                    _tableWriter.Dispose();
                    _tableWriter = null;
                    _segment = null;
                }

                // Start new
                _clock = _clock.NextOutputGeneration();

                _segment = new Segment(_clock.OutputGeneration, _clock.OutputGeneration, outputTable);
                _tableWriter = new TableWriter(outputTable.OutputStream);
                _tableWriter.WriteTableHeader(_clock.OutputGeneration, _clock.OutputGeneration);
            }

            // Now get ready to write this batch
            var changes = writeBatch.Changes.ToArray();

            // Commit header
            _tableWriter.WriteCommitHeader(changes.Length);

            // First pass: write put/tombstone keys
            foreach (var (key, writeBatchItem) in changes)
            {
                if (writeBatchItem is WriteBatchItem.Put put)
                {
                    _tableWriter.WritePut(key, put.Value.Length);
                }
                else
                {
                    _tableWriter.WriteTombstone(key);
                }
            }

            // Second pass: write payload and assemble index updates
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>(changes.Length);

            foreach (var (key, writeBatchItem) in changes)
            {
                if (writeBatchItem is WriteBatchItem.Put put)
                {
                    var valueEntry = new IndexEntry.Value(_segment, _tableWriter.Position, put.Value.Length);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, valueEntry));

                    // Write value
                    _tableWriter.Write(put.Value);
                }
                else
                {
                    var tombstoneEntry = new IndexEntry.Tombstone(_segment);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, tombstoneEntry));
                }
            }

            // Commit footer
            _tableWriter.WriteCommitFooter();

            // Hand off to index actor
            await _indexUpdateChannel.SendAsync(new UpdateIndexMessage(_clock, indexUpdates, replyChannel));
        }
    }
}
