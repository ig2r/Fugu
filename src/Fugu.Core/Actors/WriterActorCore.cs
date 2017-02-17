using Fugu.Common;
using Fugu.Format;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.Actors
{
    public sealed class WriterActorCore
    {
        private readonly IIndexActor _indexActor;

        private StateVector _clock;
        private long _generation;
        private Segment _segment;
        private TableWriter _tableWriter;

        public WriterActorCore(IIndexActor indexActor, long maxGeneration)
        {
            Guard.NotNull(indexActor, nameof(indexActor));
            _indexActor = indexActor;
            _generation = maxGeneration;
        }

        #region IWriterActor

        public async Task StartNewSegmentAsync(IOutputTable outputTable)
        {
            Guard.NotNull(outputTable, nameof(outputTable));

            // Close existing
            if (_tableWriter != null)
            {
                await _tableWriter.OutputStream.FlushAsync();
                _tableWriter.WriteTableFooter();
                _tableWriter.Dispose();
                _tableWriter = null;

                // TODO: notify that segment has been completed; increment clock?
                _segment = null;
            }

            // Create new segment
            ++_generation;
            _segment = new Segment(_generation, _generation, outputTable);
            _tableWriter = new TableWriter(outputTable.OutputStream);
            _tableWriter.WriteTableHeader(_generation, _generation);
        }

        public void Commit(WriteBatch writeBatch, TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            _clock = _clock.NextWrite();
            var changes = writeBatch.Changes.ToArray();

            // Commit header
            _tableWriter.WriteCommitHeader(changes.Length);

            // First pass: write put/tombstone keys
            foreach (var change in changes)
            {
                if (change.Value is WriteBatchItem.Put put)
                {
                    _tableWriter.WritePut(change.Key, put.Value.Length);
                }
                else
                {
                    _tableWriter.WriteTombstone(change.Key);
                }
            }

            // Second pass: write payload and assemble index updates
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>(changes.Length);

            foreach (var change in changes)
            {
                if (change.Value is WriteBatchItem.Put put)
                {
                    var valueEntry = new IndexEntry.Value(_segment, _tableWriter.Position, put.Value.Length);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(change.Key, valueEntry));

                    // Write value
                    _tableWriter.Write(put.Value);
                }
                else
                {
                    var tombstoneEntry = new IndexEntry.Tombstone(_segment);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(change.Key, tombstoneEntry));
                }
            }

            // Commit footer
            _tableWriter.WriteCommitFooter();

            // Hand off to index actor
            _indexActor.UpdateIndex(_clock, indexUpdates, replyChannel);
        }

        #endregion
    }
}
