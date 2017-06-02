using Fugu.Actors;
using Fugu.Common;
using Fugu.Format;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Fugu.Writer
{
    public sealed class WriterActorCore
    {
        private readonly ITargetBlock<UpdateIndexMessage> _indexUpdateBlock;

        private StateVector _clock = new StateVector();
        private TableWriter _tableWriter;

        public WriterActorCore(ITargetBlock<UpdateIndexMessage> indexUpdateBlock)
        {
            Guard.NotNull(indexUpdateBlock, nameof(indexUpdateBlock));
            _indexUpdateBlock = indexUpdateBlock;
        }

        public async Task WriteAsync(
            StateVector clock,
            WriteBatch writeBatch,
            Segment outputSegment,
            Stream outputStream,
            TaskCompletionSource<VoidTaskResult> replyChannel)
        {
            Guard.NotNull(writeBatch, nameof(writeBatch));
            Guard.NotNull(outputSegment, nameof(outputSegment));
            Guard.NotNull(outputStream, nameof(outputStream));
            Guard.NotNull(replyChannel, nameof(replyChannel));

            // If the output segment changes, we need to close the old output and initialze the new segment
            if (_tableWriter?.BaseStream != outputStream)
            {
                if (_tableWriter != null)
                {
                    _tableWriter.WriteTableFooter();
                    _tableWriter.BaseStream.Dispose();
                    _tableWriter.Dispose();
                }

                _tableWriter = new TableWriter(outputStream);
            }

            // Now get ready to write this batch
            _clock = StateVector.Max(_clock, clock).NextCommit();
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
                    var valueEntry = new IndexEntry.Value(outputSegment, _tableWriter.BaseStream.Position, put.Value.Length);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, valueEntry));

                    // Write value
                    _tableWriter.Write(put.Value);
                }
                else
                {
                    var tombstoneEntry = new IndexEntry.Tombstone(outputSegment);
                    indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, tombstoneEntry));
                }
            }

            // Commit footer
            _tableWriter.WriteCommitFooter();

            // Hand off to index actor
            await _indexUpdateBlock
                .SendAsync(new UpdateIndexMessage(_clock, indexUpdates, replyChannel))
                .ConfigureAwait(false);
        }
    }
}
