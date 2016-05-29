using Fugu.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu.DataFormat
{
    /// <summary>
    /// Writes sets of modifications (puts and deletes) to an output table and constructs index updates for the updated keys.
    /// </summary>
    /// <typeparam name="TPut">The type of input items that represent a put modification.</typeparam>
    public sealed class CommitBuilder<TPut> : IDisposable
    {
        private readonly Stream _outputStream;
        private readonly Segment _segment;
        private readonly Func<TPut, int> _measureValue;
        private readonly Action<TPut, Stream> _writeValue;

        private readonly TableWriter _tableWriter;
        private readonly List<KeyValuePair<byte[], TPut>> _addedPuts = new List<KeyValuePair<byte[], TPut>>();
        private List<KeyValuePair<byte[], IndexEntry>> _indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CommitBuilder{TPut}"/> class.
        /// </summary>
        /// <param name="outputStream">Target stream.</param>
        /// <param name="segment">Target segment.</param>
        /// <param name="measureValue">Function that determines the size, in bytes, of the value for a put item.</param>
        /// <param name="writeValue">Callback that writes the value associated with a put item to a given stream.</param>
        public CommitBuilder(
            Stream outputStream,
            Segment segment,
            Func<TPut, int> measureValue,
            Action<TPut, Stream> writeValue)
        {
            Guard.NotNull(outputStream, nameof(outputStream));
            Guard.NotNull(segment, nameof(segment));
            Guard.NotNull(measureValue, nameof(measureValue));
            Guard.NotNull(writeValue, nameof(writeValue));

            _outputStream = outputStream;
            _segment = segment;
            _measureValue = measureValue;
            _writeValue = writeValue;

            _tableWriter = new TableWriter(outputStream);
        }

        public void AddPut(byte[] key, TPut put)
        {
            _tableWriter.WritePut(key, _measureValue(put));
            _addedPuts.Add(new KeyValuePair<byte[], TPut>(key, put));
        }

        public void AddTombstone(byte[] key)
        {
            _tableWriter.WriteTombstone(key);
            var tombstoneEntry = new IndexEntry.Tombstone(_segment);
            _indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, tombstoneEntry));
        }

        public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> Complete()
        {
            _tableWriter.WriteStartData();

            foreach (var kvp in _addedPuts)
            {
                long position = _outputStream.Position;
                _writeValue(kvp.Value, _outputStream);

                var putEntry = new IndexEntry.Value(_segment, position, _measureValue(kvp.Value));
                _indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(kvp.Key, putEntry));
            }

            _tableWriter.WriteCommit();

            var indexUpdates = _indexUpdates;
            _indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();
            _addedPuts.Clear();

            return indexUpdates;
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _tableWriter.Dispose();
            }
        }
    }
}
