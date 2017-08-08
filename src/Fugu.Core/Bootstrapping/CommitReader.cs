using Fugu.Common;
using Fugu.IO;
using Fugu.IO.Records;
using System;
using System.Collections.Generic;

namespace Fugu.Bootstrapping
{
    public class CommitReader
    {
        private readonly Segment _segment;
        private readonly TableReader _tableReader;

        public CommitReader(Segment segment, TableReader tableReader)
        {
            Guard.NotNull(segment, nameof(segment));
            Guard.NotNull(tableReader, nameof(tableReader));

            _segment = segment;
            _tableReader = tableReader;
        }

        public CommitInfo ReadCommit()
        {
            var commitHeader = _tableReader.ReadCommitHeader();

            var putValues = new List<KeyValuePair<byte[], int>>();
            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>(commitHeader.Count);

            // Read puts and tombstones
            for (int i = 0; i < commitHeader.Count; i++)
            {
                var tag = (CommitRecordType)_tableReader.GetTag();

                switch (tag)
                {
                    case CommitRecordType.Put:
                        {
                            var putRecord = _tableReader.ReadPut();
                            var key = _tableReader.ReadBytes(putRecord.KeyLength).ToArray();
                            putValues.Add(new KeyValuePair<byte[], int>(key, putRecord.ValueLength));
                            break;
                        }

                    case CommitRecordType.Tombstone:
                        {
                            var tombstoneRecord = _tableReader.ReadTombstone();
                            var key = _tableReader.ReadBytes(tombstoneRecord.KeyLength).ToArray();
                            indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(key, new IndexEntry.Tombstone(_segment)));
                            break;
                        }
                }
            }

            // Read values
            for (int i = 0; i < putValues.Count; i++)
            {
                var (key, valueLength) = putValues[i];
                indexUpdates.Add(
                    new KeyValuePair<byte[], IndexEntry>(
                        key,
                        new IndexEntry.Value(_segment, _tableReader.Offset, valueLength)));

                _tableReader.ReadBytes(valueLength);
            }

            var commitFooter = _tableReader.ReadCommitFooter();

            return new CommitInfo(indexUpdates, commitFooter.CommitChecksum);
        }
    }
}
