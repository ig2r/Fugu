using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;

namespace Fugu.IO
{
    /// <summary>
    /// Creates a commit from a given data source.
    /// </summary>
    public abstract class CommitBuilder<T, TPut, TDelete>
        where TPut : T
        where TDelete : T
    {
        private readonly IncrementalHash _md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5);

        public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> Build(
            IReadOnlyList<KeyValuePair<byte[], T>> kvps,
            Segment segment,
            TableWriter tableWriter)
        {
            // Commit header
            tableWriter.WriteCommitHeader(kvps.Count);

            // First pass - keys
            foreach (var (key, item) in kvps)
            {
                switch (item)
                {
                    case TPut put:
                        tableWriter.WritePut(key, GetValue(put).Length);
                        break;
                    case TDelete delete:
                        tableWriter.WriteTombstone(key);
                        break;
                }

                _md5.AppendData(key);
            }

            var indexUpdates = new List<KeyValuePair<byte[], IndexEntry>>();

            // Second pass - values
            foreach (var (key, item) in kvps)
            {
                switch (item)
                {
                    case TPut put:
                        var value = GetValue(put);
                        indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(
                            key,
                            new IndexEntry.Value(segment, tableWriter.Offset, value.Length)));
                        tableWriter.Write(value);
                        _md5.AppendData(value.ToArray());
                        break;

                    case TDelete delete:
                        indexUpdates.Add(new KeyValuePair<byte[], IndexEntry>(
                            key,
                            new IndexEntry.Tombstone(segment)));
                        tableWriter.WriteTombstone(key);
                        break;
                }
            }

            // Commit footer
            var hash = _md5.GetHashAndReset();
            uint commitChecksum = hash[0] | (uint)(hash[1] << 8) | (uint)(hash[2] << 16) | (uint)(hash[3] << 24);
            tableWriter.WriteCommitFooter(commitChecksum);

            return indexUpdates;
        }

        protected abstract ReadOnlySpan<byte> GetValue(TPut put);
    }
}
