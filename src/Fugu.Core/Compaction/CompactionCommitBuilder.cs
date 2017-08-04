using Fugu.IO;
using System;
using System.Collections.Generic;
using System.Text;

namespace Fugu.Compaction
{
    public class CompactionCommitBuilder : CommitBuilder<IndexEntry, IndexEntry.Value, IndexEntry.Tombstone>
    {
        #region CommitBuilder<IndexEntry, IndexEntry.Value, IndexEntry.Tombstone>

        protected override ReadOnlySpan<byte> GetValue(IndexEntry.Value put)
        {
            return put.Segment.Table.GetSpan(put.Offset).Slice(0, put.ValueLength);
        }

        #endregion
    }
}
