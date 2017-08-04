using Fugu.IO;
using System;

namespace Fugu.Writer
{
    public class WriteBatchItemCommitBuilder : CommitBuilder<WriteBatchItem, WriteBatchItem.Put, WriteBatchItem.Delete>
    {
        #region CommitBuilder<WriteBatchItem.Put>

        protected override ReadOnlySpan<byte> GetValue(WriteBatchItem.Put put)
        {
            return put.Value;
        }

        #endregion
    }
}
