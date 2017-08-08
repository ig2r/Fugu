using Fugu.Common;
using System.Collections.Generic;

namespace Fugu.Bootstrapping
{
    public class CommitInfo
    {
        public CommitInfo(IReadOnlyList<KeyValuePair<byte[], IndexEntry>> indexUpdates, uint commitChecksum)
        {
            Guard.NotNull(indexUpdates, nameof(indexUpdates));

            IndexUpdates = indexUpdates;
            CommitChecksum = commitChecksum;
        }

        public IReadOnlyList<KeyValuePair<byte[], IndexEntry>> IndexUpdates { get; }
        public uint CommitChecksum { get; }
    }
}
