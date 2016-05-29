using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Index = Fugu.Common.CritBitTree<Fugu.Common.ByteArrayKeyTraits, byte[], Fugu.IndexEntry>;

namespace Fugu.Compaction
{
    public interface ICompactor
    {
        Task<CompactionResult> CompactAsync(Index index, long minGeneration, long maxGeneration, bool dropTombstones);
    }
}
