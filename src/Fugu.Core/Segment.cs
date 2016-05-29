using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Fugu
{
    public class Segment
    {
        public Segment(long minGeneration, long maxGeneration, ITable table)
        {
            if (minGeneration <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minGeneration));
            }

            if (maxGeneration <= 0 || maxGeneration < minGeneration)
            {
                throw new ArgumentOutOfRangeException(nameof(maxGeneration));
            }

            Guard.NotNull(table, nameof(table));

            MinGeneration = minGeneration;
            MaxGeneration = maxGeneration;
            Table = table;
        }

        public long MinGeneration { get; }
        public long MaxGeneration { get; }
        public ITable Table { get; }

        public long LiveBytes { get; private set; } = 0;
        public long DeadBytes { get; private set; } = 0;

        public void AddLiveBytes(long count)
        {
            LiveBytes += count;
        }

        public void MarkBytesAsDead(long count)
        {
            LiveBytes -= count;
            DeadBytes += count;
        }
    }
}
