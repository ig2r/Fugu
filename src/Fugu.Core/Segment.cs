using Fugu.Common;
using System;
using System.Diagnostics;

namespace Fugu
{
    [DebuggerDisplay("MinGeneration = {MinGeneration}, MaxGeneration = {MaxGeneration}")]
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

        public SegmentStats Stats { get; } = new SegmentStats();
    }
}
