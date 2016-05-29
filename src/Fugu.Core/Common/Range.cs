using System;
using System.Diagnostics;

namespace Fugu.Common
{
    [DebuggerDisplay("{Index}, {Count}")]
    public struct Range : IEquatable<Range>
    {
        public Range(int index, int count)
        {
            if (index < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }

            Index = index;
            Count = count;
        }

        public int Index { get; }
        public int Count { get; }

        #region IEquatable<Range>

        public bool Equals(Range other)
        {
            return this == other;
        }

        #endregion

        public override bool Equals(object obj)
        {
            return obj is Range && this == (Range)obj;
        }

        public override int GetHashCode()
        {
            return Index ^ Count;
        }

        public static bool operator ==(Range x, Range y)
        {
            return x.Index == y.Index && x.Count == y.Count;
        }

        public static bool operator !=(Range x, Range y)
        {
            return !(x == y);
        }
    }
}
