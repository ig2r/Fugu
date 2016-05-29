using System;
using System.Diagnostics;

namespace Fugu.Common
{
    [DebuggerDisplay("{Modification} / {Compaction}")]
    public struct VectorClock : IEquatable<VectorClock>
    {
        public VectorClock(long modification, long compaction)
        {
            Modification = modification;
            Compaction = compaction;
        }

        public long Modification { get; }
        public long Compaction { get; }

        public static VectorClock Merge(VectorClock x, VectorClock y)
        {
            return new VectorClock(Math.Max(x.Modification, y.Modification), Math.Max(x.Compaction, y.Compaction));
        }

        public bool Equals(VectorClock other)
        {
            return this == other;
        }

        public override bool Equals(object obj)
        {
            return obj is VectorClock && this == (VectorClock)obj;
        }

        public override int GetHashCode()
        {
            return Modification.GetHashCode() ^ Compaction.GetHashCode();
        }

        public static bool operator ==(VectorClock x, VectorClock y)
        {
            return x.Modification == y.Modification && x.Compaction == y.Compaction;
        }

        public static bool operator !=(VectorClock x, VectorClock y)
        {
            return !(x == y);
        }

        public static bool operator <=(VectorClock x, VectorClock y)
        {
            return x.Modification <= y.Modification && x.Compaction <= y.Compaction;
        }

        public static bool operator >=(VectorClock x, VectorClock y)
        {
            return x.Modification >= y.Modification && x.Compaction >= y.Compaction;
        }

        public static bool operator <(VectorClock x, VectorClock y)
        {
            return x <= y && !(x == y);
        }

        public static bool operator >(VectorClock x, VectorClock y)
        {
            return x >= y && !(x == y);
        }
    }
}
