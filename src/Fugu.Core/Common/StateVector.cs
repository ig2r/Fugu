using System;
using System.Diagnostics;

namespace Fugu.Common
{
    [DebuggerDisplay("<{Write}, {Index}, {Compaction}>")]
    public struct StateVector : IEquatable<StateVector>
    {
        private StateVector(long write, long index, long compaction)
        {
            Write = write;
            Index = index;
            Compaction = compaction;
        }

        public long Write { get; }
        public long Index { get; }
        public long Compaction { get; }

        public static StateVector Max(StateVector left, StateVector right)
        {
            return new StateVector(
                Math.Max(left.Write, right.Write),
                Math.Max(left.Index, right.Index),
                Math.Max(left.Compaction, right.Compaction));
        }

        public StateVector NextWrite()
        {
            return new StateVector(Write + 1, Index, Compaction);
        }

        public StateVector NextIndex()
        {
            return new StateVector(Write, Index + 1, Compaction);
        }

        public StateVector NextCompaction()
        {
            return new StateVector(Write, Index, Compaction + 1);
        }

        public bool Equals(StateVector other)
        {
            return this == other;
        }

        public override bool Equals(object obj)
        {
            return obj is StateVector && this == (StateVector)obj;
        }

        public override int GetHashCode()
        {
            return Index.GetHashCode() ^ Compaction.GetHashCode();
        }

        public static bool operator ==(StateVector left, StateVector right)
        {
            return left.Write == right.Write &&
                left.Index == right.Index &&
                left.Compaction == right.Compaction;
        }

        public static bool operator !=(StateVector left, StateVector right)
        {
            return !(left == right);
        }

        public static bool operator <=(StateVector left, StateVector right)
        {
            return left.Write <= right.Write &&
                left.Index <= right.Index &&
                left.Compaction <= right.Compaction;
        }

        public static bool operator >=(StateVector left, StateVector right)
        {
            return left.Write >= right.Write &&
                left.Index >= right.Index &&
                left.Compaction >= right.Compaction;
        }

        public static bool operator <(StateVector left, StateVector right)
        {
            return left <= right && !(left == right);
        }

        public static bool operator >(StateVector left, StateVector right)
        {
            return left >= right && !(left == right);
        }
    }
}
