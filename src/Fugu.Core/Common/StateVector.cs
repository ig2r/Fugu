using System;
using System.Diagnostics;

namespace Fugu.Common
{
    [DebuggerDisplay("<{Commit}, {OutputGeneration}, {Compaction}>")]
    public struct StateVector : IEquatable<StateVector>
    {
        public StateVector(long commit, long outputGeneration, long compaction)
        {
            Commit = commit;
            OutputGeneration = outputGeneration;
            Compaction = compaction;
        }

        /// <summary>
        /// Gets the monotonically increasing vector component associated with a commit to the store.
        /// </summary>
        public long Commit { get; }

        /// <summary>
        /// Gets the monotonically increasing vector component that indicates the generation of the current output segment.
        /// </summary>
        public long OutputGeneration { get; }

        /// <summary>
        /// Gets the monotonically increasing vector component associated with a compaction operation.
        /// </summary>
        public long Compaction { get; }

        public static StateVector Max(StateVector left, StateVector right)
        {
            return new StateVector(
                Math.Max(left.Commit, right.Commit),
                Math.Max(left.OutputGeneration, right.OutputGeneration),
                Math.Max(left.Compaction, right.Compaction));
        }

        public StateVector NextCommit()
        {
            return new StateVector(Commit + 1, OutputGeneration, Compaction);
        }

        public StateVector NextOutputGeneration()
        {
            return new StateVector(Commit, OutputGeneration + 1, Compaction);
        }

        public StateVector NextCompaction()
        {
            return new StateVector(Commit, OutputGeneration, Compaction + 1);
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
            return OutputGeneration.GetHashCode() ^ Compaction.GetHashCode();
        }

        public static bool operator ==(StateVector left, StateVector right)
        {
            return left.Commit == right.Commit &&
                left.OutputGeneration == right.OutputGeneration &&
                left.Compaction == right.Compaction;
        }

        public static bool operator !=(StateVector left, StateVector right)
        {
            return !(left == right);
        }

        public static bool operator <=(StateVector left, StateVector right)
        {
            return left.Commit <= right.Commit &&
                left.OutputGeneration <= right.OutputGeneration &&
                left.Compaction <= right.Compaction;
        }

        public static bool operator >=(StateVector left, StateVector right)
        {
            return left.Commit >= right.Commit &&
                left.OutputGeneration >= right.OutputGeneration &&
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
