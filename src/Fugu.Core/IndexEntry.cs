using Fugu.Common;

namespace Fugu
{
    /// <summary>
    /// Represents the store's state for some key. This algebraic data type will either point to the corresponding
    /// value in a segment (see <see cref="Value"/>), or mark the key as deleted (see <see cref="Tombstone"/>).
    /// </summary>
    public abstract class IndexEntry
    {
        private IndexEntry(Segment segment)
        {
            Guard.NotNull(segment, nameof(segment));
            Segment = segment;
        }

        /// <summary>
        /// Gets the segment that holds the durable representation of the current index entry.
        /// </summary>
        public Segment Segment { get; }

        public sealed class Value : IndexEntry
        {
            public Value(Segment segment, long offset, int valueLength)
                : base(segment)
            {
                Offset = offset;
                ValueLength = valueLength;
            }

            public long Offset { get; }
            public int ValueLength { get; }
        }

        public sealed class Tombstone : IndexEntry
        {
            public Tombstone(Segment segment)
                : base(segment)
            {
            }
        }
    }
}
