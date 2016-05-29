using Fugu.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Fugu
{
    /// <summary>
    /// Represents the engine's state for some key. This is an algebraic data type that, depending on the state
    /// of the data item, either holds information that can be used to retrieve the actual value for the key,
    /// or marks the value as deleted.
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

        /// <summary>
        /// Gets the size, in bytes, that the current index entry takes on disk. This MUST NEVER
        /// return 0, because that would mess with the compaction/eviction algorithm.
        /// </summary>
        public abstract int Size { get; }

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

            // TODO: As this is used to estimate how much space this entry takes on disk, it should
            // properly reflect the key's length in some way, sizeof(int) may not suffice.
            public override int Size
            {
                get { return sizeof(int) + ValueLength; }
            }
        }

        public sealed class Tombstone : IndexEntry
        {
            public Tombstone(Segment segment)
                : base(segment)
            {
            }

            // TODO: As this is used to estimate how much space this entry takes on disk, it should
            // properly reflect the key's length in some way, sizeof(int) may not suffice.
            public override int Size
            {
                get { return sizeof(int); }
            }
        }
    }
}
