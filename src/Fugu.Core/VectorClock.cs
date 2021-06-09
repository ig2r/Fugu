namespace Fugu
{
    // Represents the logical clock state ("recency") of an associated data item.
    public readonly struct VectorClock
    {
        public long Write { get; init; }
        public long Compaction { get; init; }

        public VectorClock NextWrite => new() { Write = Write + 1, Compaction = Compaction };
        public VectorClock NextCompaction => new() { Write = Write, Compaction = Compaction + 1 };

        // TODO: implement equality, partial comparison, etc.
    }
}
