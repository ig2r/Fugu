using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Actors
{
    /// <summary>
    /// Accumulates changes to the number of live/dead bytes for segments.
    /// </summary>
    public class SegmentSizeChangeTracker
    {
        private readonly Dictionary<Segment, SegmentSizeChange> _changes = new Dictionary<Segment, SegmentSizeChange>();
        private StateVector _clock;

        public void OnItemAdded(byte[] key, IndexEntry indexEntry)
        {
            var previous = _changes.TryGetValue(indexEntry.Segment, out var existing)
                ? existing
                : default(SegmentSizeChange);

            var itemSize = Measure(key, indexEntry);
            _changes[indexEntry.Segment] = new SegmentSizeChange(
                previous.LiveBytesChange + itemSize,
                previous.DeadBytesChange);
        }

        public void OnItemRemoved(byte[] key, IndexEntry indexEntry)
        {
            var previous = _changes.TryGetValue(indexEntry.Segment, out var existing)
                ? existing
                : default(SegmentSizeChange);

            var itemSize = Measure(key, indexEntry);
            _changes[indexEntry.Segment] = new SegmentSizeChange(
                previous.LiveBytesChange - itemSize,
                previous.DeadBytesChange + itemSize);
        }

        public void OnItemRejected(byte[] key, IndexEntry indexEntry)
        {
            var previous = _changes.TryGetValue(indexEntry.Segment, out var existing)
                ? existing
                : default(SegmentSizeChange);

            var itemSize = Measure(key, indexEntry);
            _changes[indexEntry.Segment] = new SegmentSizeChange(
                previous.LiveBytesChange,
                previous.DeadBytesChange + itemSize);
        }

        public bool TryGetBatchedSizeChanges(StateVector clock, out IReadOnlyList<KeyValuePair<Segment, SegmentSizeChange>> changes)
        {
            var shouldFlushChanges = clock.OutputGeneration > _clock.OutputGeneration || clock.Compaction > _clock.Compaction;
            _clock = StateVector.Max(_clock, clock);

            if (shouldFlushChanges)
            {
                changes = _changes.ToArray();
                _changes.Clear();
                return true;
            }
            else
            {
                changes = null;
                return false;
            }
        }

        private static long Measure(byte[] key, IndexEntry indexEntry)
        {
            switch (indexEntry)
            {
                case IndexEntry.Value v:
                    return key.Length + v.ValueLength;
                case IndexEntry.Tombstone t:
                    return key.Length;
                default:
                    throw new NotSupportedException();
            }
        }
    }
}
