using Fugu.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Actors
{
    /// <summary>
    /// Accumulates changes to the number of live/dead bytes for segments.
    /// </summary>
    public class SegmentSizeTracker
    {
        private readonly Dictionary<Segment, SegmentStats> _stats = new Dictionary<Segment, SegmentStats>();

        public IReadOnlyList<KeyValuePair<Segment, SegmentStats>> Stats => _stats.ToArray();

        public void OnItemAdded(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure(key, indexEntry);
            _stats[indexEntry.Segment] = new SegmentStats(
                previous.LiveBytes + itemSize,
                previous.DeadBytes);
        }

        public void OnItemRemoved(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure(key, indexEntry);
            _stats[indexEntry.Segment] = new SegmentStats(
                previous.LiveBytes - itemSize,
                previous.DeadBytes + itemSize);
        }

        public void OnItemRejected(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure(key, indexEntry);
            _stats[indexEntry.Segment] = new SegmentStats(
                previous.LiveBytes,
                previous.DeadBytes + itemSize);
        }

        public void Prune()
        {
            var prunedSegments = (from s in _stats
                                  where s.Value.LiveBytes == 0
                                  select s.Key).ToArray();

            foreach (var s in prunedSegments)
            {
                _stats.Remove(s);
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

        private SegmentStats GetStatsOrDefault(Segment segment)
        {
            return _stats.TryGetValue(segment, out var existing)
                ? existing
                : default(SegmentStats);
        }
    }
}
