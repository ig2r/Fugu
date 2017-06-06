using Fugu.Common;
using Fugu.Format;
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
        private readonly SortedDictionary<Segment, SegmentStats> _stats =
            new SortedDictionary<Segment, SegmentStats>(new SegmentGenerationComparer());

        public IReadOnlyList<KeyValuePair<Segment, SegmentStats>> Stats => _stats.ToArray();

        public void OnItemAdded(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure.GetSize(key, indexEntry);
            _stats[indexEntry.Segment] = new SegmentStats(
                previous.LiveBytes + itemSize,
                previous.DeadBytes);
        }

        public void OnItemRemoved(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure.GetSize(key, indexEntry);
            _stats[indexEntry.Segment] = new SegmentStats(
                previous.LiveBytes - itemSize,
                previous.DeadBytes + itemSize);
        }

        public void OnItemRejected(byte[] key, IndexEntry indexEntry)
        {
            var previous = GetStatsOrDefault(indexEntry.Segment);
            var itemSize = Measure.GetSize(key, indexEntry);
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

        private SegmentStats GetStatsOrDefault(Segment segment)
        {
            return _stats.TryGetValue(segment, out var existing)
                ? existing
                : default(SegmentStats);
        }

        #region Nested types

        private class SegmentGenerationComparer : IComparer<Segment>
        {
            #region IComparer<Segment>

            public int Compare(Segment x, Segment y)
            {
                var cmp = x.MinGeneration.CompareTo(y.MinGeneration);
                return cmp != 0
                    ? cmp
                    : x.MaxGeneration.CompareTo(y.MaxGeneration);
            }

            #endregion
        }

        #endregion
    }
}
