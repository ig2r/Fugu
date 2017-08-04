using Fugu.IO;
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

        // Segments in this set had their live byte count at zero at some point, and must be considered for pruning
        private readonly HashSet<Segment> _pruningCandidates = new HashSet<Segment>();

        public IReadOnlyList<KeyValuePair<Segment, SegmentStats>> Stats => _stats.ToArray();

        public void OnItemAdded(byte[] key, IndexEntry indexEntry)
        {
            var itemSize = Measure.GetSize(key, indexEntry);
            UpdateStats(indexEntry.Segment, deltaLiveBytes: itemSize);
        }

        public void OnItemRemoved(byte[] key, IndexEntry indexEntry)
        {
            var itemSize = Measure.GetSize(key, indexEntry);
            UpdateStats(indexEntry.Segment, deltaLiveBytes: -itemSize, deltaDeadBytes: itemSize);
        }

        public void OnItemRejected(byte[] key, IndexEntry indexEntry)
        {
            var itemSize = Measure.GetSize(key, indexEntry);
            UpdateStats(indexEntry.Segment, deltaDeadBytes: itemSize);
        }

        public void Prune()
        {
            foreach (var segment in _pruningCandidates)
            {
                if (_stats[segment].LiveBytes == 0)
                {
                    _stats.Remove(segment);
                }
            }

            _pruningCandidates.Clear();
        }

        private void UpdateStats(Segment segment, long deltaLiveBytes = 0, long deltaDeadBytes = 0)
        {
            var stats = _stats.TryGetValue(segment, out var existing)
                ? existing
                : default(SegmentStats);

            stats = new SegmentStats(
                stats.LiveBytes + deltaLiveBytes,
                stats.DeadBytes + deltaDeadBytes);

            _stats[segment] = stats;

            if (stats.LiveBytes == 0)
            {
                _pruningCandidates.Add(segment);
            }
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
