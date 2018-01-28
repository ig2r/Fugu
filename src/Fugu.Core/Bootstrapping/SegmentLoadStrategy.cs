using Fugu.Common;
using System.Collections.Generic;
using System.Linq;

namespace Fugu.Bootstrapping
{
    /// <summary>
    /// Given a list of segments available in a table set, this strategy determines an ordering to load data
    /// back into the in-memory index.
    /// </summary>
    public class SegmentLoadStrategy
    {
        private readonly Queue<Segment> _segments;

        public SegmentLoadStrategy(IEnumerable<Segment> segments)
        {
            Guard.NotNull(segments, nameof(segments));

            // Process segments in order, giving preference to segments that span larger generation
            // ranges since they are most likely more recent (e.g., created during compactions) than
            // segments that span smaller ranges
            _segments = new Queue<Segment>(from s in segments
                                           orderby s.MinGeneration ascending,
                                                   s.MaxGeneration descending
                                           select s);
        }

        public bool GetNext(long maxGenerationLoaded, out Segment nextSegment, out bool requireValidFooter)
        {
            // Discard segments that have no unseen data to contribute
            while (_segments.Count > 0 && _segments.Peek().MaxGeneration <= maxGenerationLoaded)
            {
                _segments.Dequeue();
            }

            // Stop enumeration if no more segments are left
            if (_segments.Count == 0)
            {
                nextSegment = null;
                requireValidFooter = false;
                return false;
            }

            nextSegment = _segments.Dequeue();

            // If the segment following our chosen segment covers the same min genration range, we know that the
            // chosen segment was created through a compaction from the following segments, so we will be picky and
            // only accept it if it can be loaded entirely, with footer intact; otherwise, we'll skip it and restore
            // data from the smaller segments that follow it
            requireValidFooter = _segments.Count > 0 &&
                _segments.Peek().MinGeneration == nextSegment.MinGeneration;

            return true;
        }
    }
}
