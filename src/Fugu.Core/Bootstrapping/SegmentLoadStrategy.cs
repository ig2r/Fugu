using Fugu.Common;
using System;
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
        private readonly Segment[] _segments;

        public SegmentLoadStrategy(IEnumerable<Segment> segments)
        {
            Guard.NotNull(segments, nameof(segments));

            // Process segments in order, giving preference to segments that span larger generation
            // ranges since they are most likely more recent (e.g., created during compactions) than
            // segments that span smaller ranges
            _segments = (from s in segments
                         orderby
                             s.MinGeneration ascending,
                             s.MaxGeneration descending
                         select s).ToArray();
        }

        public bool GetNext(long maxGenerationLoaded, out Segment nextSegment, out bool requireValidFooter)
        {
            // Find the first segment that starts at a generation greater than what we have loaded so far
            nextSegment = _segments.FirstOrDefault(s => s.MinGeneration > maxGenerationLoaded);

            if (nextSegment == null)
            {
                requireValidFooter = false;
                return false;
            }

            // If the segment following our chosen segment covers the same min genration range, we know that the
            // chosen segment was created through a compaction from the following segments, so we will be picky and
            // only accept it if it can be loaded entirely, with footer intact; otherwise, we'll skip it and restore
            // data from the smaller segments that follow it
            var index = Array.IndexOf(_segments, nextSegment);
            requireValidFooter =
                index + 1 < _segments.Length &&
                nextSegment.MinGeneration == _segments[index + 1].MinGeneration;

            return true;
        }
    }
}
